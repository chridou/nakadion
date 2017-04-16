//! The components to consume from the stream.
//!
//! This is basically the machinery that drives the consumption.
//! It will consume events and call the `Handler`
//! and react on its commands on how to continue.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{BufReader, BufRead};
use std::time::Duration;
use std::thread::{self, JoinHandle};

use serde_json::{self, Value};

use super::*;
use super::connector::{NakadiConnector, Checkpoints, ReadsStream};

const RETRY_MILLIS: &'static [u64] = &[10, 20, 50, 100, 200, 300, 400, 500, 1000, 2000, 5000,
                                       10000, 30000, 60000, 300000, 600000];

/// The worker runs the consumption of events.
/// It will try to reconnect automatically once the stream breaks.
pub struct NakadiWorker {
    is_running: Arc<AtomicBool>,
    subscription_id: SubscriptionId,
}

impl NakadiWorker {
    /// Creates a new instance. The returned `JoinHandle` can
    /// be used to synchronize with the underlying worker thread.
    /// The underlying worker will be stopped once the worker is dropped.
    pub fn new<C: NakadiConnector, H: Handler>(connector: Arc<C>,
                                               handler: H,
                                               subscription_id: SubscriptionId)
                                               -> (NakadiWorker, JoinHandle<()>) {
        let is_running = Arc::new(AtomicBool::new(true));

        let handle = start_nakadi_worker_loop(connector.clone(),
                                              handler,
                                              subscription_id.clone(),
                                              is_running.clone());

        (NakadiWorker {
             is_running: is_running,
             subscription_id: subscription_id,
         },
         handle)
    }

    /// Returns true if the worker is still running.
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    /// Stops the worker.
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed)
    }

    /// Gets the `SubscriptionId` the worker is listening to.
    pub fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
    }
}

impl Drop for NakadiWorker {
    fn drop(&mut self) {
        info!("Cleanup. Nakadi worker stopping.");
        self.stop();
    }
}

#[derive(Deserialize)]
struct DeserializedBatch {
    cursor: Cursor,
    events: Option<Vec<Value>>,
}

fn start_nakadi_worker_loop<C: NakadiConnector, H: Handler>(connector: Arc<C>,
                                                            handler: H,
                                                            subscription_id: SubscriptionId,
                                                            is_running: Arc<AtomicBool>)
                                                            -> JoinHandle<()> {
    info!("Nakadi worker loop starting");
    thread::spawn(move || {
                      let connector = connector;
                      let is_running = is_running;
                      let subscription_id = subscription_id;
                      let handler = handler;
                      nakadi_worker_loop(&*connector, handler, &subscription_id, is_running);
                  })
}

fn nakadi_worker_loop<C: NakadiConnector, H: Handler>(connector: &C,
                                                      handler: H,
                                                      subscription_id: &SubscriptionId,
                                                      is_running: Arc<AtomicBool>) {
    while (*is_running).load(Ordering::Relaxed) {
        let (src, stream_id) = if let Some(r) = connect(connector, subscription_id, &is_running) {
            r
        } else {
            warn!("Connection attempt aborted. Stopping the worker.");
            break;
        };

        let buffered_reader = BufReader::new(src);

        for line in buffered_reader.lines() {
            match line {
                Ok(line) => {
                    match process_line(connector,
                                       line.as_ref(),
                                       &handler,
                                       &stream_id,
                                       subscription_id,
                                       &is_running) {
                        Ok(AfterBatchAction::Continue) => (),
                        Ok(leaving_action) => {
                            info!("Leaving worker loop on user request: {:?}", leaving_action);
                            is_running.store(false, Ordering::Relaxed);
                            return;
                        }
                        Err(err) => {
                            error!("An error occured processing the batch. Reconnecting. Error: {}",
                                   err);
                            break;
                        }
                    }
                }
                Err(err) => {
                    error!("Stream was closed unexpectedly: {}", err);
                    break;
                }
            }
        }
    }

    info!("Nakadi worker loop stopping.");
    (&*is_running).store(false, Ordering::Relaxed);
}

fn process_line<C: Checkpoints>(connector: &C,
                                line: &str,
                                handler: &Handler,
                                stream_id: &StreamId,
                                subscription_id: &SubscriptionId,
                                is_running: &AtomicBool)
                                -> ClientResult<AfterBatchAction> {
    match serde_json::from_str::<DeserializedBatch>(line) {
        Ok(DeserializedBatch { cursor, events }) => {
            // This is a hack. We might later want to extract the slice manually.
            let events_json = events.unwrap_or(Vec::new());
            let events_str = serde_json::to_string(events_json.as_slice()).unwrap();
            match handler.handle(events_str.as_ref()) {
                AfterBatchAction::Continue => {
                    checkpoint(&*connector,
                               &stream_id,
                               subscription_id,
                               vec![cursor].as_slice(),
                               &is_running);
                    Ok(AfterBatchAction::Continue)
                }
                AfterBatchAction::Stop => {
                    checkpoint(&*connector,
                               &stream_id,
                               subscription_id,
                               vec![cursor].as_slice(),
                               &is_running);
                    Ok(AfterBatchAction::Stop)
                }
                AfterBatchAction::Abort => {
                    warn!("Abort. Skipping checkpointing.");
                    Ok(AfterBatchAction::Abort)
                }
            }
        }
        Err(err) => bail!(ClientErrorKind::UnparsableBatch(err.to_string())),
    }
}

fn connect<C: ReadsStream>(connector: &C,
                           subscription_id: &SubscriptionId,
                           is_running: &AtomicBool)
                           -> Option<(C::StreamingSource, StreamId)> {
    let mut attempt = 0;
    while is_running.load(Ordering::Relaxed) {
        attempt += 1;
        info!("Connecting to Nakadi(attempt {}).", attempt);
        match connector.read(subscription_id) {
            Ok(r) => {
                info!("Connected.");
                return Some(r);
            }
            Err(ClientError(ClientErrorKind::Conflict(msg), _)) => {
                warn!("There was a conflict. Maybe there are no shards to read from left: {}",
                      msg);
                let pause = ::std::cmp::max(retry_pause(attempt - 1), Duration::from_secs(30));
                thread::sleep(pause);
            }
            Err(err) => {
                error!("Failed to connect to Nakadi: {}", err);
                let pause = retry_pause(attempt - 1);
                thread::sleep(pause);
            }
        }
    }
    None
}

fn checkpoint<C: Checkpoints>(checkpointer: &C,
                              stream_id: &StreamId,
                              subscription_id: &SubscriptionId,
                              cursors: &[Cursor],
                              is_running: &AtomicBool) {
    let mut attempt = 0;
    while is_running.load(Ordering::Relaxed) || attempt == 0 {
        if attempt > 0 {
            let pause = retry_pause(attempt - 1);
            thread::sleep(pause)
        }
        attempt += 1;
        match checkpointer.checkpoint(stream_id, subscription_id, cursors) {
            Ok(()) => return,
            Err(err) => {
                if attempt > 5 {
                    error!("Finally gave up to checkpoint cursor after {} attempts.",
                           err);
                    return;
                } else {
                    warn!("Failed to checkpoint to Nakadi: {}", err);
                }
            }
        }
    }
    error!("Checkpointing aborted due to worker shutdown.");
}

fn retry_pause(retry: usize) -> Duration {
    let idx = ::std::cmp::min(retry, RETRY_MILLIS.len() - 1);
    ::std::time::Duration::from_millis(RETRY_MILLIS[idx])
}

#[cfg(test)]
mod test {
    extern crate env_logger;

    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use super::*;
    use super::process_line;

    const TEST_LINE_WITH_EVENTS: &'static str =
        r#"{"cursor":{"partition":"5","offset":"543","event_type":"order.ORDER_RECEIVED","cursor_token":"b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"abc","id":"111"},"data_type":"blah"}]}"#;

    const TEST_LINE_WITHOUT_EVENTS: &'static str =
        r#"{"cursor":{"partition":"0","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"bf6ee7a9-0fe5-4946-b6d6-30895baf0599"}}"#;

    const TEST_EVENTS: &'static [u8] =
        br#"{"cursor":{"partition":"5","offset":"543","event_type":"order.ORDER_RECEIVED","cursor_token":"b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba0","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"AAA","id":"111"},"data_type":"blah"}]}
           {"cursor":{"partition":"5","offset":"544","event_type":"order.ORDER_RECEIVED","cursor_token":"a28568a9-1ca0-4d9f-b519-dd6dd4b7a610"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba1","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"BBB","id":"222"},"data_type":"blah"}]}
           {"cursor":{"partition":"5","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"a241c147-c186-49ad-a96e-f1e8566de738"},"events":[{"metadata":{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7-e683e8ff4ba2","event_type":"aruha-test-hila","partition":"5","received_at":"2016-09-30T09:19:00.741Z","flow_id":"blahbloh"},"data_op":"C","data":{"order_number":"CCC","id":"333"},"data_type":"blah"}]}
           {"cursor":{"partition":"0","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"bf6ee7a9-0fe5-4946-b6d6-30895baf0599"}}
           {"cursor":{"partition":"1","offset":"545","event_type":"order.ORDER_RECEIVED","cursor_token":"9ed8058a-95be-4611-a33d-f862d6dc4af5"}}"#;


    struct DevNullCheckpointer;
    impl Checkpoints for DevNullCheckpointer {
        fn checkpoint(&self,
                      _stream_id: &StreamId,
                      _subscription: &SubscriptionId,
                      _cursors: &[Cursor])
                      -> ClientResult<()> {
            Ok(())
        }
    }

    struct CollectingCheckpointer(Mutex<Vec<Cursor>>);
    impl Checkpoints for CollectingCheckpointer {
        fn checkpoint(&self,
                      _stream_id: &StreamId,
                      _subscription: &SubscriptionId,
                      cursors: &[Cursor])
                      -> ClientResult<()> {
            let collected = &mut self.0.lock().unwrap();
            for c in cursors {
                collected.push(c.clone());
            }
            Ok(())
        }
    }

    struct DevNullHandler(AfterBatchAction);
    impl Handler for DevNullHandler {
        fn handle(&self, _batch: &str) -> AfterBatchAction {
            self.0.clone()
        }
    }

    struct CollectingHandler(Mutex<Vec<String>>);

    impl Handler for CollectingHandler {
        fn handle(&self, batch: &str) -> AfterBatchAction {
            self.0.lock().unwrap().push(batch.to_string());
            AfterBatchAction::Continue
        }
    }

    #[test]
    fn should_send_a_line_with_events_to_the_handler() {
        let is_running = AtomicBool::new(true);
        let collected = Mutex::new(Vec::new());
        let handler = CollectingHandler(collected);

        process_line(&DevNullCheckpointer,
                     TEST_LINE_WITH_EVENTS,
                     &handler,
                     &StreamId::new(""),
                     &SubscriptionId::nil(),
                     &is_running)
                .unwrap();

        let collected = handler.0.lock().unwrap();

        assert_eq!(collected.len(), 1);
        assert_eq!(true, is_running.load(Ordering::Relaxed));
    }

    #[test]
    fn should_send_a_line_without_events_to_the_handler() {
        let is_running = AtomicBool::new(true);
        let collected = Mutex::new(Vec::new());
        let handler = CollectingHandler(collected);

        process_line(&DevNullCheckpointer,
                     TEST_LINE_WITHOUT_EVENTS,
                     &handler,
                     &StreamId::new(""),
                     &SubscriptionId::nil(),
                     &is_running)
                .unwrap();

        let collected = handler.0.lock().unwrap();

        assert_eq!(1, collected.len());
        assert_eq!("[]", collected[0]);
        assert_eq!(true, is_running.load(Ordering::Relaxed));
    }

    #[test]
    fn should_checkpoint_a_line_on_continue() {
        let is_running = AtomicBool::new(true);
        let collected = Mutex::new(Vec::new());

        let checkpointer = CollectingCheckpointer(collected);

        process_line(&checkpointer,
                     TEST_LINE_WITHOUT_EVENTS,
                     &DevNullHandler(AfterBatchAction::Continue),
                     &StreamId::new(""),
                     &SubscriptionId::nil(),
                     &is_running)
                .unwrap();

        let collected = checkpointer.0.lock().unwrap();

        assert_eq!(1, collected.len());
        assert_eq!(true, is_running.load(Ordering::Relaxed));
    }

    #[test]
    fn should_checkpoint_a_line_on_stop() {
        let is_running = AtomicBool::new(true);
        let collected = Mutex::new(Vec::new());

        let checkpointer = CollectingCheckpointer(collected);

        process_line(&checkpointer,
                     TEST_LINE_WITHOUT_EVENTS,
                     &DevNullHandler(AfterBatchAction::Stop),
                     &StreamId::new(""),
                     &SubscriptionId::nil(),
                     &is_running)
                .unwrap();

        let collected = checkpointer.0.lock().unwrap();

        assert_eq!(1, collected.len());
        // process_line does not modify is_running
        assert_eq!(true, is_running.load(Ordering::Relaxed));
    }

    #[test]
    fn should_not_checkpoint_a_line_on_stop() {
        let is_running = AtomicBool::new(true);
        let collected = Mutex::new(Vec::new());

        let checkpointer = CollectingCheckpointer(collected);

        process_line(&checkpointer,
                     TEST_LINE_WITHOUT_EVENTS,
                     &DevNullHandler(AfterBatchAction::Abort),
                     &StreamId::new(""),
                     &SubscriptionId::nil(),
                     &is_running)
                .unwrap();

        let collected = checkpointer.0.lock().unwrap();

        assert_eq!(0, collected.len());
        // process_line does not modify is_running
        assert_eq!(true, is_running.load(Ordering::Relaxed));
    }

    struct TestConnector {
        checkpointed: Mutex<Vec<Cursor>>,
    }

    impl TestConnector {
        fn new() -> Self {
            TestConnector { checkpointed: Mutex::new(Vec::new()) }
        }
    }

    impl Checkpoints for TestConnector {
        fn checkpoint(&self,
                      _stream_id: &StreamId,
                      _subscription: &SubscriptionId,
                      cursors: &[Cursor])
                      -> ClientResult<()> {
            let collected = &mut self.checkpointed.lock().unwrap();
            for c in cursors {
                collected.push(c.clone());
            }
            Ok(())
        }
    }

    impl ReadsStream for TestConnector {
        type StreamingSource = &'static [u8];

        fn read(&self,
                _subscription: &SubscriptionId)
                -> ClientResult<(Self::StreamingSource, StreamId)> {
            Ok((TEST_EVENTS, StreamId::new("")))
        }
    }

    impl NakadiConnector for TestConnector {
        fn settings(&self) -> &ConnectorSettings {
            unimplemented!()
        }
    }

    struct TestHandler(Arc<Mutex<Vec<String>>>);

    impl Handler for TestHandler {
        fn handle(&self, batch: &str) -> AfterBatchAction {
            let collected = &mut self.0.lock().unwrap();
            collected.push(batch.to_string());
            let count = collected.len();
            if count == 42 {
                AfterBatchAction::Stop
            } else {
                AfterBatchAction::Continue
            }
        }
    }

    fn init_logger() {
        ::std::env::set_var("RUST_LOG", "debug");
        let _ = env_logger::init();
    }

    #[test]
    fn the_worker_loop_should_work() {
        init_logger();
        let is_running = Arc::new(AtomicBool::new(true));
        let subscription_id = SubscriptionId::nil();

        let connector = TestConnector::new();

        let collected = Arc::new(Mutex::new(Vec::new()));
        let handler = TestHandler(collected.clone());

        nakadi_worker_loop(&connector, handler, &subscription_id, is_running.clone());

        assert_eq!(42, collected.lock().unwrap().len());
        assert_eq!(42, connector.checkpointed.lock().unwrap().len());
        assert_eq!(false, is_running.load(Ordering::Relaxed));
    }

    #[test]
    fn the_worker_should_work() {
        init_logger();
        let subscription_id = SubscriptionId::nil();

        let connector = Arc::new(TestConnector::new());

        let collected = Arc::new(Mutex::new(Vec::new()));
        let handler = TestHandler(collected.clone());

        let (worker, handle) = NakadiWorker::new(connector.clone(), handler, subscription_id);

        handle.join().unwrap();

        assert_eq!(42, collected.lock().unwrap().len());
        assert_eq!(42, connector.checkpointed.lock().unwrap().len());
        assert_eq!(false, worker.is_running());
    }
}
