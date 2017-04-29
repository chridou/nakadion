extern crate env_logger;

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use super::*;
use super::process_line;
use super::metrics::*;

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
    fn handle(&self, _batch: &str, _batch_info: BatchInfo) -> AfterBatchAction {
        self.0.clone()
    }
}

struct CollectingHandler(Mutex<Vec<String>>);

impl Handler for CollectingHandler {
    fn handle(&self, batch: &str, _batch_info: BatchInfo) -> AfterBatchAction {
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
                 &is_running,
                 &WorkerMetrics::new())
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
                 &is_running,
                 &WorkerMetrics::new())
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
                 &is_running,
                 &WorkerMetrics::new())
        .unwrap();

    let collected = checkpointer.0.lock().unwrap();

    assert_eq!(1, collected.len());
    assert_eq!(true, is_running.load(Ordering::Relaxed));
}

#[test]
fn should_not_checkpoint_a_line_on_continue() {
    let is_running = AtomicBool::new(true);
    let collected = Mutex::new(Vec::new());

    let checkpointer = CollectingCheckpointer(collected);

    process_line(&checkpointer,
                 TEST_LINE_WITHOUT_EVENTS,
                 &DevNullHandler(AfterBatchAction::ContinueNoCheckpoint),
                 &StreamId::new(""),
                 &SubscriptionId::nil(),
                 &is_running,
                 &WorkerMetrics::new())
        .unwrap();

    let collected = checkpointer.0.lock().unwrap();

    assert_eq!(0, collected.len());
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
                 &is_running,
                 &WorkerMetrics::new())
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
                 &is_running,
                 &WorkerMetrics::new())
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

impl ProvidesStreamInfo for TestConnector {
    fn stream_info(&self, subscription: &SubscriptionId) -> ClientResult<StreamInfo> {
        Ok(StreamInfo::default())
    }
}

struct TestHandler(Arc<Mutex<Vec<String>>>);

impl Handler for TestHandler {
    fn handle(&self, batch: &str, _batch_info: BatchInfo) -> AfterBatchAction {
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

    nakadi_worker_loop(&connector,
                       handler,
                       &subscription_id,
                       is_running.clone(),
                       &WorkerMetrics::new());

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

    let (worker, handle) = SequentialWorker::new(connector.clone(),
                                                 handler,
                                                 subscription_id,
                                                 SequentialWorkerSettings).unwrap();

    handle.join().unwrap();

    assert_eq!(42, collected.lock().unwrap().len());
    assert_eq!(42, connector.checkpointed.lock().unwrap().len());
    assert_eq!(false, worker.is_running());
}
