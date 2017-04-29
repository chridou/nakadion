//! Consume a subscription on a single thread
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{BufReader, BufRead};
use std::time::{Instant, Duration};
use std::thread::{self, JoinHandle};

use serde_json::{self, Value};

use super::*;
use super::metrics::*;
use super::connector::{NakadiConnector, Checkpoints, ReadsStream};

const RETRY_MILLIS: &'static [u64] = &[10, 15, 20, 50, 100, 200, 300, 400, 500, 1000, 2000, 5000,
                                       10000, 30000, 60000, 300000, 600000];


pub struct SequentialWorkerSettings;

impl SequentialWorkerSettings {
    /// Create the settings from environment variables.
    ///
    /// This is a Dummy. Currently there are no settings.
    pub fn from_env() -> Result<SequentialWorkerSettings, String> {
        Ok(SequentialWorkerSettings)
    }
}

/// The worker runs the consumption of events.
/// It will try to reconnect automatically once the stream breaks.
/// All work is done sequentially on a single thread.
pub struct SequentialWorker {
    is_running: Arc<AtomicBool>,
    subscription_id: SubscriptionId,
    metrics: Arc<WorkerMetrics>,
}

impl SequentialWorker {
    /// Creates a new instance. The returned `JoinHandle` can
    /// be used to synchronize with the underlying worker thread.
    /// The underlying worker will be stopped once the worker is dropped.
    pub fn new<C: NakadiConnector, H: Handler + 'static>(connector: Arc<C>,
                                                         handler: H,
                                                         subscription_id: SubscriptionId,
                                                         _settings: SequentialWorkerSettings)
                                                         -> (SequentialWorker, JoinHandle<()>) {
        let is_running = Arc::new(AtomicBool::new(true));
        let metrics = Arc::new(WorkerMetrics::new());

        let handle = start_worker_loop(connector.clone(),
                                       handler,
                                       subscription_id.clone(),
                                       is_running.clone(),
                                       metrics.clone());

        let metrics2 = metrics.clone();
        let is_running2 = is_running.clone();

        thread::spawn(move || {
            let metrics = metrics2;
            let is_running = is_running2;
            let mut last_tick = Instant::now();
            while is_running.load(Ordering::Relaxed) {
                let now = Instant::now();
                if now - last_tick >= Duration::from_secs(5) {
                    metrics.tick();
                    last_tick = now;
                }
                thread::sleep(Duration::from_millis(100));
            }
            info!("Sequential worker timer stopped.");
        });

        (SequentialWorker {
            is_running: is_running,
            subscription_id: subscription_id,
            metrics: metrics,
        },
         handle)
    }
}

impl Worker for SequentialWorker {
    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed)
    }

    fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
    }

    fn stats(&self) -> WorkerStats {
        self.metrics.stats()
    }
}

impl Drop for SequentialWorker {
    fn drop(&mut self) {
        info!("Cleanup. Sequential worker stopping.");
        self.stop();
    }
}

#[derive(Deserialize)]
struct DeserializedBatch {
    cursor: Cursor,
    events: Option<Vec<Value>>,
}

fn start_worker_loop<C: NakadiConnector, H: Handler + 'static>(connector: Arc<C>,
                                                               handler: H,
                                                               subscription_id: SubscriptionId,
                                                               is_running: Arc<AtomicBool>,
                                                               metrics: Arc<WorkerMetrics>)
                                                               -> JoinHandle<()> {
    info!("Sequential worker loop starting");
    thread::spawn(move || {
        let connector = connector;
        let is_running = is_running;
        let subscription_id = subscription_id;
        let handler = handler;
        let metrics = metrics;
        nakadi_worker_loop(&*connector, handler, &subscription_id, is_running, &metrics);
    })
}

fn nakadi_worker_loop<C: NakadiConnector, H: Handler>(connector: &C,
                                                      handler: H,
                                                      subscription_id: &SubscriptionId,
                                                      is_running: Arc<AtomicBool>,
                                                      metrics: &WorkerMetrics) {
    while (*is_running).load(Ordering::Relaxed) {
        info!("No connection to Nakadi. Requesting connection...");
        let unconnected_since = Instant::now();
        let (src, stream_id) =
            if let Some(result) = connect(connector, subscription_id, &is_running, metrics) {
                result
            } else {
                warn!("Connection attempt aborted. Stopping the worker.");
                break;
            };
        metrics.stream.connected(unconnected_since);

        let connected_since = Instant::now();
        let buffered_reader = BufReader::new(src);

        let mut lines_read: usize = 0;
        for line in buffered_reader.lines() {
            match line {
                Ok(line) => {
                    metrics.stream.line_received(line.as_ref());
                    lines_read += 1;
                    match process_line(connector,
                                       line.as_ref(),
                                       &handler,
                                       &stream_id,
                                       subscription_id,
                                       &is_running,
                                       metrics) {
                        Ok(AfterBatchAction::Continue) => (),
                        Ok(AfterBatchAction::ContinueNoCheckpoint) => (),
                        Ok(leaving_action) => {
                            info!("Leaving worker loop for stream {} on user request: {:?}",
                                  stream_id.0,
                                  leaving_action);
                            is_running.store(false, Ordering::Relaxed);
                            return;
                        }
                        Err(err) => {
                            error!("An error occured processing the batch for stream {}. \
                                    Reconnecting. Error: {}",
                                   stream_id.0,
                                   err);
                            break;
                        }
                    }
                }
                Err(err) => {
                    error!("Stream {} was closed unexpectedly: {}", stream_id.0, err);
                    break;
                }
            }
        }
        metrics.stream.connection_ended(connected_since, lines_read as u64);
        info!("Stream({}) from Nakadi ended or there was an error. Dropping connection. Read {} \
               lines.",
              stream_id.0,
              lines_read);
    }

    info!("Nakadi worker loop stopping.");
    (&*is_running).store(false, Ordering::Relaxed);
}

fn process_line<C: Checkpoints>(connector: &C,
                                line: &str,
                                handler: &Handler,
                                stream_id: &StreamId,
                                subscription_id: &SubscriptionId,
                                is_running: &AtomicBool,
                                metrics: &WorkerMetrics)
                                -> ClientResult<AfterBatchAction> {
    let handler_metrics = &metrics.handler;
    match serde_json::from_str::<DeserializedBatch>(line) {
        Ok(DeserializedBatch { cursor, events }) => {
            // This is a hack. We might later want to extract the slice manually.
            let events_json = events.unwrap_or(Vec::new());
            let events_str = serde_json::to_string(events_json.as_slice()).unwrap();
            let batch_info = BatchInfo {
                stream_id: stream_id.clone(),
                cursor: cursor.clone(),
            };

            let started_at = Instant::now();
            handler_metrics.batch_received(events_str.as_ref());

            match handler.handle(events_str.as_ref(), batch_info) {
                AfterBatchAction::Continue => {
                    handler_metrics.batch_processed(started_at);
                    checkpoint(&*connector,
                               &stream_id,
                               subscription_id,
                               vec![cursor].as_slice(),
                               &is_running,
                               &metrics.checkpointing);
                    Ok(AfterBatchAction::Continue)
                }
                AfterBatchAction::ContinueNoCheckpoint => {
                    handler_metrics.batch_processed(started_at);
                    Ok(AfterBatchAction::ContinueNoCheckpoint)
                }
                AfterBatchAction::Stop => {
                    handler_metrics.batch_processed(started_at);
                    checkpoint(&*connector,
                               &stream_id,
                               subscription_id,
                               vec![cursor].as_slice(),
                               &is_running,
                               &metrics.checkpointing);
                    Ok(AfterBatchAction::Stop)
                }
                AfterBatchAction::Abort => {
                    handler_metrics.batch_processed(started_at);
                    warn!("Abort. Skipping checkpointing on stream {}.", stream_id.0);
                    Ok(AfterBatchAction::Abort)
                }
            }
        }
        Err(err) => {
            bail!(ClientErrorKind::UnparsableBatch(format!("Could not parse '{}': {}", line, err)))
        }
    }
}

fn connect<C: ReadsStream>(connector: &C,
                           subscription_id: &SubscriptionId,
                           is_running: &AtomicBool,
                           metrics: &WorkerMetrics)
                           -> Option<(C::StreamingSource, StreamId)> {
    let mut attempt = 0;
    while is_running.load(Ordering::Relaxed) {
        attempt += 1;
        info!("Connecting to Nakadi(attempt {}).", attempt);
        match connector.read(subscription_id) {
            Ok(r) => {
                info!("Connected to Nakadi. Stream id is {}", (r.1).0);
                return Some(r);
            }
            Err(ClientError(ClientErrorKind::Conflict(msg), _)) => {
                warn!("{}. Maybe there are no shards to read from left. Waiting 10 seconds.",
                      msg);
                let pause = Duration::from_secs(10);
                thread::sleep(pause);
            }
            Err(err) => {
                let pause = retry_pause(attempt - 1);
                error!("Failed to connect to Nakadi. Waiting {} ms: {}", duration_to_millis(pause), err);
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
                              is_running: &AtomicBool,
                              metrics: &CheckpointingMetrics) {
    let started_at = Instant::now();
    let mut attempt = 0;
    while is_running.load(Ordering::Relaxed) || attempt == 0 {
        if attempt > 0 {
            let pause = retry_pause(attempt - 1);
            thread::sleep(pause)
        }
        attempt += 1;
        match checkpointer.checkpoint(stream_id, subscription_id, cursors) {
            Ok(()) => {
                metrics.checkpointed(started_at);
                return
                }
            Err(err) => {
                if attempt > 5 {
                    metrics.checkpointing_failed(started_at);
                    error!("Finally gave up to checkpoint cursor after {} attempts.",
                           err);
                    return;
                } else {
                    metrics.checkpointing_error();
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
mod test;
