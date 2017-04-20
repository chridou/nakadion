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

mod partitionworker;

pub enum WorkerMessage {
    Stop,
    Batch(BatchInfo, String),
}

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
    pub fn new<C: NakadiConnector, H: Handler + 'static>(connector: Arc<C>,
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

fn start_nakadi_worker_loop<C: NakadiConnector, H: Handler + 'static>(connector: Arc<C>,
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
        info!("No connection to Nakadi. Requesting connection...");
        let (src, stream_id) = if let Some(r) = connect(connector, subscription_id, &is_running) {
            r
        } else {
            warn!("Connection attempt aborted. Stopping the worker.");
            break;
        };

        let buffered_reader = BufReader::new(src);

        let mut lines_read: usize = 0;
        for line in buffered_reader.lines() {
            match line {
                Ok(line) => {
                    lines_read += 1;
                    match process_line(connector,
                                       line.as_ref(),
                                       &handler,
                                       &stream_id,
                                       subscription_id,
                                       &is_running) {
                        Ok(AfterBatchAction::Continue) => (),
                        Ok(AfterBatchAction::ContinueNoCheckpoint) => (),
                        Ok(leaving_action) => {
                            info!("Leaving worker loop  for stream {} on user request: {:?}",
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
                                is_running: &AtomicBool)
                                -> ClientResult<AfterBatchAction> {
    match serde_json::from_str::<DeserializedBatch>(line) {
        Ok(DeserializedBatch { cursor, events }) => {
            // This is a hack. We might later want to extract the slice manually.
            let events_json = events.unwrap_or(Vec::new());
            let events_str = serde_json::to_string(events_json.as_slice()).unwrap();
            let batch_info = BatchInfo {
                stream_id: stream_id.clone(),
                cursor: cursor.clone(),
            };
            unimplemented!()
        }
        Err(err) => {
            bail!(ClientErrorKind::UnparsableBatch(format!("Could not parse '{}': {}", line, err)))
        }
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
                error!("Failed to connect to Nakadi: {}", err);
                let pause = retry_pause(attempt - 1);
                thread::sleep(pause);
            }
        }
    }
    None
}

fn retry_pause(retry: usize) -> Duration {
    let idx = ::std::cmp::min(retry, RETRY_MILLIS.len() - 1);
    ::std::time::Duration::from_millis(RETRY_MILLIS[idx])
}

#[cfg(test)]
mod test;
