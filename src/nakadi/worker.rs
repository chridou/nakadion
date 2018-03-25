//! Processing a partition
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use failure::*;

use nakadi::Lifecycle;
use nakadi::batch::Batch;
use nakadi::committer::Committer;
use nakadi::handler::{BatchHandler, ProcessingStatus};
use nakadi::metrics::MetricsCollector;
use nakadi::model::EventType;
use nakadi::model::PartitionId;

/// A worker is responsible for executing a handler on a given
/// partition. A worker guarantees that its `BatchHandler`
/// is always executed on at most one thread at a time.
pub struct Worker {
    /// Send batches with this sender
    sender: mpsc::Sender<Batch>,
    lifecycle: Lifecycle,
    /// The partition this worker is responsible for.
    partition: PartitionId,
}

impl Worker {
    /// Start the worker.
    ///
    /// It will run until stop is called or the `BatchHandler` fails.
    pub fn start<H, M>(
        handler: H,
        committer: Committer,
        partition: PartitionId,
        metrics_collector: M,
    ) -> Worker
    where
        H: BatchHandler + Send + 'static,
        M: MetricsCollector + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();

        let lifecycle = Lifecycle::default();

        let handle = Worker {
            lifecycle: lifecycle.clone(),
            sender,
            partition: partition.clone(),
        };

        start_handler_loop(
            receiver,
            lifecycle,
            partition,
            handler,
            committer,
            metrics_collector,
        );

        handle
    }

    /// Returns true if the `Worker` is still running
    pub fn running(&self) -> bool {
        self.lifecycle.running()
    }

    /// Request the worker to stop.
    ///
    /// This does not necessarily cause the worker to stop
    /// immediately. Poll `self::running()` until the worker has
    /// stopped if you depend on the fact that the worker reales stopped working.
    pub fn stop(&self) {
        self.lifecycle.request_abort()
    }

    /// Process the batch.
    pub fn process(&self, batch: Batch) -> Result<(), Error> {
        Ok(self.sender.send(batch).context(format!(
            "[Worker, partition={}] Could not process batch. Worker possibly closed.",
            self.partition
        ))?)
    }

    // The partition this worker is processing
    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }
}

fn start_handler_loop<H, M>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: Lifecycle,
    partition: PartitionId,
    handler: H,
    committer: Committer,
    metrics_collector: M,
) where
    H: BatchHandler + Send + 'static,
    M: MetricsCollector + Send + 'static,
{
    thread::spawn(move || {
        handler_loop(
            receiver,
            &lifecycle,
            partition,
            handler,
            committer,
            metrics_collector,
        )
    });
}

fn handler_loop<H, M>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: &Lifecycle,
    partition: PartitionId,
    handler: H,
    committer: Committer,
    metrics_collector: M,
) where
    H: BatchHandler,
    M: MetricsCollector,
{
    let stream_id = committer.stream_id().clone();
    let mut handler = handler;

    info!(
        "[Worker, stream={}, partition={}] Started.",
        stream_id, partition
    );
    metrics_collector.worker_worker_started();

    loop {
        if lifecycle.abort_requested() {
            info!(
                "[Worker, stream={}, partition={}] Stop requested externally.",
                stream_id, partition
            );
            break;
        }

        let batch = match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(batch) => batch,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                info!(
                    "[Worker, stream={}, partition={}] Channel disconnected. Stopping.",
                    stream_id, partition
                );
                break;
            }
        };

        let maybe_a_handler_result = {
            let event_type = match batch.batch_line.event_type_str() {
                Ok(et) => EventType::new(et),
                Err(err) => {
                    error!(
                        "[Worker, stream={}, partition={}] Invalid event type. Stopping: {}",
                        stream_id, partition, err
                    );
                    break;
                }
            };

            batch.batch_line.events().map(|events| {
                metrics_collector.worker_batch_size_bytes(events.len());
                let start = Instant::now();
                let res = handler.handle(event_type, events);
                metrics_collector.worker_batch_processed(start);
                res
            })
        };

        if let Some(handler_result) = maybe_a_handler_result {
            match handler_result {
                ProcessingStatus::Processed(num_events_hint) => {
                    num_events_hint
                        .iter()
                        .for_each(|n| metrics_collector.worker_events_in_same_batch_processed(*n));
                    match committer.commit(batch, num_events_hint) {
                        Ok(()) => continue,
                        Err(err) => {
                            warn!(
                                "[Worker, stream={}, partition={}] \
                                 Failed to commit. Stopping: {}",
                                stream_id, partition, err
                            );
                            break;
                        }
                    }
                }
                ProcessingStatus::Failed { reason } => {
                    warn!(
                        "[Worker, stream={}, partition={}] Stopping for reason '{}'",
                        stream_id, partition, reason
                    );
                    break;
                }
            }
        } else {
            warn!(
                "[Worker, stream={}, partition={}] \
                 Received batch without events.",
                stream_id, partition
            );
            continue;
        }
    }

    lifecycle.stopped();
    metrics_collector.worker_worker_stopped();

    info!(
        "[Worker, stream={}, partition={}] Stopped.",
        stream_id, partition
    );
}
