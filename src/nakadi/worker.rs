use nakadi::Lifecycle;
use nakadi::model::PartitionId;
use nakadi::model::StreamId;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use nakadi::handler::{AfterBatchAction, BatchHandler};
use nakadi::batch::Batch;
use nakadi::model::EventType;
use nakadi::committer::Committer;

pub struct Worker {
    /// Send batches with this sender
    sender: mpsc::Sender<Batch>,
    lifecycle: Lifecycle,
    /// The partition this worker is responsible for.
    partition: PartitionId,
    stream_id: StreamId,
}

impl Worker {
    pub fn start<H: BatchHandler + Send + 'static>(
        handler: H,
        committer: Committer,
        partition: PartitionId,
    ) -> Worker {
        let (sender, receiver) = mpsc::channel();

        let lifecycle = Lifecycle::default();

        let handle = Worker {
            lifecycle: lifecycle.clone(),
            sender,
            partition: partition.clone(),
            stream_id: committer.stream_id.clone(),
        };

        start_handler_loop(receiver, lifecycle, partition, handler, committer);

        handle
    }

    pub fn running(&self) -> bool {
        self.lifecycle.running()
    }

    pub fn stop(&self) {
        self.lifecycle.request_abort()
    }

    pub fn process(&self, batch: Batch) -> Result<(), String> {
        if let Err(err) = self.sender.send(batch) {
            Err(format!(
                "Could not process batch. Worker possibly closed: {}",
                err
            ))
        } else {
            Ok(())
        }
    }

    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }
}

fn start_handler_loop<H: BatchHandler + Send + 'static>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: Lifecycle,
    partition: PartitionId,
    handler: H,
    committer: Committer,
) {
    thread::spawn(move || handler_loop(receiver, &lifecycle, partition, handler, committer));
}

fn handler_loop<H: BatchHandler>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: &Lifecycle,
    partition: PartitionId,
    handler: H,
    committer: Committer,
) {
    let stream_id = committer.stream_id.clone();

    info!(
        "Worker on stream '{}' for partition '{}': Started.",
        &committer.stream_id, partition
    );
    loop {
        if lifecycle.abort_requested() {
            info!(
                "Worker on stream '{}' for partition '{}': Stop reqeusted externally.",
                &committer.stream_id, partition
            );
            break;
        }

        let batch = match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(batch) => batch,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                info!(
                    "Worker on stream '{}' for partition '{}': Channel disconnected. Stopping.",
                    &committer.stream_id, partition
                );
                break;
            }
        };

        let maybe_a_handler_result = {
            let event_type = match batch.batch_line.event_type_str() {
                Ok(et) => EventType::new(et),
                Err(err) => {
                    error!(
                        "Worker on stream '{}' for partition '{}': Invalid event type. Stopping: {}",
                     &committer.stream_id,
                      partition,
                       err);
                    break;
                }
            };

            batch
                .batch_line
                .events()
                .map(|events| handler.handle(event_type, events))
        };

        if let Some(handler_result) = maybe_a_handler_result {
            match handler_result {
                AfterBatchAction::Continue => match committer.commit(batch) {
                    Ok(()) => continue,
                    Err(err) => {
                        warn!(
                            "Worker on stream '{}' for partition '{}': \
                             Failed to commit. Stopping: {}",
                            &committer.stream_id, partition, err
                        );
                        break;
                    }
                },
                AfterBatchAction::Abort { reason } => {
                    warn!(
                        "Worker on stream '{}' for partition '{}' stopping: {}",
                        &committer.stream_id, partition, reason
                    );
                    break;
                }
            }
        } else {
            warn!(
                "Worker on stream '{}' for partition '{}': \
                 Received batch without events.",
                &committer.stream_id, partition
            );
            continue;
        }
    }

    lifecycle.stopped();

    info!(
        "Worker on stream '{}' for partition '{}': Stopped.",
        &committer.stream_id, partition
    );
}
