//! The processor orchestrates the workers

use std::sync::Arc;
use nakadi::Lifecycle;
use nakadi::worker::Worker;
use nakadi::model::PartitionId;
use nakadi::committer::Committer;
use nakadi::handler::HandlerFactory;
use nakadi::batch::Batch;
use std::time::Duration;
use std::thread;
use std::sync::mpsc;

/// The dispatcher takes batch lines and sends them to the workers.
pub struct Dispatcher {
    /// Send batches with this sender
    sender: mpsc::Sender<Batch>,
    lifecycle: Lifecycle,
}

impl Dispatcher {
    pub fn start<HF>(handler_factory: Arc<HF>, committer: Committer) -> Dispatcher
    where
        HF: HandlerFactory + Send + Sync + 'static,
    {
        let (sender, receiver) = mpsc::channel();

        let lifecycle = Lifecycle::default();

        let handle = Dispatcher {
            lifecycle: lifecycle.clone(),
            sender,
        };

        start_handler_loop(receiver, lifecycle, handler_factory, committer);

        handle
    }

    pub fn is_running(&self) -> bool {
        self.lifecycle.running()
    }

    pub fn stop(&self) {
        self.lifecycle.request_abort()
    }

    pub fn process(&self, batch: Batch) -> Result<(), String> {
        if let Err(err) = self.sender.send(batch) {
            Err(format!(
                "Could not send batch. Worker possibly closed: {}",
                err
            ))
        } else {
            Ok(())
        }
    }
}

fn start_handler_loop<HF>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: Lifecycle,
    handler_factory: Arc<HF>,
    committer: Committer,
) where
    HF: HandlerFactory + Send + Sync + 'static,
{
    thread::spawn(move || handler_loop(receiver, lifecycle, handler_factory, committer));
}

fn handler_loop<HF>(
    receiver: mpsc::Receiver<Batch>,
    lifecycle: Lifecycle,
    handler_factory: Arc<HF>,
    committer: Committer,
) where
    HF: HandlerFactory,
{
    let stream_id = committer.stream_id().clone();
    let mut workers: Vec<Worker> = Vec::new();

    info!("Processor on stream '{}' Started.", committer.stream_id(),);
    loop {
        if lifecycle.abort_requested() {
            info!(
                "Processor on stream '{}': Stop reqeusted externally.",
                stream_id
            );
            break;
        }

        let batch = match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(batch) => batch,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                info!(
                    "Processor on stream '{}': Channel disconnected. Stopping.",
                    stream_id
                );
                break;
            }
        };

        if batch.batch_line.events().is_none() {
            error!(
                "Processor on stream '{}': Received a keep alive batch!. Stopping.",
                stream_id
            );
            break;
        };

        let partition = match batch.batch_line.partition_str() {
            Ok(partition) => PartitionId(partition.into()),
            Err(err) => {
                error!(
                    "Processor on stream '{}': Partition id not UTF-8!. Stopping. - {}",
                    stream_id, err
                );

                break;
            }
        };

        let worker_idx = workers.iter().position(|w| w.partition() == &partition);

        let worker = if let Some(idx) = worker_idx {
            &workers[idx]
        } else {
            info!(
                "Processor on stream '{}': Creating new worker for partition {}",
                stream_id, partition
            );
            let handler = handler_factory.create_handler();
            let worker = Worker::start(handler, committer.clone(), partition.clone());
            workers.push(worker);
            &workers[workers.len() - 1]
        };

        if let Err(err) = worker.process(batch) {
            error!(
                "Processor on stream '{}': Worker did not accept batch. Stopping. - {}",
                stream_id, err
            );

            break;
        }
    }

    workers.iter().for_each(|w| w.stop());

    info!(
        "Processor on stream '{}': Waiting for workers to stop",
        stream_id
    );

    while workers.iter().any(|w| w.running()) {
        thread::sleep(Duration::from_millis(10));
    }

    info!("Processor on stream '{}': All wokers stopped.", stream_id);

    lifecycle.stopped();
    info!("Processor on stream '{}': Stopped.", stream_id);
}
