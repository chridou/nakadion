//! The processor orchestrates the workers

use nakadi::worker::Worker;
use nakadi::model::PartitionId;
use nakadi::committer::Committer;
use nakadi::HandlerFactory;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use nakadi::batch::Batch;
use std::time::Duration;
use std::thread;
use std::sync::mpsc;

/// The dispatcher takes batch lines and sends them to the workers.
pub struct Dispatcher {
    /// Send batches with this sender
    sender: mpsc::Sender<Batch>,
    /// Set by the worker to indicate whether it is running or not
    ///
    /// Starts with true and once it is false the worker has stooped
    /// working.
    is_running: Arc<AtomicBool>,
    /// Queried by the worker to determine whether it should stop
    is_stop_requested: Arc<AtomicBool>,
}

impl Dispatcher {
    pub fn start<HF: HandlerFactory + Send + 'static>(
        handler_factory: HF,
        committer: Committer,
    ) -> Dispatcher {
        let (sender, receiver) = mpsc::channel();

        let is_running = Arc::new(AtomicBool::new(true));
        let is_stop_requested = Arc::new(AtomicBool::new(false));

        let handle = Dispatcher {
            is_running: is_running.clone(),
            is_stop_requested: Arc::new(AtomicBool::new(false)),
            sender,
        };

        start_handler_loop(
            receiver,
            is_stop_requested,
            is_running,
            handler_factory,
            committer,
        );

        handle
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.is_stop_requested.store(true, Ordering::Relaxed);
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

fn start_handler_loop<HF: HandlerFactory + Send + 'static>(
    receiver: mpsc::Receiver<Batch>,
    is_stop_requested: Arc<AtomicBool>,
    is_running: Arc<AtomicBool>,
    handler_factory: HF,
    committer: Committer,
) {
    thread::spawn(move || {
        handler_loop(
            receiver,
            &is_stop_requested,
            &is_running,
            handler_factory,
            committer,
        )
    });
}

fn handler_loop<HF: HandlerFactory>(
    receiver: mpsc::Receiver<Batch>,
    is_stop_requested: &AtomicBool,
    is_running: &AtomicBool,
    handler_factory: HF,
    committer: Committer,
) {
    let stream_id = committer.stream_id.clone();
    let mut workers: Vec<Worker> = Vec::new();

    info!("Processor on stream '{}' Started.", &committer.stream_id,);
    loop {
        if is_stop_requested.load(Ordering::Relaxed) {
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

    is_running.store(false, Ordering::Relaxed);

    info!("Processor on stream '{}': Stopped.", stream_id);
}
