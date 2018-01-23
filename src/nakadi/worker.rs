use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use nakadi::BatchHandler;
use nakadi::batch::Batch;
use nakadi::model::EventType;
use nakadi::committer::Committer;

#[derive(Clone)]
pub struct WorkerHandle {
    /// Send batches with this sender
    sender: mpsc::Sender<Batch>,
    /// Set by the worker to indicate whether it is running or not
    is_running: Arc<AtomicBool>,
    /// Queried by the worker to determine whether it should stop
    is_stop_requested: Arc<AtomicBool>,
}

impl WorkerHandle {
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.is_stop_requested.store(true, Ordering::Relaxed);
    }

    pub fn handle(&self, batch: Batch) -> Result<(), String> {
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

pub struct Worker {
    handle: WorkerHandle,
    committer: Committer,
}

impl Worker
{
    pub fn run<H: BatchHandler + Send + 'static>(handler: H, committer: Committer, partition: String) -> WorkerHandle {
        let (sender, receiver) = mpsc::channel();

         let   is_running = Arc::new(AtomicBool::new(true));
         let   is_stop_requested = Arc::new(AtomicBool::new(false));

        let handle = WorkerHandle {
            is_running: is_running.clone(),
            is_stop_requested: Arc::new(AtomicBool::new(false)),
            sender,
        };

        let worker = Worker {
            committer,
            handle: handle.clone(),
        };

        start_handler_loop(
            receiver,
            is_stop_requested,
            is_running,
            partition,
            handler,
            );

        handle
    }
}

fn start_handler_loop<H: BatchHandler + Send + 'static>(
    receiver: mpsc::Receiver<Batch>,
    is_stop_requested: Arc<AtomicBool>,
    is_running: Arc<AtomicBool>,
    partition: String,
    handler: H,

) {
    thread::spawn(move || handler_loop(receiver, &is_stop_requested, &is_running, &partition, handler));
}

fn handler_loop<H: BatchHandler>(
    receiver: mpsc::Receiver<Batch>,
    is_stop_requested: &AtomicBool,
    is_running: &AtomicBool,
    partition: &str,
        handler: H,

) {
    loop {
        if (is_stop_requested.load(Ordering::Relaxed)) {
            info!("Worker for partition '{}': Stop reqeusted externally.", partition);
            break;
        }

        let batch = match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(batch) => batch,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                info!("Worker for partition '{}': Channel disconnected. Stopping.", partition);
                break;
            }
        };

        let event_type = match batch.batch_line.event_type_str() {
            Ok(et) => EventType::new(et),
            Err(err) => {
                error!("Worker for partition '{}':Invalid event type. Stopping: {}", partition, err);
                break;
            }
        };

        if let Some(events) = batch.batch_line.events()  {
            match handler.handle(event_type, events) {
                _ => unimplemented!()
            }
        } else {
            warn!("Worker for partition '{}': Received batch without events.", partition);
            continue;
        }
    }

    is_running.store(false, Ordering::Relaxed);

    info!("Worker for partition '{}': Stopped.", partition);
}
