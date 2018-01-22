use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use subscription::batch::Batch;
use subscription::model::BatchHandler;
use subscription::committer::Committer;

#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<Batch>,
    is_running: Arc<AtomicBool>,
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

pub struct Worker<H> {
    handle: WorkerHandle,
    handler: H,
    committer: Committer,
}

impl<H> Worker<H>
where
    H: BatchHandler,
{
    pub fn run(handler: H, committer: Committer, partition: String) -> WorkerHandle {
        let (sender, rx) = mpsc::channel();

        let handle = WorkerHandle {
            is_running: Arc::new(AtomicBool::new(true)),
            is_stop_requested: Arc::new(AtomicBool::new(false)),
            sender,
        };

        let worker = Worker {
            handler,
            committer,
            handle: handle.clone(),
        };

        handle
    }
}

fn start_handler_loop(
    receiver: mpsc::Receiver<Batch>,
    is_stop_requested: Arc<AtomicBool>,
    is_running: Arc<AtomicBool>,
    partition: String,
) {
    thread::spawn(move || handler_loop(receiver, &is_stop_requested, &is_running, &partition));
}

fn handler_loop(
    receiver: mpsc::Receiver<Batch>,
    is_stop_requested: &AtomicBool,
    is_running: &AtomicBool,
    partition: &str,
) {
    loop {
        if (is_stop_requested.load(Ordering::Relaxed)) {
            break;
        }
    }

    is_running.store(false, Ordering::Relaxed);

    info!("Worker for partition '{}' shut down.", partition);
}
