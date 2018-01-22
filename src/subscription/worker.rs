/*use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nakadi::batch::{SubscriptionBatchLine, SubscriptionBatch};
use nakadi::subscription::model::BatchHandler;
use nakadi::subscription::committer::Committer;

#[derive(Clone)]
pub struct WorkerHandle<B: SubscriptionBatchLine> {
    sender: mpsc::Sender<SubscriptionBatch<B>>,
    is_running: Arc<AtomicBool>,
    is_stop_requested: Arc<AtomicBool>
}

impl <B: SubscriptionBatchLine + Clone> WorkerHandle<B> {
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.is_stop_requested.store(true, Ordering::Relaxed);
    }
}

pub struct Worker<H, B: SubscriptionBatchLine> {
    handle: WorkerHandle<B>,
    handler: H,
    committer: Committer<B>,
}

impl<H, B> Worker<H,B> where 
H: BatchHandler,
B: SubscriptionBatchLine
 {
     pub fn run(handler: H, committer: Committer<SubscriptionBatch<B>>) -> WorkerHandle<B> {
         let (sender, rx) = mpsc::channel();
         
         let handle = WorkerHandle {
             is_running: Arc::new(AtomicBool::new(true)),
             is_stop_requested: Arc::new(AtomicBool::new(false)),
             sender
         };

         let worker = Worker {
              handler,
             committer,
            handle : handle.clone()
         };

         handle
     }




}*/