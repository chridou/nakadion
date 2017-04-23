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

mod concurrentworker;
mod sequentialworker;

pub use self::concurrentworker::ConcurrentWorkerSettings;
pub use self::sequentialworker::SequentialWorkerSettings;

pub enum WorkerSettings {
    Sequential(SequentialWorkerSettings),
    Concurrent(ConcurrentWorkerSettings)
}

impl WorkerSettings {
    pub fn from_env() -> Result<Self, String> {
        unimplemented!()
    }
}

pub trait Worker {
     /// Returns true if the worker is still running.
    fn is_running(&self) -> bool;

    /// Stops the worker.
    fn stop(&self);

    /// Gets the `SubscriptionId` the worker is listening to.
    fn subscription_id(&self) -> &SubscriptionId;
   
}

/// The worker runs the consumption of events.
/// It will try to reconnect automatically once the stream breaks.
pub struct NakadiWorker {
    worker: Box<Worker>,
}

impl NakadiWorker {
    /// Creates a new instance. The returned `JoinHandle` can
    /// be used to synchronize with the underlying worker thread.
    /// The underlying worker will be stopped once the worker is dropped.
    pub fn new<C: NakadiConnector, H: Handler + 'static>(connector: Arc<C>,
                                                         handler: H,
                                                         subscription_id: SubscriptionId,
                                                         settings: WorkerSettings)
                                                         -> Result<(NakadiWorker, JoinHandle<()>), String> {

        let (worker, handle) = match settings {
            WorkerSettings::Sequential(settings) =>  {      
                let (worker, handle) = sequentialworker::SequentialWorker::new(connector, handler, subscription_id, settings);
                (Box::new(worker) as Box<Worker>, handle)
            }
            WorkerSettings::Concurrent(settings) =>  {      
                let (worker, handle) = concurrentworker::Leader::new(connector, handler, subscription_id, settings)?;
                (Box::new(worker) as Box<Worker>, handle)
            }
        };
 
        Ok((NakadiWorker {
            worker: worker,
        },
         handle))
    }
}

impl Worker for NakadiWorker {

    /// Returns true if the worker is still running.
    fn is_running(&self) -> bool {
        self.worker.is_running()
    }

    /// Stops the worker.
    fn stop(&self) {
        self.worker.stop()
    }

    /// Gets the `SubscriptionId` the worker is listening to.
    fn subscription_id(&self) -> &SubscriptionId {
        self.worker.subscription_id()
    }
}

impl Drop for NakadiWorker {
    fn drop(&mut self) {
        info!("Cleanup. Nakadi worker stopping.");
        self.stop();
    }
}


