//! The components to consume from the stream.
//!
//! This is basically the machinery that drives the consumption.
//! It will consume events and call the `Handler`
//! and react on its commands on how to continue.
use std::sync::Arc;
use std::thread::JoinHandle;
use std::env::{self, VarError};

use super::*;
use super::connector::NakadiConnector;

mod concurrentworker;
mod sequentialworker;

pub use self::concurrentworker::{ConcurrentWorkerSettings, ConcurrentWorkerSettingsBuilder};
pub use self::sequentialworker::SequentialWorkerSettings;

/// Settings for the worker.
///
/// Will determine whether the worker is sequential or concurrent.
pub enum WorkerSettings {
    Sequential(SequentialWorkerSettings),
    Concurrent(ConcurrentWorkerSettings),
}

impl WorkerSettings {
    /// Creates a configuration from environment variables.
    ///
    /// The environment variable `NAKADION_USE_CONCURRENT_WORKER` will determine
    /// whether the worker will be sequential or concurrent.
    /// The default if the variable can not be found is `false`.
    ///
    /// You must provide all the necessary environment variables for settings
    /// you want to create.
    pub fn from_env() -> Result<Self, String> {
        let use_concurrent_worker = match env::var("NAKADION_USE_CONCURRENT_WORKER") {
            Ok(env_val) => {
                match env_val.parse() {
                    Ok(v) => v,
                    Err(err) => {
                        return Err(format!("Could not parse env var \
                                            'NAKADION_USE_CONCURRENT_WORKER' as a bool: {}",
                                           err))
                    }
                }
            }
            Err(VarError::NotPresent) => {
                warn!("Env var 'NAKADION_USE_CONCURRENT_WORKER' \
                       not found. Default is 'false'.");
                false
            }
            Err(err) => {
                return Err(format!("Could not read env var 'NAKADION_USE_CONCURRENT_WORKER': {}",
                                   err))
            }
        };
        let settings = if use_concurrent_worker {
            WorkerSettings::Concurrent(concurrentworker::ConcurrentWorkerSettings::from_env()?)
        } else {
            WorkerSettings::Sequential(sequentialworker::SequentialWorkerSettings::from_env()?)
        };
        Ok(settings)
    }
}

/// Common fuctionality a worker must provide.
pub trait Worker {
    /// Returns true if the worker is still running.
    fn is_running(&self) -> bool;

    /// Stops the worker.
    fn stop(&self);

    /// Gets the `SubscriptionId` the worker is listening to.
    fn subscription_id(&self) -> &SubscriptionId;
}

/// The `NakadiWorker` runs the consumption of events.
pub struct NakadiWorker {
    worker: Box<Worker + Sync + Send + 'static>,
}

impl NakadiWorker {
    /// Creates a new instance. The returned `JoinHandle` can
    /// be used to synchronize with the underlying worker thread.
    /// The underlying worker will be stopped once the worker is dropped.
    ///
    /// The `WorkerSettings` will determine what kind of worker is created.
    pub fn new<C: NakadiConnector, H: Handler + 'static>
        (connector: Arc<C>,
         handler: H,
         subscription_id: SubscriptionId,
         settings: WorkerSettings)
         -> Result<(NakadiWorker, JoinHandle<()>), String> {

        let (worker, handle) = match settings {
            WorkerSettings::Sequential(settings) => {
                let (worker, handle) = sequentialworker::SequentialWorker::new(connector,
                                                                               handler,
                                                                               subscription_id,
                                                                               settings);
                (Box::new(worker) as Box<Worker + Send + Sync + 'static>, handle)
            }
            WorkerSettings::Concurrent(settings) => {
                let (worker, handle) = concurrentworker::ConcurrentWorker::new(connector,
                                                                               handler,
                                                                               subscription_id,
                                                                               settings)?;
                (Box::new(worker) as Box<Worker + Send + Sync + 'static>, handle)
            }
        };

        Ok((NakadiWorker { worker: worker }, handle))
    }
}

impl Worker for NakadiWorker {
    fn is_running(&self) -> bool {
        self.worker.is_running()
    }

    fn stop(&self) {
        self.worker.stop()
    }

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
