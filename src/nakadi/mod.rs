/// Describes what to do after a batch has been processed.
///
/// Use to control what should happen next.
use nakadi::handler::HandlerFactory;
use nakadi::client::StreamingClient;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

pub mod handler;
pub mod consumer;
pub mod model;
pub mod client;
pub mod committer;
pub mod worker;
pub mod batch;
pub mod dispatcher;
pub mod maintenance;
pub mod publisher;

/// Stragtegy for committing cursors
#[derive(Clone, Copy)]
pub enum CommitStrategy {
    /// Commit all cursors immediately
    AllBatches,
    /// Commit as late as possile
    MaxAge,
    /// Commit latest after N seconds
    EveryNSeconds(u16),
    /// Commit latest after N batches
    EveryNBatches(u16),
}

#[derive(Clone)]
pub struct Lifecycle {
    state: Arc<(AtomicBool, AtomicBool)>,
}

impl Lifecycle {
    pub fn abort_requested(&self) -> bool {
        self.state.0.load(Ordering::Relaxed)
    }

    pub fn request_abort(&self) {
        self.state.0.store(true, Ordering::Relaxed)
    }

    pub fn stopped(&self) {
        self.state.1.store(true, Ordering::Relaxed)
    }

    pub fn running(&self) -> bool {
        self.state.1.load(Ordering::Relaxed)
    }
}

impl Default for Lifecycle {
    fn default() -> Lifecycle {
        Lifecycle {
            state: Arc::new((AtomicBool::new(false), AtomicBool::new(true))),
        }
    }
}

#[derive(Clone)]
pub struct InFlightCounter {
    in_flight: Arc<AtomicIsize>,
}

impl InFlightCounter {
    pub fn inc(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn limit_reached(&self, limit: isize) -> bool {
        self.in_flight.load(Ordering::Relaxed) >= limit
    }
}

impl Default for InFlightCounter {
    fn default() -> InFlightCounter {
        InFlightCounter {
            in_flight: Arc::new(AtomicIsize::new(0)),
        }
    }
}

pub struct NakadionConfig {
    commit_strategy: CommitStrategy,
    max_in_flight: u64,
}

pub struct Nakadion {
    guard: Arc<DropGuard>,
}

struct DropGuard {
    consumer: consumer::Consumer,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.consumer.stop()
    }
}

impl Nakadion {
    pub fn start<HF, C>(
        config: NakadionConfig,
        client: C,
        handler_factory: HF,
    ) -> Result<Nakadion, String>
    where
        C: StreamingClient + Clone + Sync + Send + 'static,
        HF: HandlerFactory + Sync + Send + 'static,
    {
        let consumer = consumer::Consumer::start(client, handler_factory, config.commit_strategy);

        let guard = Arc::new(DropGuard { consumer });
        Ok(Nakadion { guard })
    }

    pub fn running(&self) -> bool {
        self.guard.consumer.running()
    }

    pub fn stop(&self) {
        self.guard.consumer.stop()
    }
}
