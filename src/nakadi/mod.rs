/// Describes what to do after a batch has been processed.
///
/// Use to control what should happen next.
use nakadi::model::SubscriptionId;
use nakadi::api_client::ApiClient;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

use failure::*;

use nakadi::handler::HandlerFactory;
use nakadi::streaming_client::StreamingClient;

pub mod handler;
pub mod consumer;
pub mod model;
pub mod streaming_client;
pub mod committer;
pub mod worker;
pub mod batch;
pub mod dispatcher;
pub mod publisher;
pub mod api_client;

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
    /// Commit latest after N events
    EveryNEvents(u16),
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
        self.state.1.store(false, Ordering::Relaxed)
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

pub struct Nakadion {
    guard: Arc<DropGuard>,
}

struct DropGuard {
    consumer: consumer::Consumer,
}

impl DropGuard {
    fn running(&self) -> bool {
        self.consumer.running()
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.consumer.stop()
    }
}

impl Nakadion {
    pub fn start<HF, C, A>(
        streaming_client: C,
        api_client: A,
        subscription_id: SubscriptionId,
        handler_factory: HF,
        commit_strategy: CommitStrategy,
    ) -> Result<Nakadion, Error>
    where
        C: StreamingClient + Clone + Sync + Send + 'static,
        A: ApiClient + Clone + Sync + Send + 'static,
        HF: HandlerFactory + Sync + Send + 'static,
    {
        let consumer = consumer::Consumer::start(
            streaming_client,
            api_client,
            subscription_id,
            handler_factory,
            commit_strategy,
        );

        let guard = Arc::new(DropGuard { consumer });
        Ok(Nakadion { guard })
    }

    pub fn running(&self) -> bool {
        self.guard.consumer.running()
    }

    pub fn stop(&self) {
        self.guard.consumer.stop()
    }

    pub fn block(&self) {
        self.block_with_interval(Duration::from_secs(1))
    }

    pub fn block_with_interval(&self, poll_interval: Duration) {
        while self.running() {
            thread::sleep(poll_interval);
        }
    }
}
