//! Use the `NakadiClient` to consume events from `Nakadi`
//!
//! The `NakadiClient` the
//! [`Nakadi Subscription API`](https://github.com/zalando/nakadi#subscriptions).
use std::sync::Arc;
use std::thread::JoinHandle;

use uuid::Uuid;

use super::EventType;

mod clienterrors;
mod connector;
mod worker;

pub use self::connector::{NakadiConnector, Checkpoints, ReadsStream, HyperClientConnector,
                          ConnectorSettings, ConnectorSettingsBuilder};
pub use self::clienterrors::*;
pub use self::worker::NakadiWorker;

/// A `SubscriptionId` is used to guaratee a continous flow of events for a client.
#[derive(Clone, Debug)]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    pub fn nil() -> Self {
        SubscriptionId(Uuid::nil())
    }
}

/// A `StreamId` identifies a subscription. It must be provided for checkpointing with a `Cursor`.
pub struct StreamId(String);

impl StreamId {
    pub fn new<T: Into<String>>(id: T) -> Self {
        StreamId(id.into())
    }
}

/// A `Cursor` describes a position in the stream. The cursor is used for checkpointing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cursor {
    pub partition: usize,
    pub offset: String,
    pub event_type: EventType,
    pub cursor_token: Uuid,
}

/// Describes what to do after a batch has been processed.
///
/// Use to control what should happen next.
#[derive(Debug, Clone)]
pub enum AfterBatchAction {
    /// Checkpoint and get next
    Continue,
    /// Checkpoint then stop.
    Stop,
    /// Stop without checkpointing
    Abort,
}

/// Handles batches of events received from `Nakadi`.
pub trait Handler: Send + Sync + 'static {
    /// Handle the batch of events. The supplied string contains
    /// the whole batch of events as a `JSOS` array.
    /// Return an `AfterBatchAction` to tell what to do next.
    /// The batch array may be empty.
    /// You may not panic within the handler.
    fn handle(&self, batch: &str) -> AfterBatchAction;
}

/// The client to consume events from `Nakadi`
pub struct NakadiClient<C: NakadiConnector> {
    worker: NakadiWorker,
    connector: Arc<C>,
}

impl<C: NakadiConnector> NakadiClient<C> {
    /// Creates a new instance. The returned `JoinHandle` can
    /// be used to synchronize with the underlying worker.
    /// The underlying worker will be stopped once the client is dropped.
    pub fn new<H: Handler>(subscription_id: SubscriptionId,
                           connector: Arc<C>,
                           handler: H)
                           -> (Self, JoinHandle<()>) {
        let (worker, handle) = NakadiWorker::new(connector.clone(), handler, subscription_id);
        (NakadiClient {
             worker: worker,
             connector: connector,
         },
         handle)
    }

    /// Returns true if the underlying `NakadiWorker` is still running.
    pub fn is_running(&self) -> bool {
        self.worker.is_running()
    }

    /// Stop the underlying `NakadiWorker`.
    pub fn stop(&self) {
        self.worker.stop();
    }

    /// Get access to the underlying `NakadiConnector`.
    pub fn connector(&self) -> &C {
        &self.connector
    }

    /// Return the `SubscriptionId` this `NakadiClient` is listening to.
    pub fn subscription_id(&self) -> &SubscriptionId {
        self.worker.subscription_id()
    }
}
