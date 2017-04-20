//! # Nakadion
//!
//! A client for the [Nakadi](https://github.com/zalando/nakadi) Event Broker.
//!
//! Nakadion uses the [Subscription API](https://github.com/zalando/nakadi#subscriptions)
//! of Nakadi on the consuming side.
//!
//! ## Documentation
//!
//! Documenatation can be found on [docs.rs](https://docs.rs)
//!
//! ## Performance
//!
//! This library is not meant to be used in a high performance scenario.
//! It uses synchronous IO and lacks optimizations.
//!
//! ## License
//!
//! Nakadion is distributed under the terms of both the MIT license
//! and the Apache License (Version 2.0).
//!
//! See LICENSE-APACHE and LICENSE-MIT for details.
#![recursion_limit = "1024"]

#[macro_use]
extern crate log;

extern crate uuid;
extern crate url;
#[macro_use]
extern crate hyper;
extern crate hyper_native_tls;
#[macro_use]
extern crate derive_builder;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

#[macro_use]
extern crate error_chain;

extern crate metrics as libmetrics;
extern crate histogram;

use std::sync::Arc;
use std::thread::JoinHandle;

use uuid::Uuid;

mod tokenerrors;
pub mod metrics;
mod clienterrors;
mod connector;
mod worker;

pub use tokenerrors::*;
pub use clienterrors::*;
pub use self::connector::{NakadiConnector, Checkpoints, ReadsStream, HyperClientConnector,
                          ConnectorSettings, ConnectorSettingsBuilder};
pub use self::worker::NakadiWorker;

/// A token used for authentication against `Nakadi`.
#[derive(Clone, Debug)]
pub struct Token(pub String);

impl Token {
    /// Creates a new token.
    pub fn new<T: Into<String>>(token: T) -> Token {
        Token(token.into())
    }
}

/// Provides a `Token`.
///
/// Authentication can be disabled by returning `None` on `get_token`.
pub trait ProvidesToken: Send + Sync + 'static {
    /// Get a new `Token`. Return `None` to disable authentication.
    fn get_token(&self) -> TokenResult<Option<Token>>;
}

/// The [`Nakadi Event Type`](https://github.com/zalando/nakadi#creating-event-types).
/// Similiar to a topic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventType(pub String);

impl EventType {
    /// Creates a new instance of an
    /// [`EventType`](https://github.com/zalando/nakadi#creating-event-types).
    pub fn new<T: Into<String>>(value: T) -> EventType {
        EventType(value.into())
    }
}

/// A `SubscriptionId` is used to guaratee a continous flow of events for a client.
#[derive(Clone, Debug)]
pub struct SubscriptionId(pub Uuid);

impl SubscriptionId {
    pub fn nil() -> Self {
        SubscriptionId(Uuid::nil())
    }
}

/// A `StreamId` identifies a subscription. It must be provided for checkpointing with a `Cursor`.
#[derive(Clone, Debug)]
pub struct StreamId(pub String);

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

pub struct BatchInfo {
    pub stream_id: StreamId,
    pub cursor: Cursor,
}

/// Describes what to do after a batch has been processed.
///
/// Use to control what should happen next.
#[derive(Debug, Clone)]
pub enum AfterBatchAction {
    /// Checkpoint and get next
    Continue,
    /// Get next without checkpointing.
    ContinueNoCheckpoint,
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
    fn handle(&self, batch: &str, info: BatchInfo) -> AfterBatchAction;
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
