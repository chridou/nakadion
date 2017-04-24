//! # Nakadion
//!
//! A client for the [Nakadi](https://github.com/zalando/nakadi) Event Broker.
//!
//! Nakadion uses the Subscription API of Nakadi.
//!
//! ## Consuming
//!
//! Nakadion supports two modes of consuming events. A sequuential one and a cuncurrent one.
//!
//! ### Sequential Consumption
//!
//! In this mode Nakadion will read a batch from Nakadi then call a handler on theese
//! and afterwards try to commit the batch.
//! This mode of operation is simple, straight forward and should be sufficient for most scenarios.
//! No batches are buffered when consuming sequentially.
//!
//! ### Concurrent Consumption
//!
//! **DO NOT USE FOR NOW**
//!
//! ## Configuration
//!
//! Nakadion is configured by environment variables.
//!
//! ### Setting up the subscription
//!
//! * `NAKADION_NAKADI_HOST`: See `ConnectorSettings::nakadi_host`
//! * `NAKADION_MAX_UNCOMMITED_EVENTS`: See `ConnectorSettings::max_uncommitted_events`
//! * `NAKADION_BATCH_LIMIT`: See `ConnectorSettings::batch_limit`
//! * `NAKADION_BATCH_FLUSH_TIMEOUT_SECS`: See `ConnectorSettings::batch_flush_timeout`
//! * `NAKADION_STREAM_TIMEOUT_SECS`: See `ConnectorSettings::stream_timeout`
//! * `NAKADION_STREAM_LIMIT`: See `ConnectorSettings::stream_limit`
//! * `NAKADION_STREAM_KEEP_ALIVE_LIMIT`: See `ConnectorSettings::stream_keep_alive_limit`
//!
//! ### Setting up the Sequential Worker
//!
//! Just set `NAKADION_USE_CONCURRENT_WORKER` to `false` which is also the default.
//!
//! ### Setting up the Concurent Worker:
//!
//! Just set `NAKADION_USE_CONCURRENT_WORKER` to `true`.
//!
//! Configure the worker:
//!
//! * `NAKADION_MAX_WORKERS`: See `ConcurrentWorkerSettings::max_workers`
//! * `NAKADION_WORKER_BUFFER_SIZE`: See `ConcurrentWorkerSettings::worker_buffer_size`
//!
//! In this mode Nakadion will spawn a number of worker threads
//! and distribute work among them based on
//! the `partion id` of a batch. The workers are not dedidacted to a partition.
//! Work is rather distributed based on a hash of the `partition id`.
//!
//! ## Performance
//!
//! This library is not meant to be used in a high performance scenario. It uses synchronous IO.
//!
//! ## Documentation
//!
//! Documenatation can be found at [docs.rs](https://docs.rs/nakadion)
//!
//! ## License
//!
//! Nakadion is distributed under the terms of both the MIT license
//! and the Apache License (Version 2.0).
//!
//! See LICENSE-APACHE and LICENSE-MIT for details.#![recursion_limit = "1024"]
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
use std::env;

use uuid::Uuid;
use serde::{Deserialize, Deserializer};

use worker::{Worker, NakadiWorker, WorkerSettings};

mod tokenerrors;
pub mod metrics;
mod clienterrors;
mod connector;
pub mod worker;

pub use tokenerrors::*;
pub use clienterrors::*;
pub use self::connector::{NakadiConnector, Checkpoints, ReadsStream, HyperClientConnector,
                          ConnectorSettings, ConnectorSettingsBuilder, ProvidesStreamInfo};

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

/// A partition id that comes with a `Cursor`
#[derive(Debug, Clone, Serialize)]
pub struct PartitionId(pub usize);

impl Deserialize for PartitionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        let v = String::deserialize(deserializer)?;
        match v.parse() {
            Ok(v) => Ok(PartitionId(v)),
            Err(err) => {
                Err(serde::de::Error::custom(format!("{} is not a usize required for \
                                                      PartitionId.",
                                                     err)))
            }
        }
    }
}

/// Information on a partition
#[derive(Debug, Deserialize)]
pub struct PartitionInfo {
    partition: PartitionId,
    stream_id: StreamId,
    unconsumed_events: usize,
}

/// An `EventType` can be published on multiple partitions.
#[derive(Debug, Deserialize)]
pub struct EventTypeInfo {
    event_type: EventType,
    partitions: Vec<PartitionInfo>,
}

impl EventTypeInfo {
    /// Returns the number of partitions this `EventType` is
    /// published over.
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }
}

/// A stream can provide multiple `EventTypes` where each of them can have
/// its own partitioning setup.
#[derive(Debug, Deserialize)]
pub struct StreamInfo {
    #[serde(rename="items")]
    event_types: Vec<EventTypeInfo>,
}

impl StreamInfo {
    /// Returns the number of partitions of the `EventType`
    /// that has the most partitions.
    pub fn max_partitions(&self) -> usize {
        self.event_types
            .iter()
            .map(|et| et.num_partitions())
            .max()
            .unwrap_or(0)
    }
}


/// A `StreamId` identifies a subscription. It must be provided for checkpointing with a `Cursor`.
#[derive(Clone, Debug, Deserialize)]
pub struct StreamId(pub String);

impl StreamId {
    pub fn new<T: Into<String>>(id: T) -> Self {
        StreamId(id.into())
    }
}

/// A `Cursor` describes a position in the stream. The cursor is used for checkpointing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cursor {
    pub partition: PartitionId,
    pub offset: String,
    pub event_type: EventType,
    pub cursor_token: Uuid,
}

/// Information on a current batch. This might be
/// useful for a `Handler` that wants to do checkpointing on its own.
#[derive(Clone, Debug)]
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
pub trait Handler: Send + Sync {
    /// Handle the batch of events. The supplied string contains
    /// the whole batch of events as a `JSOS` array.
    /// Return an `AfterBatchAction` to tell what to do next.
    /// The batch array may be empty.
    /// You may not panic within the handler.
    fn handle(&self, batch: &str, info: BatchInfo) -> AfterBatchAction;
}

impl<F> Handler for F
    where F: Send + Sync + 'static + Fn(&str, BatchInfo) -> AfterBatchAction
{
    fn handle(&self, batch: &str, info: BatchInfo) -> AfterBatchAction {
        (*self)(batch, info)
    }
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
    pub fn new<H: Handler + 'static>(subscription_id: SubscriptionId,
                                     connector: Arc<C>,
                                     handler: H,
                                     settings: WorkerSettings)
                                     -> Result<(Self, JoinHandle<()>), String> {
        let (worker, handle) =
            NakadiWorker::new(connector.clone(), handler, subscription_id, settings)?;
        Ok((NakadiClient {
                worker: worker,
                connector: connector,
            },
            handle))
    }

    /// Configure the client from environment variables.
    ///
    /// The `SubscriptionId` is provided manually. Useful if you want to consume
    /// multiple subscriptions but want to use the same settings everywhere else.
    pub fn from_env_with_subscription<H: Handler + 'static, T: ProvidesToken>
        (subscription_id: SubscriptionId,
         handler: H,
         token_provider: T)
         -> Result<(NakadiClient<HyperClientConnector>, JoinHandle<()>), String> {
        let connector = HyperClientConnector::from_env(Box::new(token_provider))?;
        let connector = Arc::new(connector);
        let worker_settings = WorkerSettings::from_env()?;
        let (worker, handle) =
            NakadiWorker::new(connector.clone(), handler, subscription_id, worker_settings)?;
        let client: NakadiClient<HyperClientConnector> = NakadiClient {
            worker: worker,
            connector: connector,
        };
        Ok((client, handle))

    }

    /// Configure the client solely from environment variables.
    /// Including the `SubscriptionId`.
    pub fn from_env<H: Handler + 'static, T: ProvidesToken>
        (handler: H,
         token_provider: T)
         -> Result<(NakadiClient<HyperClientConnector>, JoinHandle<()>), String> {
        let subscription_id: SubscriptionId = match env::var("NAKADION_SUBSCRIPTION_ID") {
            Ok(env_val) => SubscriptionId(
                env_val
                    .parse()
                    .map_err(|err| format!(
                        "Could not parse 'NAKADION_SUBSCRIPTION_ID': {}",
                        err))?),
            Err(err) => {
                return Err(format!("Could not get env var 'NAKADION_SUBSCRIPTION_ID': {}", err))
            }
        };
        NakadiClient::<HyperClientConnector>::from_env_with_subscription(subscription_id,
                                                                         handler,
                                                                         token_provider)
    }

    /// Get access to the underlying `NakadiConnector`.
    pub fn connector(&self) -> &C {
        &self.connector
    }
}

impl<C: NakadiConnector> Worker for NakadiClient<C> {
    /// Returns true if the underlying `NakadiWorker` is still running.
    fn is_running(&self) -> bool {
        self.worker.is_running()
    }

    /// Stop the underlying `NakadiWorker`.
    fn stop(&self) {
        self.worker.stop();
    }

    /// Return the `SubscriptionId` this `NakadiClient` is listening to.
    fn subscription_id(&self) -> &SubscriptionId {
        self.worker.subscription_id()
    }
}
