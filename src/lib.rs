//! # Nakadion
//!
//! A client for the [Nakadi](http://nakadi.io) Event Broker.
//!
//! ## Summary
//!
//! `Nakadion` is client that connects to the Nakadi Subscription API. It
//! does all the cursor management so that users can concentrate on
//! implementing their logic for processing events. The code implemented
//! to process events by a user does not get in touch with the internals of Nakadi.
//!
//! `Nakadion` processes batches of events from Nakadi on a worker per partition basis.
//! A worker new is spawned for each new partion discovered. These workers are guaranteed
//! to be run ona single thread at a time. To process batches of events a
//! handler factory has to be implemented which is creates handlers that are executed by the
//! workers.
//!
//! `Nakadion` is almost completely configurable with environment variables.
//!
//! Please have a look at the documentation of [Nakadi](http://nakadi.io)
//! first to become comfortable with the concepts of Nakadi.
//!
//! ## How to use
//!
//! 1. Implement a `BatchHandler` that contains all your batch processing logic
//! 2. Implement a `Handlerfactory` creates handlers for the workers.
//! 3. Configure `Nakadion`
//! 4. Hand your worker factory over to `Nakadion`
//! 5. Consume events!
//!
//! ## How Nakadion works
//!
//! ### Load balancing
//!
//! A started instance connects to the Nakadi Event Broker with one active connection. Due to
//! Nakadi`s capability of automatically distributing partitions among clients Nakadion does
//! not need to track concurrently consuming clients. In most use cases it does not make
//! any sense to have more clients running than the number partitions assigned
//! to an event type.
//!
//! ### Consuming events
//!
//! Nakadi delivers events in batches. Each batch contains the events of a single partition
//! along with a cursor that is used for reporting progress to Nakadi.
//!
//! To consume events with `Nakadion` one has to implement a `BatchHandler`. This `BatchHandler`
//! provides the processing logic and is passed the bytes containing the events of a batch.
//!
//! `Nakadion` itself does not do any deserialization of events. The `BatchHandler` is responsible
//! for deserializing events. Nevertheless there is a `TypedBatchHandler` for convinience
//! that does the deserialization of events using `serde`.
//!
//! When `Nakadion` receives a batch it just extract the necessary data from
//! the bytes received over the network and then delagates the batch
//! to a dispatcher which spawns workers that are then passed the batch.
//!
//! This means `Nakadion` itself does not have any knowledge of the events contained in a batch.
//!
//! ### Buffering batches and maximizing throughput
//!
//! `Nakadion` has an unbounded buffer for events. When looking at how Nakadi works it turns
//! out that a bounded buffer is not necessary.
//!
//! Nakadi has a timeout for committing the cursors of batches. This tiemout is 60 seconds.
//! Furthermore Nakadi has a configuration parameter called `max_uncommitted_events`.
//! With this paramteter which can be configured for `Nakadion` one can steer how many
//! events can be at most in `Nakadion`s buffers. In conjunction with a
//! `CommitStrategy` one can optimize for maximum throughput and keep the amount
//! of buffered events under control.
//!
//! ### Logging
//!
//! `Nakadion` does verbose logging when connecting to a stream and when a stream is closed. The
//! reason is that this information can be quite important when probles arise. A reconnect
//! happens roughly every full hour unless configured otherwise on Nakadi's side.
//!
//! `Nakadion` also logs a message each time a new worker is created and each time a worker is
//! shut down.
//!
//! Othrwise `Nakadion` only logs problems and errors.
//!
//! So in the end your log files will not be flodded with messages from `Nakadion`.
//!
//! ### Metrics
//!
//! `Nakadion` provides an interface for attaching metrics libraries. Metrics are especially
//! useful when optimizing for maximum throughput since one can see what
//! effect (especially on cursors) the different possible settings have.
//!
//! ### Performance
//!
//! Nakadion is not meant to be used in a high performance scenario. It uses synchronous IO.
//! Nevertheless it is easily possible to consume tens of thousands events per second depending
//! on the complexity of your processing logic.
#[macro_use]
extern crate failure;
#[macro_use]
extern crate hyper;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;
extern crate serde_json;

extern crate reqwest;
extern crate uuid;

extern crate chrono;

extern crate backoff;

extern crate url;

#[cfg(feature = "metrix")]
extern crate metrix;

pub mod auth;

mod nakadi;

pub use nakadi::api;
pub use nakadi::consumer;
pub use nakadi::handler::*;
pub use nakadi::metrics;
pub use nakadi::model::{EventType, FlowId, PartitionId, StreamId, SubscriptionId};
pub use nakadi::streaming_client;
pub use nakadi::{CommitStrategy, Nakadion, NakadionBuilder, NakadionConfig, SubscriptionDiscovery};

pub use nakadi::publisher;

pub use nakadi::events;
