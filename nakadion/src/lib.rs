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
//! `Nakadion` is almost completely configurable from environment variables.
//!
//! Please have a look at the documentation of [Nakadi](http://nakadi.io)
//! first to become comfortable with the concepts of Nakadi.
//!
//! Currently `Nakadion` only works with the `tokio` runtime. Further execution
//! environments might be added in the future.
//!
//! ## How to use
//!
//! To run this example the following environment variables need to be set:
//!
//! * `NAKADION_NAKADI_BASE_URL`
//! * `NAKADION_SUBSCRIPTION_ID`
//! * `NAKADION_ACCESS_TOKEN_FIXED` with a valid token or `NAKADION_ACCESS_TOKEN_ALLOW_NONE` set to `true`
//!
//! ```ignore
//! use nakadion::api::ApiClient;
//! use nakadion::consumer::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = ApiClient::default_builder().finish_from_env()?;
//!
//!     let consumer = Consumer::builder_from_env()?.build_with(
//!         client,
//!         handler::MyHandlerFactory,
//!         StdOutLogger::default(),
//!     )?;
//!
//!     let (_handle, consuming) = consumer.start();
//!
//!     let _ = consuming.await.into_result()?;
//!
//!     Ok(())
//! }
//!
//! mod handler {
//!     use futures::future::{BoxFuture, FutureExt};
//!
//!     use nakadion::handler::*;
//!
//!     pub struct MyHandler {
//!         events_received: usize,
//!     }
//!
//!     impl EventsHandler for MyHandler {
//!         type Event = serde_json::Value;
//!         fn handle<'a>(
//!             &'a mut self,
//!             events: Vec<Self::Event>,
//!             _meta: BatchMeta<'a>,
//!         ) -> EventsHandlerFuture {
//!             async move {
//!                 self.events_received += events.len();
//!                 EventsPostAction::Commit
//!             }
//!             .boxed()
//!         }
//!     }
//!
//!     pub struct MyHandlerFactory;
//!
//!     impl BatchHandlerFactory for MyHandlerFactory {
//!         fn handler(
//!             &self,
//!             _assignment: &HandlerAssignment,
//!         ) -> BoxFuture<Result<Box<dyn BatchHandler>, Error>> {
//!             async { Ok(Box::new(MyHandler { events_received: 0 }) as Box<_>) }.boxed()
//!         }
//!     }
//! }
//! ```
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
//! for deserializing events. Nevertheless there is a `EventsHandler` for convenience
//! that does the deserialization of events using `serde`.
//!
//! When `Nakadion` receives a batch it just extract the necessary data from
//! the bytes received over the network and then delegates the batch
//! to a dispatcher which spawns workers that are then passed the batch.
//! This means `Nakadion` itself does not have any knowledge of the events contained in a batch.
//!
//! ### Buffering batches and maximizing throughput
//!
//! `Nakadion` has an unbounded buffer for events. When looking at how Nakadi works it turns
//! out that a bounded buffer is not necessary.
//!
//! Nakadi has a timeout for committing the cursors of batches. This default timeout is 60 seconds.
//! Furthermore Nakadi has a configuration parameter called `max_uncommitted_events`.
//! With this parameter which can be configured for `Nakadion` one can steer how many
//! events can be at most in `Nakadion`s buffers. In conjunction with a
//! `CommitStrategy` one can optimize for maximum throughput and keep the amount
//! of buffered events under control.
//!
//! ### Logging
//!
//! `Nakadion` does verbose logging when connecting to a stream and when a stream is closed. The
//! reason is that this information can be quite important when problems arise. A reconnect
//! happens roughly every full hour unless configured otherwise on Nakadi's side.
//!
//! `Nakadion` also logs a message each time a new worker is created and each time a worker is
//! shut down.
//!
//! Otherwise `Nakadion` mostly only logs problems and errors.
//! In the end your log files will not be flooded with messages from `Nakadion`.
//!
//! ### Metrics
//!
//! `Nakadion` provides an interface for attaching metrics libraries. Metrics are especially
//! useful when optimizing for maximum throughput since one can see what
//! effect (especially on cursors) the different possible settings have.
//!
//! ### Performance
//!
//! Nakadion is not meant to be used in a high performance scenario. It uses asynchronous IO.
//! Nevertheless it is easily possible to consume tens of thousands events per second depending
//! on the complexity of your processing logic.
//!
//! ## Documentation and Environment Variables
//!
//! Within the documentation environment variables can contain spaces and
//! line breaks. This is because part of the documentation was created using macros.
//! The names of the variables of cause must not contain these characters. So be
//! careful when copy & pasting.
//!
//! ## Recent Changes
//!
//! See CHANGELOG
//!
//! ## License
//!
//! Nakadion is distributed under the terms of both the MIT license and the Apache License (Version
//! 2.0).
//!
//! See LICENSE-APACHE and LICENSE-MIT for details.
//!
//! License: Apache-2.0/MIT
pub(crate) mod helpers;

pub use nakadi_types;
pub(crate) use nakadi_types::Error;

pub mod api;
pub mod auth;
pub mod components;
pub mod consumer;
pub mod handler;
pub mod instrumentation;
pub mod publisher;
pub(crate) mod tools;

pub(crate) mod internals;
pub(crate) mod logging;
