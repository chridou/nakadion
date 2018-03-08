//! WARNING! This is the fist iteration of a rewrite. API might change and
//! features will be added. Documentation not yet updated!

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

pub mod auth;

mod nakadi;

pub use nakadi::handler::*;
pub use nakadi::consumer::*;
pub use nakadi::model::{EventType, FlowId, PartitionId, StreamId, SubscriptionId};
pub use nakadi::streaming_client;
pub use nakadi::api_client;
pub use nakadi::{CommitStrategy, Nakadion, NakadionBuilder, NakadionConfig, SubscriptionDiscovery};

pub use nakadi::publisher;

pub use nakadi::events;
