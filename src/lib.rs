#[macro_use]
extern crate derive_builder;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate hyper;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

extern crate reqwest;
extern crate uuid;

extern crate backoff;

extern crate url;

mod auth;

mod nakadi;

pub use auth::{AccessToken, ProvidesAccessToken, TokenError};
pub use nakadi::handler::*;
pub use nakadi::model::{EventType, StreamId, SubscriptionId};
pub use nakadi::client::{Client, ClientConfig, ClientConfigBuilder, ConnectError, LineResult,
                         StreamingClient};
pub use nakadi::CommitStrategy;
pub use nakadi::{Nakadion, NakadionConfig};

pub use nakadi::maintenance;
