//! # Nakadion
//!
//! A client for the [Nakadi](https://github.com/zalando/nakadi) Event Broker.
//!
//! Nakadion uses the [Subscription API](https://github.com/zalando/nakadi#subscriptions)
//! of Nakadi on the consuming side.
//!
//! A producer might be added later.
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

pub mod client;
mod tokenerrors;

pub use tokenerrors::*;

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
