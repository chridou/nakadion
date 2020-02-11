#[cfg(feature = "metrix")]
extern crate metrix;

pub(crate) mod env_vars;

pub use nakadi_types;
pub(crate) use nakadi_types::GenericError;

pub mod api;
pub mod auth;
pub mod consumer;
pub mod event_handler;

pub(crate) mod event_stream;
pub(crate) mod internals;
pub(crate) mod logging;
