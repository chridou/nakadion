#[cfg(feature = "metrix")]
extern crate metrix;

pub(crate) mod env_vars;

pub use nakadi_types;
pub(crate) use nakadi_types::GenericError;

// mod internals;

pub mod auth;
pub mod event_stream;
//pub mod handler;
//pub mod model;
pub mod api;
pub(crate) mod internals;
