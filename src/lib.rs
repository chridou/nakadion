#[cfg(feature = "metrix")]
extern crate metrix;

mod internals;

pub(crate) mod helpers;

pub mod auth;
pub mod event_stream;
pub mod handler;
pub mod model;
pub mod nakadi_api;
