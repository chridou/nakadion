#[cfg(feature = "metrix")]
extern crate metrix;

mod internals;

pub mod event_stream;
pub mod handler;
pub mod model;
pub mod nakadi_api;
