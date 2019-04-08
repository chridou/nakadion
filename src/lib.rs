#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde;
#[cfg(feature = "metrix")]
extern crate metrix;

pub mod model;

pub mod auth;
