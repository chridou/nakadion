#[macro_use]
extern crate derive_builder;
#[macro_use]
extern crate error_chain;
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

extern crate url;

mod auth;
mod nakadi;

/// Describes what to do after a batch has been processed.
///
/// Use to control what should happen next.
pub enum AfterBatchAction {
    Continue,
    Abort { reason: String },
}
