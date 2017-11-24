#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate derive_builder;

#[macro_use]
extern crate log;

extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate reqwest;
extern crate uuid;

extern crate url;

mod model;
mod auth;
mod nakadi;


/// A `SubscriptionId` is used to guaratee a continous flow of events for a client.
#[derive(Clone, Debug)]
pub struct SubscriptionId(pub String);

/// Describes what to do after a batch has been processed.
///
/// Use to control what should happen next.
pub enum AfterBatchAction {
    Continue,
    Abort { reason: String },
}

pub struct EventType<'a>(&'a str);

pub trait Handler {
    fn handle_batch(&self, event_type: EventType, data: &[u8]) -> AfterBatchAction;
}
