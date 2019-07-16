//! Types that appear at multiple locations

/// Indicator of the Application owning this EventType.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*owning_application)
use serde::{Deserialize, Serialize};

use crate::model::subscription::SubscriptionCursor;
use crate::model::core_model::StreamId;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwningApplication(String);

impl OwningApplication {
    pub fn new(v: impl Into<String>) -> Self {
        OwningApplication(v.into())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationAttribute {
    data_type: String,
    value: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd, Ord)]
pub struct BatchCounter(usize);

pub struct BatchMetadata<'a> {
    cursor: &'a SubscriptionCursor,
    stream: StreamId,
    batch_counter: BatchCounter,
}
