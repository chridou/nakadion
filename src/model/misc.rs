//! Types that appear at multiple locations

/// Indicator of the Application owning this EventType.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*owning_application)
use serde::{Deserialize, Serialize};

use crate::model::core_model::StreamId;
use crate::model::subscription::SubscriptionCursor;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwningApplication(String);

impl OwningApplication {
    pub fn new(v: impl Into<String>) -> Self {
        OwningApplication(v.into())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationAttribute {
    pub data_type: String,
    pub value: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd, Ord)]
pub struct BatchCounter(pub usize);

pub struct BatchMetadata<'a> {
    pub cursor: &'a SubscriptionCursor,
    pub stream: StreamId,
    pub batch_counter: BatchCounter,
}
