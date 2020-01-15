//! Types that appear at multiple locations

use serde::{Deserialize, Serialize};

use crate::model::core_model::StreamId;
use crate::model::subscription::SubscriptionCursor;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationAttribute {
    pub data_type: AuthAttDataType,
    pub value: AuthAttValue,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthAttDataType(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthAttValue(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd, Ord)]
pub struct BatchCounter(pub usize);

pub struct BatchMetadata<'a> {
    pub cursor: &'a SubscriptionCursor,
    pub stream: StreamId,
    pub batch_counter: BatchCounter,
}
