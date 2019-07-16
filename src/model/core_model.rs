//! Essential types
//!

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PartitionId(String);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Partition {
event_type: EventTypeName,
      partition: PartitionId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct StreamId(Uuid);

/// The flow id of the request, which is written into the logs and passed to called services. Helpful
/// for operational troubleshooting and log analysis.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get*x-flow-id)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowId(String);

/// Generates a random `FlowId` if passed as a parameter.
///
/// This struct is just for readability. `()` can also be used.
pub struct RandomFlowId;

impl FlowId {
    pub fn new<T: Into<String>>(v: T) -> Self {
        Self(v.into())
    }
}

impl From<RandomFlowId> for FlowId {
    fn from(_v: RandomFlowId) -> Self {
        FlowId(uuid::Uuid::new_v4().to_string())
    }
}

impl<T> From<T> for FlowId where T: Into<String>  {
    fn from(v: T) -> Self {
        FlowId::new(v.into())
    }
}

/// Name of an EventType. The name is constrained by a regular expression.
///
/// Note: the name can encode the owner/responsible for this EventType and ideally should
/// follow a common pattern that makes it easy to read and understand, but this level of
/// structure is not enforced. For example a team name and data type can be used such as
/// ‘acme-team.price-change’.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*name)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct EventTypeName(String);

impl EventTypeName {
    pub fn new(v: impl Into<String>) -> Self {
        EventTypeName(v.into())
    }
}

/// Represents event-type:partition pair.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventTypePartition)
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct EventTypePartition {
    pub event_type: EventTypeName,
    pub partition: PartitionId,
}
