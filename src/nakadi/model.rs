//! Some common types
use std::fmt;

use uuid::Uuid;

/// A `SubscriptionId` is used to guarantee a continous flow of events for
/// clients.
///
/// If an event type is streamed over multiple
/// partitioned multiple clients can consume
/// the event type.
///
/// For more information on event types and subscriptions
/// see [subscriptions](http://nakadi.io/manual.html#using_consuming-events-hila)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionId(pub String);

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl SubscriptionId {
    pub fn new<T: Into<String>>(id: T) -> SubscriptionId {
        SubscriptionId(id.into())
    }
}

/// A partition id that comes with a cursor retrieved from a batch.
///
/// A `PartitionId` is passed to a `HandlerFactory` when
/// creating a new `BatchHandler`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionId(pub String);

impl PartitionId {
    /// Create a new `PartitionId`
    pub fn new<T: Into<String>>(id: T) -> PartitionId {
        PartitionId(id.into())
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

/// A `StreamId` identifies connection to a subscription.
///
/// It must be provided for committing
/// a `Cursor`.
///
/// For more information on event types and subscriptions
/// see [subscriptions](http://nakadi.io/manual.html#using_consuming-events-hila)
#[derive(Clone, Debug)]
pub struct StreamId(pub String);

impl StreamId {
    pub fn new<T: Into<String>>(id: T) -> Self {
        StreamId(id.into())
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

/// A `FlowId` helps when finding problems and tracing
/// workflows. It is usually submitted when interacting
/// with the Nakadi REST API but also contained
/// in received event metadata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowId(pub String);

impl FlowId {
    pub fn new<T: Into<String>>(id: T) -> Self {
        FlowId(id.into())
    }
}

impl fmt::Display for FlowId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Default for FlowId {
    fn default() -> FlowId {
        FlowId(Uuid::new_v4().to_string())
    }
}

/// Information on a current batch. This might be
/// useful for a `Handler` that wants to do checkpointing on its own.
#[derive(Clone, Debug)]
pub struct BatchCommitData<'a> {
    pub stream_id: StreamId,
    pub cursor: &'a [u8],
}

/// The [`Nakadi Event Type`](https://github.com/zalando/nakadi#creating-event-types).
/// Similiar to a topic.
#[derive(Clone, Debug)]
pub struct EventType<'a>(pub &'a str);

impl<'a> EventType<'a> {
    /// Creates a new instance of an
    /// [`EventType`](https://github.com/zalando/nakadi#creating-event-types).
    pub fn new(value: &'a str) -> EventType {
        EventType(value)
    }
}
