use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::model::misc::{AuthorizationAttribute, OwningApplication};
pub use crate::model::{EventTypeName, PartitionId, StreamId};

/// Id of subscription
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    pub fn new(id: Uuid) -> Self {
        SubscriptionId(id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeNames(Vec<EventTypeName>);

/// The value describing the use case of this subscription.
/// In general that is an additional identifier used to differ subscriptions having the same
/// owning_application and event_types.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroup(String);

impl ConsumerGroup {
    pub fn new(v: impl Into<String>) -> Self {
        ConsumerGroup(v.into())
    }
}

/// Status of one event-type within a context of subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStatus)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEventTypeStatus {
    event_type: EventTypeName,
    partitions: Vec<SubscriptionPartitionStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionPartitionStatus {
    partition: PartitionId,
    state: PartitionState,
    stream_id: Option<StreamId>,
    assignment_type: Option<PartitionAssignmentType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionState {
    #[serde(rename = "unassigned")]
    Unassigned,
    #[serde(rename = "reassigned")]
    Reassigned,
    #[serde(rename = "assigned")]
    Assigned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionAssignmentType {
    #[serde(rename = "direct")]
    Direct,
    #[serde(rename = "auto")]
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionAuthorization {
    admins: Vec<AuthorizationAttribute>,
    readers: Vec<AuthorizationAttribute>,
}

impl Default for SubscriptionAuthorization {
    fn default() -> Self {
        Self {
            admins: Vec::default(),
            readers: Vec::default(),
        }
    }
}

/// Subscription is a high level consumption unit.
///
/// Subscriptions allow applications to easily scale the number of clients by managing
/// consumed event offsets and distributing load between instances.
/// The key properties that identify subscription are ‘owning_application’, ‘event_types’ and ‘consumer_group’.
/// It’s not possible to have two different subscriptions with these properties being the same.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Subscription)
#[derive(Debug, Clone, Deserialize)]
pub struct Subcription {
    id: SubscriptionId,
    owning_application: OwningApplication,
    event_types: EventTypeNames,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_group: Option<ConsumerGroup>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Subscription is a high level consumption unit.
///
/// Subscriptions allow applications to easily scale the number of clients by managing
/// consumed event offsets and distributing load between instances.
/// The key properties that identify subscription are ‘owning_application’, ‘event_types’ and ‘consumer_group’.
/// It’s not possible to have two different subscriptions with these properties being the same.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Subscription)
#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionInput {
    id: SubscriptionId,
    owning_application: OwningApplication,
    event_types: EventTypeNames,
    consumer_group: Option<ConsumerGroup>,
    read_from: (),
    initial_cursors: (),
    status: Vec<SubscriptionEventTypeStatus>,
    #[serde(default)]
    authorization: SubscriptionAuthorization,
}
