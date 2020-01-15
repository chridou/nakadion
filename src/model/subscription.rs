use std::error::Error;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::env_vars::*;
use crate::helpers::MessageError;
use crate::model::cursor::CursorOffset;
use crate::model::misc::{AuthorizationAttribute, OwningApplication};
use crate::model::{EventTypeName, PartitionId, StreamId};

use must_env_parsed;

/// Id of subscription
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    pub fn new(id: Uuid) -> Self {
        SubscriptionId(id)
    }

    pub fn from_env() -> Result<Self, Box<dyn Error + 'static>> {
        Self::from_env_named(NAKADION_SUBSCRIPTION_ID_ENV_VAR)
    }

    pub fn from_env_named<T: AsRef<str>>(name: T) -> Result<Self, Box<dyn Error + 'static>> {
        must_env_parsed!(name.as_ref())
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SubscriptionId {
    type Err = Box<dyn Error + 'static>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SubscriptionId(s.parse().map_err(|err| {
            MessageError::new(format!("could not parse subscription id: {}", err))
        })?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeNames(pub Vec<EventTypeName>);

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
    pub event_type: EventTypeName,
    pub partitions: Vec<SubscriptionPartitionStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionPartitionStatus {
    pub partition: PartitionId,
    pub state: PartitionState,
    pub stream_id: Option<StreamId>,
    pub assignment_type: Option<PartitionAssignmentType>,
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
    pub admins: Vec<AuthorizationAttribute>,
    pub readers: Vec<AuthorizationAttribute>,
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
pub struct Subscription {
    pub id: SubscriptionId,
    pub owning_application: OwningApplication,
    pub event_types: EventTypeNames,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<ConsumerGroup>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
    pub id: SubscriptionId,
    pub owning_application: OwningApplication,
    pub event_types: EventTypeNames,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<ConsumerGroup>,
    /// Position to start reading events from.
    ///
    /// Currently supported values:
    ///
    /// * Begin - read from the oldest available event.
    /// * End - read from the most recent offset.
    /// * Cursors - read from cursors provided in initial_cursors property.
    /// Applied when the client starts reading from a subscription.
    pub read_from: ReadFrom,
    /// List of cursors to start reading from.
    ///
    /// This property is required when `read_from` = `ReadFrom::Cursors`.
    /// The initial cursors should cover all partitions of subscription.
    /// Clients will get events starting from next offset positions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_cursors: Option<Vec<SubscriptionCursorWithoutToken>>,
    pub status: Vec<SubscriptionEventTypeStatus>,
    pub authorization: SubscriptionAuthorization,
}

/// Position to start reading events from. Currently supported values:
///
///  * Begin - read from the oldest available event.
///  * End - read from the most recent offset.
///  * Cursors - read from cursors provided in initial_cursors property.
///  Applied when the client starts reading from a subscription.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Subscription)
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadFrom {
    Start,
    End,
    Cursors,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionCursorWithoutToken {
    pub partition: PartitionId,
    pub offset: CursorOffset,
    pub event_type: EventTypeName,
}

/// An opaque value defined by the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorToken(String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionCursor {
    pub partition: PartitionId,
    pub offset: CursorOffset,
    pub event_type: EventTypeName,
    pub cursor_token: CursorToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResult {
    pub cursor: SubscriptionCursor,
    pub result: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEventTypeStats {
    pub event_type: EventTypeName,
    #[serde(default)]
    pub partitions: Vec<PartitionStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStats {
    partition: PartitionId,
    unconsumed_events: u64,
    #[serde(deserialize_with = "crate::helpers::deserialize_empty_string_is_none")]
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id: Option<StreamId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_lag_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignment_type: Option<String>,
    state: String,
}
