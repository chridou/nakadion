use std::error::Error;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::env_vars::*;
use crate::helpers::MessageError;
use crate::model::cursor::CursorOffset;
use crate::model::event_type::OwningApplication;
use crate::model::misc::AuthorizationAttribute;
use crate::model::{EventTypeName, PartitionId, StreamId};

use super::Cursor;

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// Status of one event-type within a context of subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStatus)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionPartitionStatus {
    pub partition: PartitionId,
    pub state: PartitionState,
    pub stream_id: Option<StreamId>,
    pub assignment_type: Option<PartitionAssignmentType>,
}

/// Assignment state of a partition within a context of subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStatus)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionState {
    #[serde(rename = "unassigned")]
    Unassigned,
    #[serde(rename = "reassigned")]
    Reassigned,
    #[serde(rename = "assigned")]
    Assigned,
}

/// Assignment type of a partition within a context of subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStatus)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionAssignmentType {
    #[serde(rename = "direct")]
    Direct,
    #[serde(rename = "auto")]
    Auto,
}

/// Authorization section of a Subscription
///
/// This section defines two access control lists: one for consuming events and
/// committing cursors (‘readers’), and one for administering a subscription (‘admins’).
/// Regardless of the values of the authorization properties,
/// administrator accounts will always be authorized.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionAuthorization)
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionAuthorization {
    pub admins: Vec<AuthorizationAttribute>,
    pub readers: Vec<AuthorizationAttribute>,
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

/// Cursor of a subscription without a `CursorToken`.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionCursorWithoutToken)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionCursorWithoutToken {
    #[serde(flatten)]
    pub cursor: Cursor,
    pub event_type: EventTypeName,
}

/// An opaque value defined by the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CursorToken(String);

impl CursorToken {
    pub fn new<T: Into<String>>(token: T) -> Self {
        Self(token.into())
    }
}

/// Cursor of a subscription with a `CursorToken` which is usually found within a
/// `SubscriptionEventStreamBatch`.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionCursor)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionCursor {
    #[serde(flatten)]
    pub cursor: Cursor,
    /// The name of the event type this partition’s events belong to.
    pub event_type: EventTypeName,
    /// An opaque value defined by the server.
    pub cursor_token: CursorToken,
}

/// The result of all cursor commits with the `SubscriptionCursor`s themselves.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_CursorCommitResult)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CursorCommitResults {
    pub commit_results: Vec<CursorCommitResult>,
}

impl CursorCommitResults {
    pub fn all_committed(&self) -> bool {
        self.commit_results
            .iter()
            .all(|r| r.result == CommitResult::Committed)
    }

    pub fn iter_committed_cursors(&self) -> impl Iterator<Item = &SubscriptionCursor> {
        self.commit_results
            .iter()
            .filter(|r| r.is_committed())
            .map(|r| &r.cursor)
    }

    pub fn into_iter_committed_cursors(self) -> impl Iterator<Item = SubscriptionCursor> {
        self.commit_results
            .into_iter()
            .filter(|r| r.is_committed())
            .map(|r| r.cursor)
    }

    pub fn iter_outdated_cursors(&self) -> impl Iterator<Item = &SubscriptionCursor> {
        self.commit_results
            .iter()
            .filter(|r| r.is_outdated())
            .map(|r| &r.cursor)
    }

    pub fn into_iter_outdated_cursors(self) -> impl Iterator<Item = SubscriptionCursor> {
        self.commit_results
            .into_iter()
            .filter(|r| r.is_outdated())
            .map(|r| r.cursor)
    }

    pub fn into_inner(self) -> Vec<CursorCommitResult> {
        self.commit_results
    }
}

impl From<CursorCommitResults> for Vec<CursorCommitResult> {
    fn from(v: CursorCommitResults) -> Self {
        v.into_inner()
    }
}

/// The result of single cursor commit with the `SubscriptionCursor` itself.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_CursorCommitResult)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorCommitResult {
    pub cursor: SubscriptionCursor,
    pub result: CommitResult,
}

impl CursorCommitResult {
    pub fn is_committed(&self) -> bool {
        self.result == CommitResult::Committed
    }

    pub fn is_outdated(&self) -> bool {
        self.result == CommitResult::Outdated
    }
}

/// The result of cursor commit.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_CursorCommitResult)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommitResult {
    /// Cursor was successfully committed
    Committed,
    /// There already was more recent (or the same) cursor committed,
    ///so the current one was not committed as it is outdated
    Outdated,
}

/// Statistics of one `EventType` within a context of subscription.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStats)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEventTypeStats {
    pub event_type: EventTypeName,
    #[serde(default)]
    pub partitions: Vec<SubscriptionEventTypePartitionStats>,
}

/// Statistics of one partition for an `EventType` within a context of subscription.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStats)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEventTypePartitionStats {
    partition: PartitionId,
    unconsumed_events: u64,
    #[serde(deserialize_with = "crate::helpers::deserialize_empty_string_is_none")]
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id: Option<StreamId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_lag_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignment_type: Option<String>,
    state: SubscriptionPartitionState,
}

/// The state of this partition in current subscription.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStats)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionPartitionState {
    /// The partition is assigned to a client
    Assigned,
    /// The partition is currently not assigned to any client
    Unassigned,
    /// The partition is currently reasssigning from one client to another
    Reassigning,
}

/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStats)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionPartitionAssignmentType {
    /// Partition can’t be transferred to another stream until the stream is closed
    Direct,
    /// Partition can be transferred to another stream in case of rebalance, or if another stream
    /// requests to read from this partition
    Auto,
}

#[cfg(test)]
mod test {
    use super::super::CursorOffset;
    use super::*;

    use serde_json::{self, json};

    #[test]
    fn subscription_cursor_without_token() {
        let json = json!({
            "event_type": "the event",
            "partition": "the partition",
            "offset": "12345",
        });

        let sample = SubscriptionCursorWithoutToken {
            event_type: EventTypeName::new("the event"),
            cursor: Cursor {
                partition: PartitionId::new("the partition"),
                offset: CursorOffset::new("12345"),
            },
        };

        assert_eq!(
            serde_json::to_value(sample.clone()).unwrap(),
            json,
            "serialize"
        );
        assert_eq!(
            serde_json::from_value::<SubscriptionCursorWithoutToken>(json).unwrap(),
            sample,
            "deserialize"
        );
    }

    #[test]
    fn subscription_cursor() {
        let json = json!({
            "event_type": "the event",
            "partition": "the partition",
            "offset": "12345",
            "cursor_token": "abcdef",
        });

        let sample = SubscriptionCursor {
            event_type: EventTypeName::new("the event"),
            cursor_token: CursorToken::new("abcdef"),
            cursor: Cursor {
                partition: PartitionId::new("the partition"),
                offset: CursorOffset::new("12345"),
            },
        };

        assert_eq!(
            serde_json::to_value(sample.clone()).unwrap(),
            json,
            "serialize"
        );
        assert_eq!(
            serde_json::from_value::<SubscriptionCursor>(json).unwrap(),
            sample,
            "deserialize"
        );
    }
}
