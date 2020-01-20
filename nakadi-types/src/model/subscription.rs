//! Essential types
use std::convert::AsRef;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::env_vars;
use crate::model::event_type::EventTypeName;
use crate::model::misc::{AuthorizationAttribute, OwningApplication};
use crate::model::partition::{Cursor, PartitionId};
use crate::GenericError;

use from_env_maybe;

/// Id of subscription
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    pub fn new<T: Into<Uuid>>(id: T) -> Self {
        Self(id.into())
    }

    pub fn into_inner(self) -> Uuid {
        self.0
    }

    pub fn from_env() -> Result<Self, GenericError> {
        from_env!(
            postfix => env_vars::SUBSCRIPTION_ID_ENV_VAR
        )
    }

    pub fn from_env_named<T: AsRef<str>>(name: T) -> Result<Self, GenericError> {
        from_env!(name.as_ref())
    }

    pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, GenericError> {
        from_env!(
            prefix => prefix.as_ref() , postfix => env_vars::SUBSCRIPTION_ID_ENV_VAR
        )
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SubscriptionId {
    type Err = GenericError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SubscriptionId(s.parse().map_err(|err| {
            GenericError::new(format!("could not parse subscription id: {}", err))
        })?))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct StreamId(Uuid);

impl StreamId {
    pub fn new<T: Into<Uuid>>(id: T) -> Self {
        Self(id.into())
    }

    pub fn into_inner(self) -> Uuid {
        self.0
    }
}

impl FromStr for StreamId {
    type Err = GenericError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(StreamId(s.parse().map_err(|err| {
            GenericError::new(format!("could not parse stream id: {}", err))
        })?))
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents event-type:partition pair.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventTypePartition)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct EventTypePartition {
    pub event_type: EventTypeName,
    pub partition: PartitionId,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventTypeNames(pub Vec<EventTypeName>);

/// The value describing the use case of this subscription.
///
/// In general that is an additional identifier used to differ subscriptions having the same
/// owning_application and event_types.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroup(String);

impl ConsumerGroup {
    pub fn new<T: Into<String>>(v: T) -> Self {
        ConsumerGroup(v.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
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
    #[serde(deserialize_with = "crate::deserialize_empty_string_is_none")]
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
    use super::*;
    use crate::model::partition::CursorOffset;

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

/// Parameters for starting a new stream on a subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
#[derive(Debug, Default, Clone, Serialize)]
pub struct StreamParameters {
    /// List of partitions to read from in this stream. If absent or empty - then the partitions will be
    /// automatically assigned by Nakadi.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<EventTypePartition>,
    /// The maximum number of uncommitted events that Nakadi will stream before pausing the stream. When in
    /// paused state and commit comes - the stream will resume.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uncommitted_events: Option<u32>,
    /// Maximum number of Events in each chunk (and therefore per partition) of the stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_limit: Option<u32>,
    /// Maximum number of Events in this stream (over all partitions being streamed in this
    /// connection)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_limit: Option<u32>,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "batch_flush_timeout")]
    pub batch_flush_timeout_secs: Option<u32>,
    /// Useful for batching events based on their received_at timestamp. For example, if `batch_timespan` is 5
    /// seconds then Nakadi would flush a batch as soon as the difference in time between the first and the
    /// last event in the batch exceeds 5 seconds. It waits for an event outside of the window to signal the
    /// closure of a batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "batch_timespan")]
    pub batch_timespan_secs: Option<u32>,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "stream_timeout")]
    pub stream_timeout_secs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "commit_timeout")]
    /// Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.
    /// In case if commit does not come within this timeout, Nakadi will initialize stream termination, no
    /// new data will be sent. Partitions from this stream will be assigned to other streams.
    /// Setting commit_timeout to 0 is equal to setting it to the maximum allowed value - 60 seconds.
    pub commit_timeout_secs: Option<u32>,
}

impl StreamParameters {
    pub fn from_env() -> Result<Self, GenericError> {
        let mut me = Self::default();
        me.fill_from_env()?;
        Ok(me)
    }

    pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, GenericError> {
        let mut me = Self::default();
        me.fill_from_env_prefixed(prefix)?;
        Ok(me)
    }

    pub fn fill_from_env(&mut self) -> Result<(), GenericError> {
        if let Some(v) = from_env_maybe!(postfix => env_vars::MAX_UNCOMMITTED_EVENTS_ENV_VAR)? {
            self.max_uncommitted_events = Some(v)
        };
        if let Some(v) = from_env_maybe!(postfix => env_vars::BATCH_LIMIT_ENV_VAR)? {
            self.batch_limit = Some(v)
        };
        if let Some(v) = from_env_maybe!(postfix => env_vars::STREAM_LIMIT_ENV_VAR)? {
            self.stream_limit = Some(v)
        };
        if let Some(v) = from_env_maybe!(postfix => env_vars::BATCH_FLUSH_TIMEOUT_SECS_ENV_VAR)? {
            self.batch_flush_timeout_secs = Some(v)
        };
        if let Some(v) = from_env_maybe!(postfix => env_vars::BATCH_TIMESPAN_SECS_ENV_VAR)? {
            self.batch_timespan_secs = Some(v)
        };
        if let Some(v) = from_env_maybe!(postfix => env_vars::STREAM_TIMEOUT_SECS_ENV_VAR)? {
            self.stream_timeout_secs = Some(v)
        };
        if let Some(v) = from_env_maybe!(postfix => env_vars::COMMIT_TIMEOUT_SECS_ENV_VAR)? {
            self.commit_timeout_secs = Some(v)
        };

        Ok(())
    }

    pub fn fill_from_env_prefixed<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), GenericError> {
        if let Some(v) = from_env_maybe!(prefix => prefix.as_ref(), postfix => env_vars::MAX_UNCOMMITTED_EVENTS_ENV_VAR)?
        {
            self.max_uncommitted_events = Some(v)
        };
        if let Some(v) =
            from_env_maybe!(prefix => prefix.as_ref(), postfix => env_vars::BATCH_LIMIT_ENV_VAR)?
        {
            self.batch_limit = Some(v)
        };
        if let Some(v) =
            from_env_maybe!(prefix => prefix.as_ref(), postfix => env_vars::STREAM_LIMIT_ENV_VAR)?
        {
            self.stream_limit = Some(v)
        };
        if let Some(v) = from_env_maybe!(prefix => prefix.as_ref(), postfix => env_vars::BATCH_FLUSH_TIMEOUT_SECS_ENV_VAR)?
        {
            self.batch_flush_timeout_secs = Some(v)
        };
        if let Some(v) = from_env_maybe!(prefix => prefix.as_ref(), postfix => env_vars::BATCH_TIMESPAN_SECS_ENV_VAR)?
        {
            self.batch_timespan_secs = Some(v)
        };
        if let Some(v) = from_env_maybe!(prefix => prefix.as_ref(), postfix => env_vars::STREAM_TIMEOUT_SECS_ENV_VAR)?
        {
            self.stream_timeout_secs = Some(v)
        };
        if let Some(v) = from_env_maybe!(prefix => prefix.as_ref(), postfix => env_vars::COMMIT_TIMEOUT_SECS_ENV_VAR)?
        {
            self.commit_timeout_secs = Some(v)
        };

        Ok(())
    }
}
