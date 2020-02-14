//! Types for subscribing to an `EventType`
use std::convert::AsRef;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::event_type::EventTypeName;
use crate::model::misc::{AuthorizationAttribute, OwningApplication};
use crate::model::partition::{Cursor, PartitionId};
use crate::Error;

pub mod subscription_builder;

new_type! {
/// Id of a subscription
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
       pub copy struct SubscriptionId(Uuid, env="SUBSCRIPTION_ID");
}

impl SubscriptionId {
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }
}

new_type! {
/// Id of a stream
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub copy struct StreamId(Uuid);
}

impl StreamId {
    pub fn random() -> Self {
        Self(Uuid::new_v4())
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
pub struct EventTypeNames(Vec<EventTypeName>);

impl EventTypeNames {
    pub fn new<T: Into<Vec<EventTypeName>>>(event_types: T) -> Self {
        Self(event_types.into())
    }

    pub fn into_event_type_names(self) -> Vec<EventTypeName> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push<T: Into<EventTypeName>>(&mut self, event_type: T) {
        self.0.push(event_type.into())
    }
}

impl AsRef<[EventTypeName]> for EventTypeNames {
    fn as_ref(&self) -> &[EventTypeName] {
        &self.0
    }
}

new_type! {
/// The value describing the use case of this subscription.
///
/// In general that is an additional identifier used to differ subscriptions having the same
/// owning_application and event_types.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct ConsumerGroup(String, env="CONSUMER_GROUP");
}

/// The value describing the use case of this subscription.
///
/// In general that is an additional identifier used to differ subscriptions having the same
/// owning_application and event_types.

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

impl SubscriptionAuthorization {
    pub fn add_admin<T: Into<AuthorizationAttribute>>(&mut self, admin: T) {
        self.admins.push(admin.into())
    }
    pub fn add_reader<T: Into<AuthorizationAttribute>>(&mut self, reader: T) {
        self.readers.push(reader.into())
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
    /// Must be set **if and only** if an updating operation is performed(e.g. Auth)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<SubscriptionId>,
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
    pub authorization: SubscriptionAuthorization,
}

impl SubscriptionInput {
    pub fn builder() -> subscription_builder::SubscriptionInputBuilder {
        subscription_builder::SubscriptionInputBuilder::default()
    }
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
    pub max_uncommitted_events: Option<MaxUncommittedEvents>,
    /// Maximum number of Events in each chunk (and therefore per partition) of the stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_limit: Option<BatchLimit>,
    /// Maximum number of Events in this stream (over all partitions being streamed in this
    /// connection)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_limit: Option<StreamLimit>,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_flush_timeout: Option<BatchFlushTimeoutSecs>,
    /// Useful for batching events based on their received_at timestamp. For example, if `batch_timespan` is 5
    /// seconds then Nakadi would flush a batch as soon as the difference in time between the first and the
    /// last event in the batch exceeds 5 seconds. It waits for an event outside of the window to signal the
    /// closure of a batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_timespan: Option<BatchTimespanSecs>,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_timeout: Option<StreamTimeoutSecs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.
    /// In case if commit does not come within this timeout, Nakadi will initialize stream termination, no
    /// new data will be sent. Partitions from this stream will be assigned to other streams.
    /// Setting commit_timeout to 0 is equal to setting it to the maximum allowed value - 60 seconds.
    pub commit_timeout: Option<CommitTimeoutSecs>,
}

impl StreamParameters {
    pub fn from_env() -> Result<Self, Error> {
        let mut me = Self::default();
        me.fill_from_env()?;
        Ok(me)
    }

    pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, Error> {
        let mut me = Self::default();
        me.fill_from_env_prefixed(prefix)?;
        Ok(me)
    }

    pub fn fill_from_env(&mut self) -> Result<(), Error> {
        self.fill_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    pub fn fill_from_env_prefixed<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.max_uncommitted_events.is_none() {
            self.max_uncommitted_events =
                MaxUncommittedEvents::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.batch_limit.is_none() {
            self.batch_limit = BatchLimit::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.stream_limit.is_none() {
            self.stream_limit = StreamLimit::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.batch_flush_timeout.is_none() {
            self.batch_flush_timeout =
                BatchFlushTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.batch_timespan.is_none() {
            self.batch_timespan = BatchTimespanSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.stream_timeout.is_none() {
            self.stream_timeout = StreamTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.commit_timeout.is_none() {
            self.commit_timeout = CommitTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }

        Ok(())
    }

    /// List of partitions to read from in this stream. If absent or empty - then the partitions will be
    /// automatically assigned by Nakadi.
    pub fn partitions(mut self, partitions: Vec<EventTypePartition>) -> Self {
        self.partitions = partitions;
        self
    }
    /// The maximum number of uncommitted events that Nakadi will stream before pausing the stream.
    ///
    /// When in
    /// paused state and commit comes - the stream will resume.
    pub fn max_uncommitted_events<T: Into<MaxUncommittedEvents>>(mut self, value: T) -> Self {
        self.max_uncommitted_events = Some(value.into());
        self
    }
    /// Maximum number of Events in each chunk (and therefore per partition) of the stream.
    pub fn batch_limit<T: Into<BatchLimit>>(mut self, value: T) -> Self {
        self.batch_limit = Some(value.into());
        self
    }
    /// Maximum number of Events in this stream (over all partitions being streamed in this
    /// connection)
    pub fn stream_limit<T: Into<StreamLimit>>(mut self, value: T) -> Self {
        self.stream_limit = Some(value.into());
        self
    }
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    pub fn batch_flush_timeout<T: Into<BatchFlushTimeoutSecs>>(mut self, value: T) -> Self {
        self.batch_flush_timeout = Some(value.into());
        self
    }
    /// Useful for batching events based on their received_at timestamp.
    ///
    /// For example, if `batch_timespan` is 5
    /// seconds then Nakadi would flush a batch as soon as the difference in time between the first and the
    /// last event in the batch exceeds 5 seconds. It waits for an event outside of the window to signal the
    /// closure of a batch.
    pub fn batch_timespan<T: Into<BatchTimespanSecs>>(mut self, value: T) -> Self {
        self.batch_timespan = Some(value.into());
        self
    }
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    pub fn stream_timeout<T: Into<StreamTimeoutSecs>>(mut self, value: T) -> Self {
        self.stream_timeout = Some(value.into());
        self
    }
    /// Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.
    ///
    /// In case if commit does not come within this timeout, Nakadi will initialize stream termination, no
    /// new data will be sent. Partitions from this stream will be assigned to other streams.
    /// Setting commit_timeout to 0 is equal to setting it to the maximum allowed value - 60 seconds.
    pub fn commit_timeout<T: Into<CommitTimeoutSecs>>(mut self, value: T) -> Self {
        self.commit_timeout = Some(value.into());
        self
    }

    /// Returns the configured value or the Nakadi default
    pub fn effective_commit_timeout_secs(&self) -> u32 {
        self.commit_timeout.map(|s| s.into_inner()).unwrap_or(60)
    }

    /// Returns the configured value or the Nakadi default
    pub fn effective_max_uncommitted_events(&self) -> u32 {
        self.max_uncommitted_events
            .map(|s| s.into_inner())
            .unwrap_or(10)
    }

    /// Returns the configured value or the Nakadi default
    pub fn effective_batch_limit(&self) -> u32 {
        self.batch_limit.map(|s| s.into_inner()).unwrap_or(1)
    }
}

new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct MaxUncommittedEvents(u32, env="MAX_UNCOMMITTED_EVENTS");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct BatchLimit(u32, env="BATCH_LIMIT");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct StreamLimit(u32, env="STREAM_LIMIT");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct BatchFlushTimeoutSecs(u32, env="BATCH_FLUSH_TIMEOUT_SECS");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct BatchTimespanSecs(u32, env="BATCH_TIMESPAN_SECS");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct StreamTimeoutSecs(u32, env="STREAM_TIMEOUT_SECS");
}
new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitTimeoutSecs(u32, env="COMMIT_TIMEOUT_SECS");
}