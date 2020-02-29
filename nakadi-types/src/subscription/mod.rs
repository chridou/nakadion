//! Types for subscribing to an `EventType`
use std::convert::AsRef;
use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::misc::{AuthorizationAttribute, AuthorizationAttributes, OwningApplication};
use crate::partition::{Cursor, CursorOffset, PartitionId};
use crate::Error;

mod subscription_input;
pub use crate::event_type::EventTypeName;
pub use subscription_input::*;

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

/// Something that has an event type and a partition
///
/// Must only return event types and partitions that belong together.
pub trait EventTypePartitionLike {
    fn event_type(&self) -> &EventTypeName;

    fn partition(&self) -> &PartitionId;
}

/// Represents event-type:partition pair.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventTypePartition)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct EventTypePartition {
    pub event_type: EventTypeName,
    pub partition: PartitionId,
}

impl EventTypePartition {
    pub fn new<E: Into<EventTypeName>, P: Into<PartitionId>>(event_type: E, partition: P) -> Self {
        Self {
            event_type: event_type.into(),
            partition: partition.into(),
        }
    }

    pub fn split(self) -> (EventTypeName, PartitionId) {
        (self.event_type, self.partition)
    }

    pub fn event_type(&self) -> &EventTypeName {
        &self.event_type
    }

    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }
}

impl fmt::Display for EventTypePartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[event_type={};partition={}]",
            self.event_type, self.partition
        )?;
        Ok(())
    }
}
impl EventTypePartitionLike for EventTypePartition {
    fn event_type(&self) -> &EventTypeName {
        &self.event_type
    }

    fn partition(&self) -> &PartitionId {
        &self.partition
    }
}

impl From<SubscriptionCursor> for EventTypePartition {
    fn from(v: SubscriptionCursor) -> Self {
        Self {
            event_type: v.event_type,
            partition: v.cursor.partition,
        }
    }
}

impl From<SubscriptionCursorWithoutToken> for EventTypePartition {
    fn from(v: SubscriptionCursorWithoutToken) -> Self {
        Self {
            event_type: v.event_type,
            partition: v.cursor.partition,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventTypeNames(Vec<EventTypeName>);

impl EventTypeNames {
    pub fn new<T: Into<Vec<EventTypeName>>>(event_types: T) -> Self {
        Self(event_types.into())
    }

    pub fn event_type_name<T: Into<EventTypeName>>(mut self, v: T) -> Self {
        self.0.push(v.into());
        self
    }

    pub fn into_inner(self) -> Vec<EventTypeName> {
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
    /// The partition id
    pub partition: PartitionId,
    /// The state of this partition in current subscription.
    pub state: PartitionState,
    /// The id of the stream that consumes data from this partition
    pub stream_id: Option<StreamId>,
    pub assignment_type: Option<PartitionAssignmentType>,
}

/// Assignment state of a partition within a context of subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStatus)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionState {
    /// The partition is currently not assigned to any client
    #[serde(rename = "unassigned")]
    Unassigned,
    /// The partition is currently reassigning from one client to another
    #[serde(rename = "reassigned")]
    Reassigned,
    /// The partition is assigned to a client.
    #[serde(rename = "assigned")]
    Assigned,
}

/// Assignment type of a partition within a context of subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStatus)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionAssignmentType {
    /// Partition can’t be transferred to another stream until the stream is closed
    #[serde(rename = "direct")]
    Direct,
    /// Partition can be transferred to another stream in case of rebalance, or if another stream
    /// requests to read from this partition.
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
    pub admins: AuthorizationAttributes,
    pub readers: AuthorizationAttributes,
}

impl SubscriptionAuthorization {
    pub fn new<A, R>(admins: A, readers: R) -> Self
    where
        A: Into<AuthorizationAttributes>,
        R: Into<AuthorizationAttributes>,
    {
        Self {
            admins: admins.into(),
            readers: readers.into(),
        }
    }

    pub fn admin<T: Into<AuthorizationAttribute>>(mut self, admin: T) -> Self {
        self.admins.push(admin.into());
        self
    }
    pub fn reader<T: Into<AuthorizationAttribute>>(mut self, reader: T) -> Self {
        self.readers.push(reader.into());
        self
    }

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

/// Cursor of a subscription without a `CursorToken`.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionCursorWithoutToken)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionCursorWithoutToken {
    #[serde(flatten)]
    pub cursor: Cursor,
    /// The name of the event type this partition’s events belong to.
    pub event_type: EventTypeName,
}

impl EventTypePartitionLike for SubscriptionCursorWithoutToken {
    fn event_type(&self) -> &EventTypeName {
        &self.event_type
    }

    fn partition(&self) -> &PartitionId {
        &self.cursor.partition
    }
}

impl From<SubscriptionCursor> for SubscriptionCursorWithoutToken {
    fn from(c: SubscriptionCursor) -> Self {
        c.into_without_token()
    }
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

impl SubscriptionCursor {
    pub fn into_without_token(self) -> SubscriptionCursorWithoutToken {
        SubscriptionCursorWithoutToken {
            cursor: self.cursor,
            event_type: self.event_type,
        }
    }

    /// Turns this into a `SubscriptionCursorWithoutToken` that points to the begin of the subscription
    pub fn into_without_token_begin(self) -> SubscriptionCursorWithoutToken {
        SubscriptionCursorWithoutToken {
            cursor: Cursor {
                partition: self.cursor.partition,
                offset: CursorOffset::Begin,
            },
            event_type: self.event_type,
        }
    }
}

impl EventTypePartitionLike for SubscriptionCursor {
    fn event_type(&self) -> &EventTypeName {
        &self.event_type
    }

    fn partition(&self) -> &PartitionId {
        &self.cursor.partition
    }
}

/// The result of all cursor commits with the `SubscriptionCursor`s themselves.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_CursorCommitResult)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CursorCommitResults {
    #[serde(rename = "items")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionStats {
    #[serde(rename = "items")]
    pub event_type_stats: Vec<SubscriptionEventTypeStats>,
}

impl SubscriptionStats {
    pub fn unconsumed_events(&self) -> usize {
        self.event_type_stats
            .iter()
            .map(|set_stats| set_stats.unconsumed_events())
            .sum()
    }

    pub fn all_events_consumed(&self) -> bool {
        self.unconsumed_events() == 0
    }
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

impl SubscriptionEventTypeStats {
    pub fn unconsumed_events(&self) -> usize {
        self.partitions.iter().map(|p| p.unconsumed_events).sum()
    }

    pub fn all_events_consumed(&self) -> bool {
        self.unconsumed_events() == 0
    }
}

/// Statistics of one partition for an `EventType` within a context of subscription.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_SubscriptionEventTypeStats)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEventTypePartitionStats {
    partition: PartitionId,
    unconsumed_events: usize,
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
    /// The partition is currently reassigning from one client to another
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
    use crate::partition::CursorOffset;

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
///
/// # Environment
///
/// When initialized/updated from the environment the following environment variable
/// are used which by are by default prefixed with "NAKADION_" or a custom prefix "<prefix>_":
///
/// * "MAX_UNCOMMITTED_EVENTS"
/// * "BATCH_LIMIT"
/// * "STREAM_LIMIT"
/// * "BATCH_FLUSH_TIMEOUT_SECS"
/// * "BATCH_TIMESPAN_SECS"
/// * "STREAM_TIMEOUT_SECS"
/// * "COMMIT_TIMEOUT_SECS"
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
    #[serde(rename = "batch_flush_timeout")]
    pub batch_flush_timeout_secs: Option<BatchFlushTimeoutSecs>,
    /// Useful for batching events based on their received_at timestamp. For example, if `batch_timespan` is 5
    /// seconds then Nakadi would flush a batch as soon as the difference in time between the first and the
    /// last event in the batch exceeds 5 seconds. It waits for an event outside of the window to signal the
    /// closure of a batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "batch_timespan")]
    pub batch_timespan_secs: Option<BatchTimespanSecs>,
    /// Maximum time in seconds a stream will live before connection is closed by the server.
    ///
    /// If 0 or unspecified will stream for 1h ±10min.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "stream_timeout")]
    pub stream_timeout_secs: Option<StreamTimeoutSecs>,
    /// Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.
    /// In case if commit does not come within this timeout, Nakadi will initialize stream termination, no
    /// new data will be sent. Partitions from this stream will be assigned to other streams.
    /// Setting commit_timeout to 0 is equal to setting it to the maximum allowed value - 60 seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "commit_timeout")]
    pub commit_timeout_secs: Option<CommitTimeoutSecs>,
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
        if self.batch_flush_timeout_secs.is_none() {
            self.batch_flush_timeout_secs =
                BatchFlushTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.batch_timespan_secs.is_none() {
            self.batch_timespan_secs = BatchTimespanSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.stream_timeout_secs.is_none() {
            self.stream_timeout_secs = StreamTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.commit_timeout_secs.is_none() {
            self.commit_timeout_secs = CommitTimeoutSecs::try_from_env_prefixed(prefix.as_ref())?;
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
    pub fn batch_flush_timeout_secs<T: Into<BatchFlushTimeoutSecs>>(mut self, value: T) -> Self {
        self.batch_flush_timeout_secs = Some(value.into());
        self
    }
    /// Useful for batching events based on their received_at timestamp.
    ///
    /// For example, if `batch_timespan` is 5
    /// seconds then Nakadi would flush a batch as soon as the difference in time between the first and the
    /// last event in the batch exceeds 5 seconds. It waits for an event outside of the window to signal the
    /// closure of a batch.
    pub fn batch_timespan_secs<T: Into<BatchTimespanSecs>>(mut self, value: T) -> Self {
        self.batch_timespan_secs = Some(value.into());
        self
    }
    /// Maximum time in seconds a stream will live before connection is closed by the server..
    pub fn stream_timeout_secs<T: Into<StreamTimeoutSecs>>(mut self, value: T) -> Self {
        self.stream_timeout_secs = Some(value.into());
        self
    }
    /// Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.
    ///
    /// In case if commit does not come within this timeout, Nakadi will initialize stream termination, no
    /// new data will be sent. Partitions from this stream will be assigned to other streams.
    /// Setting commit_timeout to 0 is equal to setting it to the maximum allowed value - 60 seconds.
    pub fn commit_timeout_secs<T: Into<CommitTimeoutSecs>>(mut self, value: T) -> Self {
        self.commit_timeout_secs = Some(value.into());
        self
    }

    /// Returns the configured value or the Nakadi default
    pub fn effective_commit_timeout_secs(&self) -> u32 {
        self.commit_timeout_secs
            .map(|s| s.into_inner())
            .unwrap_or(60)
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
    #[doc="The maximum number of uncommitted events that Nakadi will stream before pausing the stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct MaxUncommittedEvents(u32, env="MAX_UNCOMMITTED_EVENTS");
}
new_type! {
    #[doc="Maximum number of Events in each chunk (and therefore per partition) of the stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct BatchLimit(u32, env="BATCH_LIMIT");
}
new_type! {
    #[doc="Maximum number of Events in this stream \
    (over all partitions being streamed in this connection).\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct StreamLimit(u32, env="STREAM_LIMIT");
}
new_type! {
    #[doc="Maximum time in seconds to wait for the flushing of each chunk (per partition).\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct BatchFlushTimeoutSecs(u32, env="BATCH_FLUSH_TIMEOUT_SECS");
}
new_type! {
    #[doc="Useful for batching events based on their received_at timestamp.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct BatchTimespanSecs(u32, env="BATCH_TIMESPAN_SECS");
}
new_type! {
    #[doc="Maximum time in seconds a stream will live before connection is closed by the server.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct StreamTimeoutSecs(u32, env="STREAM_TIMEOUT_SECS");
}
new_type! {
    #[doc="Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct CommitTimeoutSecs(u32, env="COMMIT_TIMEOUT_SECS");
}
