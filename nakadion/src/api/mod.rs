//! Direct interaction with Nakadi through its REST API
//!
//! See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
use std::error::Error as StdError;
use std::fmt;

use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};

use nakadi_types::event_type::*;
use nakadi_types::misc::OwningApplication;
use nakadi_types::partition::*;
use nakadi_types::publishing::*;
use nakadi_types::subscription::*;
use nakadi_types::{Error, FlowId};

use dispatch_http_request::RemoteCallError;

pub use self::client::{ApiClient, Builder};
pub use self::error::*;
pub use crate::components::IoError;

pub mod api_ext;
mod client;
pub mod dispatch_http_request;
mod error;

pub type ApiFuture<'a, T> = BoxFuture<'a, Result<T, NakadiApiError>>;
pub type BytesStream = BoxStream<'static, Result<Bytes, IoError>>;

pub trait MonitoringApi {
    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
    fn get_cursor_distances<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        query: &[CursorDistanceQuery],
        flow_id: T,
    ) -> ApiFuture<Vec<CursorDistanceResult>>;

    /// Used when a consumer wants to know how far behind
    /// in the stream its application is lagging.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursors-lag_post)
    fn get_cursor_lag<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: T,
    ) -> ApiFuture<Vec<Partition>>;

    /// Lists the Partitions for the given event-type.
    ///
    /// This endpoint is mostly interesting for
    /// monitoring purposes or in cases when consumer wants
    /// to start consuming older messages.
    /// If per-EventType authorization is enabled,
    /// the caller must be authorized to read from the EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/partitions_get)
    fn get_event_type_partitions<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<Vec<Partition>>;
}

pub trait SchemaRegistryApi {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types<T: Into<FlowId>>(&self, flow_id: T) -> ApiFuture<Vec<EventType>>;

    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type<T: Into<FlowId>>(
        &self,
        event_type: &EventTypeInput,
        flow_id: T,
    ) -> ApiFuture<()>;

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<EventType>;

    /// Updates the EventType identified by its name.
    ///
    /// Updates the EventType identified by its name. Behaviour is the same as creation of
    /// EventType (See POST /event-type) except where noted below.
    ///
    /// The name field cannot be changed. Attempting to do so will result in a 422 failure.
    ///
    /// Modifications to the schema are constrained by the specified compatibility_mode.
    ///
    /// Updating the EventType is only allowed for clients that satisfy the authorization admin requirements,
    /// if it exists.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: T,
    ) -> ApiFuture<()>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type<T: Into<FlowId>>(&self, name: &EventTypeName, flow_id: T)
        -> ApiFuture<()>;
}

/// Possible error variants returned from publishing events
#[derive(Debug)]
pub enum PublishError {
    SubmissionFailed(FailedSubmission),
    /// There was an error that was not `Unprocessable`
    Other(NakadiApiError),
}

impl StdError for PublishError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            PublishError::Other(err) => err.source(),
            _ => None,
        }
    }
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PublishError::Other(err) => write!(f, "publishing failed: {}", err)?,
            PublishError::SubmissionFailed(failure) => write!(f, "submission failed: {}", failure)?,
        }

        Ok(())
    }
}

impl From<NakadiApiError> for PublishError {
    fn from(api_error: NakadiApiError) -> Self {
        Self::Other(api_error)
    }
}

impl From<RemoteCallError> for PublishError {
    fn from(remote_call_error: RemoteCallError) -> Self {
        let api_error = NakadiApiError::from(remote_call_error);
        Self::Other(api_error)
    }
}

impl From<PublishError> for Error {
    fn from(err: PublishError) -> Self {
        Error::from_error(err)
    }
}

pub type PublishFuture<'a> = BoxFuture<'a, Result<(), PublishError>>;

/// Publishes a batch of Events.
///
/// All items must be of the EventType identified by name.
///
/// Reception of Events will always respect the configuration of its EventType with respect to
/// validation, enrichment and partition. The steps performed on reception of incoming message
/// are:
///
/// 1.  Every validation rule specified for the EventType will be checked in order against the
///     incoming Events. Validation rules are evaluated in the order they are defined and the Event
///     is rejected in the first case of failure. If the offending validation rule provides
///     information about the violation it will be included in the BatchItemResponse. If the
///     EventType defines schema validation it will be performed at this moment. The size of each
///     Event will also be validated. The maximum size per Event is configured by the administrator.
///     We use the batch input to measure the size of events, so unnecessary spaces, tabs, and
///     carriage returns will count towards the event size.
///
/// 2.  Once the validation succeeded, the content of the Event is updated according to the
///     enrichment rules in the order the rules are defined in the EventType. No preexisting
///     value might be changed (even if added by an enrichment rule). Violations on this will force
///     the immediate rejection of the Event. The invalid overwrite attempt will be included in
///     the item’s BatchItemResponse object.
///
/// 3.  The incoming Event’s relative ordering is evaluated according to the rule on the
///     EventType. Failure to evaluate the rule will reject the Event.
///
///     Given the batched nature of this operation, any violation on validation or failures on
///     enrichment or partitioning will cause the whole batch to be rejected, i.e. none of its
///     elements are pushed to the underlying broker.
///
///     Failures on writing of specific partitions to the broker might influence other
///     partitions. Failures at this stage will fail only the affected partitions.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
pub trait PublishApi {
    /// Publishes a batch of Events of this EventType. All items must be of the EventType
    /// identified by name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
    fn publish_events_batch<'a, B: Into<Bytes>, T: Into<FlowId>>(
        &'a self,
        event_type: &'a EventTypeName,
        events: B,
        flow_id: T,
    ) -> PublishFuture<'a>;
}

pub trait SubscriptionApi {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription<T: Into<FlowId>>(
        &self,
        input: &SubscriptionInput,
        flow_id: T,
    ) -> ApiFuture<Subscription>;

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<Subscription>;

    /// Lists all subscriptions that exist in a system.
    ///
    /// List is ordered by creation date/time descending (newest
    /// subscriptions come first).
    ///
    /// Returns a stream of `Subscription`s. The stream contains an error if requesting
    /// a page from Nakadi fails or if the result could not be deserialized.
    ///
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_get)
    ///
    /// ## Usage
    ///
    /// The parameter `offset` does not change its meaning. It can
    /// be used if streams are created one after the other.
    ///
    /// The parameter `limit` is of limited use. It controls the
    /// page size returned by Nakadi. Since a stream is generated it rather controls
    /// how many requests (the frequency of calls) are sent to Nakadi to fill the stream.
    fn list_subscriptions<T: Into<FlowId>>(
        &self,
        event_type: Option<&EventTypeName>,
        owning_application: Option<&OwningApplication>,
        limit: Option<usize>,
        offset: Option<usize>,
        show_status: bool,
        flow_id: T,
    ) -> BoxStream<'static, Result<Subscription, NakadiApiError>>;

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth<T: Into<FlowId>>(&self, input: &SubscriptionInput, flow_id: T) -> ApiFuture<()>;

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription<T: Into<FlowId>>(&self, id: SubscriptionId, flow_id: T)
        -> ApiFuture<()>;

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_subscription_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<Vec<SubscriptionCursor>>;

    /// Exposes statistics of specified subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/stats_get)
    fn get_subscription_stats<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: T,
    ) -> ApiFuture<SubscriptionStats>;

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        cursors: &[EventTypeCursor],
        flow_id: T,
    ) -> ApiFuture<()>;
}

pub trait SubscriptionStreamApi {
    /// Starts a new stream for reading events from this subscription.
    ///
    /// Starts a new stream for reading events from this subscription. The minimal consumption unit is a partition, so
    /// it is possible to start as many streams as the total number of partitions in event-types of this subscription.
    /// The position of the consumption is managed by Nakadi. The client is required to commit the cursors he gets in
    /// a stream.
    ///
    /// If you create a stream without specifying the partitions to read from - Nakadi will automatically assign
    /// partitions to this new stream. By default Nakadi distributes partitions among clients trying to give an equal
    /// number of partitions to each client (the amount of data is not considered). This is default and the most common
    /// way to use streaming endpoint.
    ///
    /// It is also possible to directly request specific partitions to be delivered within the stream. If these
    /// partitions are already consumed by another stream of this subscription - Nakadi will trigger a rebalance that
    /// will assign these partitions to the new stream. The request will fail if user directly requests partitions that
    /// are already requested directly by another active stream of this subscription. The overall picture will be the
    /// following: streams which directly requested specific partitions will consume from them; streams that didn’t
    /// specify which partitions to consume will consume partitions that left - Nakadi will autobalance free partitions
    /// among these streams (balancing happens by number of partitions).
    ///
    /// Specifying partitions to consume is not a trivial way to consume as it will require additional coordination
    /// effort from the client application, that’s why it should only be used if such way of consumption should be
    /// implemented due to some specific requirements.
    ///
    /// Also, when using streams with directly assigned partitions, it is the user’s responsibility to detect, and react
    /// to, changes in the number of partitions in the subscription (following the re-partitioning of an event type).
    /// Using the GET /subscriptions/{subscription_id}/stats endpoint can be helpful.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
    fn request_stream<T: Into<FlowId>>(
        &self,
        subscription_id: SubscriptionId,
        parameters: &StreamParameters,
        flow_id: T,
    ) -> ApiFuture<SubscriptionStreamChunks>;
}

pub trait SubscriptionCommitApi {
    /// Endpoint for committing offsets of the subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_post)
    fn commit_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> ApiFuture<CursorCommitResults>;
}

/// A stream of of chunks directly from Nakadi
pub struct SubscriptionStreamChunks {
    pub stream_id: StreamId,
    pub chunks: BytesStream,
}

impl<'a> SubscriptionStreamChunks {
    pub fn parts(self) -> (StreamId, BytesStream) {
        (self.stream_id, self.chunks)
    }
}
