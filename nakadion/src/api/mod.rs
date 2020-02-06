use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::{
    future::BoxFuture,
    stream::{BoxStream, Stream},
};
use http::StatusCode;
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Serialize};
use url::Url;

use nakadi_types::model::event_type::*;
use nakadi_types::model::partition::*;
use nakadi_types::model::publishing::*;
use nakadi_types::model::subscription::*;
use nakadi_types::FlowId;

use dispatch_http_request::RemoteCallError;

pub use self::client::ApiClient;

mod client;
pub mod dispatch_http_request;

pub type ApiFuture<'a, T> = BoxFuture<'a, Result<T, NakadiApiError>>;
pub type BytesStream = BoxStream<'static, Result<Bytes, IoError>>;

#[derive(Debug)]
pub struct IoError(pub String);

impl IoError {
    pub fn new<T: Into<String>>(s: T) -> Self {
        Self(s.into())
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl Error for IoError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

pub trait MonitoringApi {
    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
    fn get_cursor_distances(
        &self,
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: FlowId,
    ) -> ApiFuture<CursorDistanceResult>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursors-lag_post)
    fn get_cursor_lag(
        &self,
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: FlowId,
    ) -> ApiFuture<Vec<Partition>>;

    fn get_event_type_partitions(
        &self,
        name: &EventTypeName,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<Partition>>;
}

pub trait SchemaRegistryApi {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types(&self, flow_id: FlowId) -> ApiFuture<Vec<EventType>>;

    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type(&self, event_type: &EventType, flow_id: FlowId) -> ApiFuture<()>;

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type(&self, name: &EventTypeName, flow_id: FlowId) -> ApiFuture<EventType>;

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type(
        &self,
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: FlowId,
    ) -> ApiFuture<()>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type(&self, name: &EventTypeName, flow_id: FlowId) -> ApiFuture<()>;
}

/// Possible error variants returned from publishing events
#[derive(Debug)]
pub enum PublishFailure {
    /// The submitted events were unprocessable so none were published
    Unprocessable(BatchResponse),
    /// Only events failed.
    PartialFailure(BatchResponse),
    /// There was an error that was not `Unprocessable`
    Other(NakadiApiError),
}

impl Error for PublishFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PublishFailure::Other(err) => err.source(),
            _ => None,
        }
    }
}

impl fmt::Display for PublishFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PublishFailure::Other(err) => write!(f, "{}", err)?,
            PublishFailure::PartialFailure(batch) => write!(f, "{}", batch)?,
            PublishFailure::Unprocessable(batch) => write!(f, "{}", batch)?,
        }

        Ok(())
    }
}

impl From<NakadiApiError> for PublishFailure {
    fn from(api_error: NakadiApiError) -> Self {
        Self::Other(api_error)
    }
}

impl From<RemoteCallError> for PublishFailure {
    fn from(remote_call_error: RemoteCallError) -> Self {
        let api_error = NakadiApiError::from(remote_call_error);
        Self::Other(api_error)
    }
}

type PublishFuture<'a> = BoxFuture<'a, Result<(), PublishFailure>>;

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
    fn publish_events<E: Serialize>(
        &self,
        event_type: &EventTypeName,
        events: &[E],
        flow_id: FlowId,
    ) -> PublishFuture;
}

pub trait SubscriptionApi {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription(
        &self,
        input: &SubscriptionInput,
        flow_id: FlowId,
    ) -> ApiFuture<Subscription>;

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription(&self, id: SubscriptionId, flow_id: FlowId) -> ApiFuture<Subscription>;

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth(&self, input: &SubscriptionInput, flow_id: FlowId) -> ApiFuture<()>;

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription(&self, id: SubscriptionId, flow_id: FlowId) -> ApiFuture<()>;

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_committed_offsets(
        &self,
        id: SubscriptionId,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionCursor>>;

    /// Exposes statistics of specified subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/stats_get)
    fn get_subscription_stats(
        &self,
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionEventTypeStats>>;

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors(
        &self,
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: FlowId,
    ) -> ApiFuture<()>;
}

pub trait SubscriptionCommitApi {
    /// Endpoint for committing offsets of the subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_post)
    fn commit_cursors(
        &self,
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: FlowId,
    ) -> ApiFuture<CursorCommitResults>;
}

/*

pub struct ConnectFuture {
    inner: Box<dyn Future<Output = Result<EventStream, ConnectError>> + Send + 'static>,
}
*/

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
    fn request_stream(
        &self,
        subscription_id: SubscriptionId,
        parameters: &StreamParameters,
        flow_id: FlowId,
    ) -> ApiFuture<SubscriptionStream>;
}

/// A stream of event type partitions from Nakadi
pub struct SubscriptionStream {
    pub stream_id: StreamId,
    pub stream: BytesStream,
}

impl SubscriptionStream {
    pub fn parts(self) -> (StreamId, BytesStream) {
        (self.stream_id, self.stream)
    }
}

pub trait NakadionEssentials:
    SubscriptionCommitApi + SubscriptionStreamApi + Send + Sync + 'static
{
}

impl<T> NakadionEssentials for T where
    T: SubscriptionCommitApi + SubscriptionStreamApi + Send + Sync + 'static
{
}

#[derive(Debug)]
pub struct NakadiApiError {
    message: String,
    cause: Option<Box<dyn Error + Send + 'static>>,
    problem: Option<HttpApiProblem>,
    kind: NakadiApiErrorKind,
    flow_id: Option<FlowId>,
}

impl NakadiApiError {
    pub fn new<T: Into<String>>(kind: NakadiApiErrorKind, message: T) -> Self {
        Self {
            message: message.into(),
            cause: None,
            problem: None,
            kind,
            flow_id: None,
        }
    }

    pub fn with_problem(self, problem: HttpApiProblem) -> NakadiApiError {
        NakadiApiError {
            message: self.message,
            cause: self.cause,
            problem: Some(problem),
            kind: self.kind,
            flow_id: None,
        }
    }

    pub fn caused_by<E>(mut self, err: E) -> Self
    where
        E: Error + Send + 'static,
    {
        self.cause = Some(Box::new(err));
        self
    }

    pub fn with_flow_id(mut self, flow_id: FlowId) -> Self {
        self.flow_id = Some(flow_id);
        self
    }

    pub fn with_maybe_flow_id(mut self, flow_id: Option<FlowId>) -> Self {
        self.flow_id = flow_id;
        self
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> NakadiApiErrorKind {
        self.kind
    }

    pub fn flow_id(&self) -> Option<&FlowId> {
        self.flow_id.as_ref()
    }

    pub fn problem(&self) -> Option<&HttpApiProblem> {
        self.problem.as_ref()
    }

    pub fn try_into_problem(self) -> Result<HttpApiProblem, Self> {
        if self.problem.is_some() {
            Ok(self.problem.unwrap())
        } else {
            Err(self)
        }
    }

    pub fn status(&self) -> Option<StatusCode> {
        self.problem().and_then(|p| p.status)
    }

    pub fn is_client_error(&self) -> bool {
        match self.kind {
            NakadiApiErrorKind::NotFound
            | NakadiApiErrorKind::BadRequest
            | NakadiApiErrorKind::UnprocessableEntity
            | NakadiApiErrorKind::AccessDenied => true,
            _ => self.status().map(|s| s.is_client_error()).unwrap_or(false),
        }
    }
}

impl Error for NakadiApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause.as_ref().map(|p| &**p as &dyn Error)
    }
}

impl fmt::Display for NakadiApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;

        Ok(())
    }
}

impl From<NakadiApiErrorKind> for NakadiApiError {
    fn from(kind: NakadiApiErrorKind) -> Self {
        Self::new(kind, kind.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NakadiApiErrorKind {
    NotFound,
    BadRequest,
    UnprocessableEntity,
    AccessDenied,
    ServerError,
    Io,
    Other,
}

impl fmt::Display for NakadiApiErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NakadiApiErrorKind::NotFound => {
                write!(f, "not found")?;
            }
            NakadiApiErrorKind::BadRequest => {
                write!(f, "bad request")?;
            }
            NakadiApiErrorKind::UnprocessableEntity => {
                write!(f, "unprocessable entity")?;
            }
            NakadiApiErrorKind::AccessDenied => {
                write!(f, "access denied")?;
            }
            NakadiApiErrorKind::ServerError => {
                write!(f, "server error")?;
            }
            NakadiApiErrorKind::Io => {
                write!(f, "io error")?;
            }
            NakadiApiErrorKind::Other => {
                write!(f, "other error")?;
            }
        }

        Ok(())
    }
}
