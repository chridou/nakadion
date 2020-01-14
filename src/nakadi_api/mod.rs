use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::{future::BoxFuture, stream::Stream};
use http::StatusCode;
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Serialize};
use url::Url;

use crate::event_stream::EventStream;
use crate::model::*;

use dispatch_http_request::RemoteCallError;

pub mod client;
pub mod dispatch_http_request;

type ApiFuture<'a, T> = BoxFuture<'a, Result<T, NakadiApiError>>;

trait MonitoringApi {
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
        cursors: &Vec<Cursor>,
        flow_id: FlowId,
    ) -> ApiFuture<CursorLagResult>;
}
/*
trait SchemaRegistryApi {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types(flow_id: FlowId) -> ApiFuture<Vec<EventType>>;

    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type(event_type: &EventType, flow_id: FlowId) -> ApiFuture<()>;

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type(name: &EventTypeName, flow_id: FlowId) -> ApiFuture<EventType>;

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type(
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: FlowId,
    ) -> ApiFuture<()>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type(name: &EventTypeName, flow_id: FlowId) -> ApiFuture<()>;
}

struct PublishFuture {
    inner: Box<dyn Future<Output = Result<(), PublishError>> + Send>,
}

trait PublishApi {
    /// Publishes a batch of Events of this EventType. All items must be of the EventType
    /// identified by name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
    fn publish_events<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        events: &[E],
        flow_id: FlowId,
    ) -> PublishFuture {
        unimplemented!()
    }

    fn publish_event<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        event: &E,
        flow_id: FlowId,
    ) -> PublishFuture {
        unimplemented!()
    }

    fn publish_raw_events(
        name: &EventTypeName,
        raw_events: &[u8],
        flow_id: FlowId,
    ) -> PublishFuture;
}

trait SubscriptionApi {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription(input: &SubscriptionInput, flow_id: FlowId) -> ApiFuture<Subscription>;

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription(name: SubscriptionId, flow_id: FlowId) -> ApiFuture<Subscription>;

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth(name: &SubscriptionInput, flow_id: FlowId) -> ApiFuture<Subscription>;

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription(id: SubscriptionId, flow_id: FlowId) -> ApiFuture<()>;

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_committed_offsets(
        id: SubscriptionId,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionCursor>>;

    fn subscription_stats(
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionEventTypeStats>>;

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors(
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: FlowId,
    ) -> ApiFuture<()>;
}

pub struct CommitFuture {
    inner: Box<dyn Future<Output = Result<Committed, CommitError>> + Send + 'static>,
}

pub trait CommitApi {
    /// Endpoint for committing offsets of the subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_post)
    fn commit_cursors(
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: FlowId,
    ) -> CommitFuture;
}

pub struct ConnectFuture {
    inner: Box<dyn Future<Output = Result<EventStream, ConnectError>> + Send + 'static>,
}

pub trait ConnectApi {
    /// Starts a new stream for reading events from this subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
    fn connect(id: SubscriptionId, parameters: &StreamParameters, flow_id: FlowId)
        -> ConnectFuture;
}

*/
pub struct StreamParameters {
    partitions: Vec<Partition>,
    max_uncommitted_events: u32,
    batch_limit: u32,
    stream_limit: u32,
    batch_flush_timeout: u32,
    stream_timeout: u32,
    commit_timeout: u32,
}

pub enum Committed {
    AllCommitted,
    NotAllCommitted(Vec<CommitResult>),
}

#[derive(Debug)]
struct NakadiApiError<P = HttpApiProblem> {
    message: String,
    cause: Option<Box<dyn Error + Send + 'static>>,
    payload: Option<P>,
    kind: NakadiApiErrorKind,
}

impl NakadiApiError<HttpApiProblem> {
    pub fn new<T: Into<String>>(kind: NakadiApiErrorKind, message: T) -> Self {
        Self {
            message: message.into(),
            cause: None,
            payload: None,
            kind,
        }
    }
}

impl<P> NakadiApiError<P> {
    pub fn caused_by<E>(mut self, err: E) -> Self
    where
        E: Error + Send + 'static,
    {
        self.cause = Some(Box::new(err));
        self
    }

    pub fn with_payload<PP>(self, payload: PP) -> NakadiApiError<PP> {
        NakadiApiError {
            message: self.message,
            cause: self.cause,
            payload: Some(payload),
            kind: self.kind,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> NakadiApiErrorKind {
        self.kind
    }

    pub fn payload(&self) -> Option<&P> {
        self.payload.as_ref()
    }

    pub fn try_into_payload(self) -> Result<P, Self> {
        if self.payload.is_some() {
            Ok(self.payload.unwrap())
        } else {
            Err(self)
        }
    }
}

impl<P> Error for NakadiApiError<P>
where
    P: fmt::Debug + fmt::Display,
{
    fn cause(&self) -> Option<&dyn Error> {
        self.cause.as_ref().map(|p| &**p as &dyn Error)
    }
}

impl<P> fmt::Display for NakadiApiError<P>
where
    P: fmt::Display,
{
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
pub struct PublishError;

pub struct CommitError;

pub struct ConnectError;
