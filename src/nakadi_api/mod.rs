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

pub use self::client::ApiClient;

mod client;
pub mod dispatch_http_request;

type ApiFuture<'a, T> = BoxFuture<'a, Result<T, NakadiApiError>>;

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
        cursors: &Vec<Cursor>,
        flow_id: FlowId,
    ) -> ApiFuture<CursorLagResult>;
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

pub trait PublishApi {
    /// Publishes a batch of Events of this EventType. All items must be of the EventType
    /// identified by name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
    fn publish_events<E: Serialize>(
        &self,
        name: &EventTypeName,
        events: &[E],
        flow_id: FlowId,
    ) -> ApiFuture<BatchResponse>;
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

pub trait ConnectApi {
    /// Starts a new stream for reading events from this subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
    fn connect(id: SubscriptionId, parameters: &StreamParameters, flow_id: FlowId)
        -> ConnectFuture;
}

*/
pub struct StreamParameters {
    pub partitions: Vec<Partition>,
    pub max_uncommitted_events: u32,
    pub batch_limit: u32,
    pub stream_limit: u32,
    pub batch_flush_timeout: u32,
    pub stream_timeout: u32,
    pub commit_timeout: u32,
}

pub enum Committed {
    AllCommitted,
    NotAllCommitted(Vec<CommitResult>),
}

#[derive(Debug)]
pub struct NakadiApiError {
    message: String,
    cause: Option<Box<dyn Error + Send + 'static>>,
    problem: Option<HttpApiProblem>,
    kind: NakadiApiErrorKind,
}

impl NakadiApiError {
    pub fn new<T: Into<String>>(kind: NakadiApiErrorKind, message: T) -> Self {
        Self {
            message: message.into(),
            cause: None,
            problem: None,
            kind,
        }
    }
}

impl NakadiApiError {
    pub fn caused_by<E>(mut self, err: E) -> Self
    where
        E: Error + Send + 'static,
    {
        self.cause = Some(Box::new(err));
        self
    }

    pub fn with_problem(self, problem: HttpApiProblem) -> NakadiApiError {
        NakadiApiError {
            message: self.message,
            cause: self.cause,
            problem: Some(problem),
            kind: self.kind,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> NakadiApiErrorKind {
        self.kind
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
}

impl Error for NakadiApiError {
    fn cause(&self) -> Option<&dyn Error> {
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
