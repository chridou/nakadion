use std::error::Error;
use std::fmt;


use serde::Serialize;
use http::StatusCode;
use http_api_problem::HttpApiProblem;

use crate::event_stream::EventStream;
use crate::model::*;

mod reqwest_client;

pub type NakadiApiResult<T> = Result<T, NakadiApiError>;

trait MonitoringApi {
    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
    fn get_cursor_distances<T: Into<FlowId>>(
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: T,
    ) -> NakadiApiResult<CursorDistanceResult>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursors-lag_post)
    fn get_cursor_lag<T: Into<FlowId>>(
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: T,
    ) -> NakadiApiResult<CursorLagResult>;
}

trait SchemaRegistryApi {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types<T: Into<FlowId>>(flow_id: T) -> NakadiApiResult<Vec<EventType>>;

    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type<T: Into<FlowId>>(
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()>;

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        flow_id: T,
    ) -> NakadiApiResult<EventType>;

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type<T: Into<FlowId>>(name: &EventTypeName, flow_id: T) -> NakadiApiResult<()>;
}

trait StreamApi {
    /// Publishes a batch of Events of this EventType. All items must be of the EventType
    /// identified by name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
    fn publish_events<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        events: &[E],
        flow_id: T,
    ) -> Result<(), PublishError> {
        unimplemented!()
    }

    fn publish_event<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        event: &E,
        flow_id: T,
    ) -> Result<(), PublishError> {
        unimplemented!()
    }

    fn publish_raw_events<T: Into<FlowId>>(
        name: &EventTypeName,
        raw_events: &[u8],
        flow_id: T,
    ) -> Result<(), PublishError>;
}

trait SubscriptionApi {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription<T: Into<FlowId>>(
        input: &SubscriptionInput,
        flow_id: T,
    ) -> NakadiApiResult<Subcription>;

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription<T: Into<FlowId>>(
        name: SubscriptionId,
        flow_id: T,
    ) -> NakadiApiResult<Subcription>;

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth<T: Into<FlowId>>(
        name: &SubscriptionInput,
        flow_id: T,
    ) -> NakadiApiResult<Subcription>;

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription<T: Into<FlowId>>(id: SubscriptionId, flow_id: T) -> NakadiApiResult<()>;

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_committed_offsets<T: Into<FlowId>>(
        id: SubscriptionId,
        flow_id: T,
    ) -> NakadiApiResult<Vec<SubscriptionCursor>>;

    /// Endpoint for committing offsets of the subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_post)
    fn commit_cursors<T: Into<FlowId>>(
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> Result<Committed, CommitError>;

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors<T: Into<FlowId>>(
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> Result<Committed, CommitError>;

    /// Starts a new stream for reading events from this subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
    fn events<T: Into<FlowId>>(
        id: SubscriptionId,
        parameters: &StreamParameters,
        flow_id: T,
    ) -> Result<EventStream, ConnectError>;

    fn subscription_stats<T: Into<FlowId>>(
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: T,
    ) -> Result<Vec<SubscriptionEventTypeStats>, NakadiApiError>;
}

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

pub struct NakadiApiError;

pub struct PublishError;

pub struct CommitError;

pub struct ConnectError;

#[derive(Debug)]
pub struct RemoteCallError {
    message: Option<String>,
    status_code: Option<StatusCode>,
    cause: Option<Box<dyn Error + Send + 'static>>,
    pub(crate) problem: Option<HttpApiProblem>,
    kind: RemoteCallErrorKind,
}


impl RemoteCallError {
    pub(crate) fn new<M: Into<String>>(kind:RemoteCallErrorKind, message: M, status_code: Option<StatusCode>) -> Self {
        Self {
            message: Some(message.into()),
            status_code,
            kind,
            problem: None,
            cause: None
        }
    }

    pub fn is_server(&self) -> bool {
        self.kind == RemoteCallErrorKind::Server
    }

    pub fn is_client(&self) -> bool {
        self.kind == RemoteCallErrorKind::Client
    }

    pub fn is_serialization(&self) -> bool {
        self.kind == RemoteCallErrorKind::Serialization
    }

   pub fn is_io(&self) -> bool {
        self.kind == RemoteCallErrorKind::Io
    }

   pub fn is_other(&self) -> bool {
        self.kind == RemoteCallErrorKind::Other
    }

    pub fn status_code(&self) -> Option<StatusCode> {
        self.status_code
    }

    pub fn problem(&self) -> Option<&HttpApiProblem> {
        self.problem.as_ref()
    }

    pub fn is_retry_suggested(&self) -> bool {
        match self.kind {
            RemoteCallErrorKind::Client => false,
            RemoteCallErrorKind::Server => true,
            RemoteCallErrorKind::Serialization => false,
            RemoteCallErrorKind::Io => true,
            RemoteCallErrorKind::Other => false,
        }
    }

    pub(crate) fn with_cause<E: Error + Send + 'static>(mut self, cause: E ) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }
}

impl fmt::Display for RemoteCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RemoteCallErrorKind::*;

        match self.kind {
            Client => {
            write!(f, "client error")?;

            },
            Server => {
            write!(f, "server error")?;

            },
            Serialization => {
            write!(f, "serialization error")?;

            },
            Io => {
            write!(f, "io error")?;

            },
            Other => {
            write!(f, "other error")?;

            },
        }

        if let Some(status_code) = self.status_code {
            write!(f, " - status: {}", status_code)?;
        }

       if let Some(ref message) = self.message {
            write!(f, " - message: {}", message)?;
        }

        Ok(())
    }
}

impl Error for RemoteCallError {
    fn cause(&self) -> Option<&Error> {
        self.cause.as_ref().map(|e| &**e as &Error)
    }
}

impl From<serde_json::Error> for RemoteCallError {
    fn from(err: serde_json::Error) -> Self {
        Self{
               message: Some("de/-serialization error".to_string()),
    status_code: None,
    cause: Some(Box::new(err)),
    problem: None,
    kind: RemoteCallErrorKind::Serialization,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteCallErrorKind {
    Client,
    Server,
    Serialization,
    Io,
    Other,
}

