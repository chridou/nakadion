//! A connector to directly consume Nakadi Streams
use std::error::Error as StdError;
use std::fmt;
use std::time::Instant;

use futures::{
    future::{BoxFuture, FutureExt},
    stream::{BoxStream, StreamExt},
};
use http::StatusCode;
use serde::de::DeserializeOwned;
use serde_json;
use tokio::time::timeout;

pub use crate::components::{
    streams::{BatchLine, BatchLineError, NakadiFrame},
    IoError,
};

pub use crate::api::BytesStream;
pub use crate::api::{NakadiApiError, SubscriptionApi, SubscriptionStreamChunks};
pub use crate::consumer::ConnectStreamTimeoutSecs;
use crate::instrumentation::{Instrumentation, Instruments};
pub use crate::nakadi_types::{
    subscription::{StreamId, StreamParameters, SubscriptionCursor, SubscriptionId},
    Error, FlowId, RandomFlowId,
};

pub type ConnectResult<T> = Result<T, ConnectError>;
pub type ConnectFuture<'a, T> = BoxFuture<'a, ConnectResult<T>>;

pub type EventsStream<E> = BoxStream<'static, Result<(StreamedBatchMeta, Option<Vec<E>>), Error>>;
pub type FramesStream = BoxStream<'static, Result<NakadiFrame, IoError>>;
pub type BatchStream = BoxStream<'static, Result<BatchLine, BatchLineError>>;

/// Gives a `Connects`
pub trait ProvidesConnector {
    fn connector(&self) -> Box<dyn Connects + Send + Sync + 'static>;
}

impl<T> ProvidesConnector for T
where
    T: SubscriptionApi + Sync + Send + 'static + Clone,
{
    fn connector(&self) -> Box<dyn Connects + Send + Sync + 'static> {
        Box::new(Connector::from_client(self.clone()))
    }
}

/// Can connect to a Nakadi Stream
pub trait Connects {
    /// Return instrumentation.
    ///
    /// Returns `Instrumentation::default()` by default.
    fn instrumentation(&self) -> Instrumentation {
        Instrumentation::default()
    }

    /// Get a stream of chunks directly from Nakadi.
    fn connect(&self, subscription_id: SubscriptionId) -> ConnectFuture<SubscriptionStreamChunks>;

    fn set_flow_id(&mut self, flow_id: FlowId);

    fn set_connect_stream_timeout_secs(&mut self, connect_stream_timeout: ConnectStreamTimeoutSecs);

    fn set_instrumentation(&mut self, instrumentation: Instrumentation);

    fn stream_parameters_mut(&mut self) -> &mut StreamParameters;
}

/// Parameters to configure the `Connector`
#[derive(Default, Debug, Clone)]
pub struct ConnectorParams {
    pub stream_params: StreamParameters,
    pub connect_stream_timeout_secs: ConnectStreamTimeoutSecs,
    pub instrumentation: Instrumentation,
}

/// A `Connector` to connect to a Nakadi Stream
pub struct Connector<C> {
    stream_client: C,
    params: ConnectorParams,
    flow_id: Option<FlowId>,
}

impl<C> Connector<C>
where
    C: SubscriptionApi + Send + Sync + 'static,
{
    pub fn new(stream_client: C, params: ConnectorParams) -> Self {
        Self {
            stream_client,
            params,
            flow_id: None,
        }
    }

    pub fn from_client(stream_client: C) -> Self {
        Self {
            stream_client,
            params: ConnectorParams::default(),
            flow_id: None,
        }
    }
}

impl<C> From<C> for Connector<C>
where
    C: SubscriptionApi + Send + Sync + 'static,
{
    fn from(c: C) -> Self {
        Self::from_client(c)
    }
}

impl<C> Connects for Connector<C>
where
    C: SubscriptionApi + Sync + Send + 'static,
{
    fn instrumentation(&self) -> Instrumentation {
        self.params.instrumentation.clone()
    }
    fn connect(&self, subscription_id: SubscriptionId) -> ConnectFuture<SubscriptionStreamChunks> {
        async move {
            let flow_id = self.flow_id.clone().unwrap_or_else(|| RandomFlowId.into());
            let f = self.stream_client.request_stream(
                subscription_id,
                &self.params.stream_params,
                flow_id,
            );
            let connect_timeout = self.params.connect_stream_timeout_secs.into_duration();
            let started = Instant::now();
            match timeout(connect_timeout, f).await {
                Ok(Ok(stream)) => {
                    self.params
                        .instrumentation
                        .stream_connect_attempt_success(started.elapsed());
                    Ok(stream)
                }
                Ok(Err(api_error)) => {
                    self.params
                        .instrumentation
                        .stream_connect_attempt_failed(started.elapsed());
                    Err(api_error.into())
                }
                Err(err) => {
                    self.params
                        .instrumentation
                        .stream_connect_attempt_failed(started.elapsed());
                    Err(ConnectError::io()
                        .context(format!(
                            "Connecting to Nakadi for a stream timed ot after {:?}.",
                            started.elapsed()
                        ))
                        .caused_by(err))
                }
            }
        }
        .boxed()
    }

    fn set_flow_id(&mut self, flow_id: FlowId) {
        self.flow_id = Some(flow_id);
    }

    fn set_connect_stream_timeout_secs(
        &mut self,
        connect_stream_timeout_secs: ConnectStreamTimeoutSecs,
    ) {
        self.params.connect_stream_timeout_secs = connect_stream_timeout_secs;
    }

    fn set_instrumentation(&mut self, instrumentation: Instrumentation) {
        self.params.instrumentation = instrumentation;
    }

    fn stream_parameters_mut(&mut self) -> &mut StreamParameters {
        &mut self.params.stream_params
    }
}

/// Error returned on failed connect attempts for a stream
#[derive(Debug)]
pub struct ConnectError {
    context: Option<String>,
    kind: ConnectErrorKind,
    source: Option<Box<dyn StdError + Send + 'static>>,
}

impl ConnectError {
    pub fn new(kind: ConnectErrorKind) -> Self {
        Self {
            context: None,
            kind,
            source: None,
        }
    }

    pub fn not_found() -> Self {
        Self::new(ConnectErrorKind::SubscriptionNotFound)
    }
    pub fn access_denied() -> Self {
        Self::new(ConnectErrorKind::AccessDenied)
    }
    pub fn unprocessable() -> Self {
        Self::new(ConnectErrorKind::Unprocessable)
    }
    pub fn bad_request() -> Self {
        Self::new(ConnectErrorKind::BadRequest)
    }
    pub fn io() -> Self {
        Self::new(ConnectErrorKind::Io)
    }
    pub fn other() -> Self {
        Self::new(ConnectErrorKind::Other)
    }

    pub fn context<T: Into<String>>(mut self, context: T) -> Self {
        self.context = Some(context.into());
        self
    }

    pub fn caused_by<E: StdError + Send + 'static>(mut self, source: E) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn kind(&self) -> ConnectErrorKind {
        self.kind
    }
}

impl StdError for ConnectError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn StdError + 'static))
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref context) = self.context {
            write!(f, "{}", context)?;
        } else {
            write!(f, "could not connect to stream: {}", self.kind)?;
        }
        Ok(())
    }
}

impl From<ConnectError> for Error {
    fn from(err: ConnectError) -> Self {
        Error::from_error(err)
    }
}

impl From<NakadiApiError> for ConnectError {
    fn from(api_error: NakadiApiError) -> Self {
        if let Some(status) = api_error.status() {
            match status {
                StatusCode::NOT_FOUND => ConnectError::not_found().caused_by(api_error),
                StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                    ConnectError::access_denied().caused_by(api_error)
                }
                StatusCode::BAD_REQUEST => ConnectError::bad_request().caused_by(api_error),
                StatusCode::UNPROCESSABLE_ENTITY => {
                    ConnectError::unprocessable().caused_by(api_error)
                }
                _ => ConnectError::other().caused_by(api_error),
            }
        } else if api_error.is_io_error() {
            ConnectError::bad_request().caused_by(api_error)
        } else {
            ConnectError::other().caused_by(api_error)
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnectErrorKind {
    SubscriptionNotFound,
    AccessDenied,
    Unprocessable,
    BadRequest,
    Io,
    Other,
}

impl fmt::Display for ConnectErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectErrorKind::SubscriptionNotFound => write!(f, "subscription not found")?,
            ConnectErrorKind::AccessDenied => write!(f, "access denied")?,
            ConnectErrorKind::Unprocessable => write!(f, "unprocessable")?,
            ConnectErrorKind::BadRequest => write!(f, "bad request")?,
            ConnectErrorKind::Io => write!(f, "io")?,
            ConnectErrorKind::Other => write!(f, "other")?,
        }
        Ok(())
    }
}

/// Metadata on the currently received batch
#[derive(Debug, Clone)]
pub struct StreamedBatchMeta {
    pub cursor: SubscriptionCursor,
    pub received_at: Instant,
    pub frame_id: usize,
}

/// `Connects` with extensions
pub trait ConnectsExt: Connects + Sync {
    /// Get a stream of frames(lines) directly from Nakadi.
    fn frame_stream(
        &self,
        subscription_id: SubscriptionId,
    ) -> ConnectFuture<(StreamId, FramesStream)> {
        async move {
            let (stream_id, chunks) = self.connect(subscription_id).await?.parts();

            let framed =
                crate::components::streams::FramedStream::new(chunks, self.instrumentation());

            Ok((stream_id, framed.boxed()))
        }
        .boxed()
    }

    /// Get a stream of analyzed lines.
    fn batch_stream(
        &self,
        subscription_id: SubscriptionId,
    ) -> ConnectFuture<(StreamId, BatchStream)> {
        async move {
            let (stream_id, frames) = self.frame_stream(subscription_id).await?;

            let lines =
                crate::components::streams::BatchLineStream::new(frames, self.instrumentation());

            Ok((stream_id, lines.boxed()))
        }
        .boxed()
    }

    /// Get a stream of deserialized events.
    fn events_stream<E: DeserializeOwned>(
        &self,
        subscription_id: SubscriptionId,
    ) -> ConnectFuture<(StreamId, EventsStream<E>)> {
        async move {
            let (stream_id, batches) = self.batch_stream(subscription_id).await?;

            let events = batches.map(|r| match r {
                Ok(batch_line) => {
                    let cursor = batch_line.cursor_deserialized::<SubscriptionCursor>()?;
                    let meta = StreamedBatchMeta {
                        cursor,
                        received_at: batch_line.received_at(),
                        frame_id: batch_line.frame_id(),
                    };

                    if let Some(events_bytes) = batch_line.events_bytes() {
                        let events = serde_json::from_slice::<Vec<E>>(&events_bytes)?;
                        Ok((meta, Some(events)))
                    } else {
                        Ok((meta, None))
                    }
                }
                Err(err) => Err(Error::from_error(err)),
            });

            Ok((stream_id, events.boxed()))
        }
        .boxed()
    }
}

impl<T> ConnectsExt for T where T: Connects + Sync {}

impl Connects for Box<dyn Connects + Send + Sync> {
    fn instrumentation(&self) -> Instrumentation {
        self.as_ref().instrumentation()
    }

    /// Get a stream of chunks directly from Nakadi.
    fn connect(&self, subscription_id: SubscriptionId) -> ConnectFuture<SubscriptionStreamChunks> {
        self.as_ref().connect(subscription_id)
    }

    fn set_flow_id(&mut self, flow_id: FlowId) {
        self.as_mut().set_flow_id(flow_id);
    }

    fn set_connect_stream_timeout_secs(
        &mut self,
        connect_stream_timeout_secs: ConnectStreamTimeoutSecs,
    ) {
        self.as_mut()
            .set_connect_stream_timeout_secs(connect_stream_timeout_secs)
    }

    fn set_instrumentation(&mut self, instrumentation: Instrumentation) {
        self.as_mut().set_instrumentation(instrumentation)
    }

    fn stream_parameters_mut(&mut self) -> &mut StreamParameters {
        self.as_mut().stream_parameters_mut()
    }
}
