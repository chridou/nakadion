//! A connector to directly consume Nakadi Streams
use std::error::Error as StdError;
use std::fmt;
use std::time::{Duration, Instant};

use futures::stream::{BoxStream, StreamExt};
use http::StatusCode;
use tokio::time::{delay_for, timeout};

pub use crate::components::{
    streams::{BatchLine, BatchLineError, NakadiFrame},
    IoError,
};
use crate::logging::{DevNullLoggingAdapter, Logger};

pub use crate::api::BytesStream;
pub use crate::api::{NakadiApiError, SubscriptionStreamApi, SubscriptionStreamChunks};
use crate::instrumentation::{Instrumentation, Instruments};
pub use crate::nakadi_types::{
    subscription::{StreamId, StreamParameters, SubscriptionCursor, SubscriptionId},
    Error, FlowId, RandomFlowId,
};

mod config;
pub use config::*;

pub type EventsStream<E> = BoxStream<'static, Result<(StreamedBatchMeta, Option<Vec<E>>), Error>>;
pub type FramesStream = BoxStream<'static, Result<NakadiFrame, IoError>>;
pub type BatchStream = BoxStream<'static, Result<BatchLine, BatchLineError>>;

/// A `Connector` to connect to a Nakadi Stream
///
/// ## Usage
///
/// If other than default values should be used the `Connector`
/// can be configured via its mutable methods.
pub struct Connector<C> {
    stream_client: C,
    config: ConnectConfig,
    flow_id: Option<FlowId>,
    logger: Box<dyn Logger>,
    instrumentation: Instrumentation,
}

impl<C> Connector<C>
where
    C: SubscriptionStreamApi + Send + Sync + 'static,
{
    pub fn new_with_config(stream_client: C, config: ConnectConfig) -> Self {
        Self {
            stream_client,
            config,
            flow_id: None,
            logger: Box::new(DevNullLoggingAdapter),
            instrumentation: Instrumentation::default(),
        }
    }

    pub fn new(stream_client: C) -> Self {
        Self {
            stream_client,
            config: ConnectConfig::default(),
            flow_id: None,
            logger: Box::new(DevNullLoggingAdapter),
            instrumentation: Instrumentation::default(),
        }
    }

    /// Connect to Nakadi for a stream of byte chunks.
    pub async fn connect(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<SubscriptionStreamChunks, ConnectError> {
        self.connect_abortable(subscription_id, || false).await
    }

    /// Connect to Nakadi for a stream of byte chunks.
    ///
    /// A closure which is regularly polled must be provided for
    /// the connector to determine whether connecting to Nakadi
    /// shall be aborted.
    pub async fn connect_abortable<F>(
        &self,
        subscription_id: SubscriptionId,
        abort: F,
    ) -> Result<SubscriptionStreamChunks, ConnectError>
    where
        F: FnMut() -> bool + Send,
    {
        let flow_id = self.flow_id.clone().unwrap_or_else(FlowId::random);
        self.connect_to_stream(subscription_id, flow_id, abort)
            .await
    }

    /// Sets the logger to be used from now on.
    ///
    /// The `Logger` is used mostly for emitting retry warnings.
    pub fn set_logger<L: Logger>(&mut self, logger: L) {
        self.logger = Box::new(logger);
    }

    /// Sets the logger to be used from now on in a builder method style.
    ///
    /// The `Logger` is used mostly for emitting retry warnings.
    pub fn logger<L: Logger>(mut self, logger: L) -> Self {
        self.set_logger(logger);
        self
    }

    /// Get the currently used instrumentation
    pub fn instrumentation(&self) -> Instrumentation {
        self.instrumentation.clone()
    }

    /// Sets the `FlowId` for the next connect attempts.
    ///
    /// If not set, a random `FlowId` will be generated.
    pub fn set_flow_id(&mut self, flow_id: FlowId) {
        self.flow_id = Some(flow_id);
    }

    /// Set the `ConnectConfig` in a builder style
    pub fn configure(mut self, config: ConnectConfig) -> Self {
        self.set_config(config);
        self
    }

    /// Set the `ConnectConfig` which shall be used for the
    /// next connect attempt
    pub fn set_config(&mut self, config: ConnectConfig) {
        self.config = config;
    }

    /// Returns a reference to the currently used config
    pub fn config(&self) -> &ConnectConfig {
        &self.config
    }

    /// Returns a mutable reference to the currently used config
    pub fn config_mut(&mut self) -> &mut ConnectConfig {
        &mut self.config
    }

    /// Sets the `Instrumentation` to be used with this connector.
    ///
    /// Uses the `Instrumentation` of the consumer since
    /// this is a component used by the `Consumer` itself.
    pub fn set_instrumentation(&mut self, instrumentation: Instrumentation) {
        self.instrumentation = instrumentation;
    }

    /// Gets a mutable reference to the `StreamParameters` of the currently
    /// used `ConnectConfig`
    pub fn stream_parameters_mut(&mut self) -> &mut StreamParameters {
        &mut self.config.stream_parameters
    }

    /// Get a stream of frames(lines) directly from Nakadi.
    pub async fn frame_stream(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(StreamId, FramesStream), ConnectError> {
        let (stream_id, chunks) = self.connect(subscription_id).await?.parts();

        let framed = crate::components::streams::FramedStream::new(chunks, self.instrumentation());

        Ok((stream_id, framed.boxed()))
    }

    /// Get a stream of analyzed (parsed) lines.
    pub async fn batch_stream(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(StreamId, BatchStream), ConnectError> {
        let (stream_id, frames) = self.frame_stream(subscription_id).await?;

        let lines =
            crate::components::streams::BatchLineStream::new(frames, self.instrumentation());

        Ok((stream_id, lines.boxed()))
    }

    /// Get a stream of deserialized events.
    pub async fn events_stream<E: serde::de::DeserializeOwned>(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(StreamId, EventsStream<E>), ConnectError> {
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

    async fn connect_to_stream<F>(
        &self,
        subscription_id: SubscriptionId,
        flow_id: FlowId,
        check_abort: F,
    ) -> Result<SubscriptionStreamChunks, ConnectError>
    where
        F: FnMut() -> bool + Send,
    {
        if let Some(connect_timeout) = self
            .config()
            .timeout_secs
            .unwrap_or_default()
            .into_duration_opt()
        {
            let started = Instant::now();
            match timeout(
                connect_timeout,
                self.connect_to_stream_with_retries(subscription_id, flow_id, check_abort),
            )
            .await
            {
                Ok(r) => r,
                Err(err) => {
                    self.instrumentation.stream_not_connected(started.elapsed());
                    Err(ConnectError::aborted()
                        .context(format!(
                            "Connecting to Nakadi for a stream completely timed out after {:?}.",
                            started.elapsed()
                        ))
                        .caused_by(err))
                }
            }
        } else {
            self.connect_to_stream_with_retries(subscription_id, flow_id, check_abort)
                .await
        }
    }

    async fn connect_to_stream_with_retries<F>(
        &self,
        subscription_id: SubscriptionId,
        flow_id: FlowId,
        mut check_abort: F,
    ) -> Result<SubscriptionStreamChunks, ConnectError>
    where
        F: FnMut() -> bool + Send,
    {
        let instrumentation = self.instrumentation.clone();
        let abort_connect_on_subscription_not_found: bool = self
            .config()
            .abort_connect_on_subscription_not_found
            .unwrap_or_default()
            .into();
        let abort_on_auth_error: bool = self.config.abort_on_auth_error.unwrap_or_default().into();

        let mut backoff = Backoff::new(self.config.max_retry_delay_secs.unwrap_or_default().into());

        let connect_started_at = Instant::now();
        let mut num_attempts = 1;
        loop {
            if check_abort() {
                return Err(ConnectError::aborted());
            }

            match self
                .connect_single_attempt(subscription_id, flow_id.clone())
                .await
            {
                Ok(stream) => {
                    instrumentation.stream_connected(connect_started_at.elapsed());

                    return Ok(stream);
                }
                Err(err) => {
                    let retry_allowed = match err.kind() {
                        ConnectErrorKind::Aborted => false,
                        ConnectErrorKind::AccessDenied => !abort_on_auth_error,
                        ConnectErrorKind::BadRequest => false,
                        ConnectErrorKind::Io => true,
                        ConnectErrorKind::NakadiError => true,
                        ConnectErrorKind::Other => false,
                        ConnectErrorKind::Unprocessable => false,
                        ConnectErrorKind::Conflict => true,
                        ConnectErrorKind::SubscriptionNotFound => {
                            !abort_connect_on_subscription_not_found
                        }
                    };
                    if retry_allowed {
                        let delay = backoff.next();
                        self.logger.warn(format_args!(
                            "connect attempt {} failed after {:?} have already elapsed (retry in {:?}) \
                            with error: {}",
                            num_attempts, connect_started_at.elapsed(), delay, err
                        ));
                        num_attempts += 1;
                        delay_for(delay).await;
                        continue;
                    } else {
                        instrumentation.stream_not_connected(connect_started_at.elapsed());
                        return Err(err);
                    }
                }
            }
        }
    }

    async fn connect_single_attempt(
        &self,
        subscription_id: SubscriptionId,
        flow_id: FlowId,
    ) -> Result<SubscriptionStreamChunks, ConnectError> {
        let stream_params = &self.config.stream_parameters;
        let f = self
            .stream_client
            .request_stream(subscription_id, &stream_params, flow_id);
        let started = Instant::now();
        let attempt_timeout = self
            .config
            .attempt_timeout_secs
            .unwrap_or_default()
            .into_duration();
        match timeout(attempt_timeout, f).await {
            Ok(Ok(stream)) => {
                self.instrumentation
                    .stream_connect_attempt_success(started.elapsed());
                Ok(stream)
            }
            Ok(Err(api_error)) => {
                self.instrumentation
                    .stream_connect_attempt_failed(started.elapsed());
                Err(api_error.into())
            }
            Err(err) => {
                self.instrumentation
                    .stream_connect_attempt_failed(started.elapsed());
                Err(ConnectError::io()
                    .context(format!(
                        "Request to Nakadi for a stream timed ot after {:?}.",
                        started.elapsed()
                    ))
                    .caused_by(err))
            }
        }
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

    pub fn nakadi() -> Self {
        Self::new(ConnectErrorKind::NakadiError)
    }

    pub fn aborted() -> Self {
        Self::new(ConnectErrorKind::Aborted)
    }

    pub fn conflict() -> Self {
        Self::new(ConnectErrorKind::Conflict)
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

impl From<tokio::time::Elapsed> for ConnectError {
    fn from(err: tokio::time::Elapsed) -> Self {
        ConnectError::io()
            .context("request to nakadi timed out")
            .caused_by(err)
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
                StatusCode::CONFLICT => ConnectError::conflict().caused_by(api_error),
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
    Aborted,
    Conflict,
    NakadiError,
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
            ConnectErrorKind::Aborted => write!(f, "connect aborted")?,
            ConnectErrorKind::Conflict => write!(f, "conflict")?,
            ConnectErrorKind::NakadiError => write!(f, "nakadi error")?,
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

/*
/// `Connects` with extensions
pub trait ConnectsExt: Connects + Sync {
 }
*/

// Sequence of backoffs after failed connect attempts
const CONNECT_RETRY_BACKOFF_SECS: &[u64] = &[
    0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 10, 10, 10, 10, 10, 20, 20, 20, 20,
    20, 30, 30, 30, 30, 30, 45, 45, 45, 45, 45, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 90, 90, 90,
    90, 90, 90, 90, 90, 90, 90, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 180, 180, 180,
    180, 180, 180, 180, 180, 180, 180, 240, 240, 240, 240, 240, 240, 240, 240, 240, 300, 300, 300,
    300, 300, 300, 300, 300, 300, 300, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 960, 960,
    960, 960, 960, 960, 960, 960, 960, 960, 960, 1920, 1920, 1920, 1920, 1920, 1920, 1920, 1920,
    1920, 1920, 2400, 2400, 2400, 2400, 2400, 2400, 2400, 2400, 2400, 2400, 3000, 3000, 3000, 3000,
    3000, 3000, 3000, 3000, 3000, 3000, 3600,
];

struct Backoff {
    max: u64,
    iter: Box<dyn Iterator<Item = u64> + Send + 'static>,
}

impl Backoff {
    pub fn new(max: u64) -> Self {
        let iter = Box::new(CONNECT_RETRY_BACKOFF_SECS.iter().copied());
        Backoff { iter, max }
    }

    pub fn next(&mut self) -> Duration {
        let d = if let Some(next) = self.iter.next() {
            next
        } else {
            self.max
        };

        let d = std::cmp::min(d, self.max);

        Duration::from_secs(d)
    }
}
