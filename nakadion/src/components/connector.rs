//! A connector to directly consume Nakadi Streams
use std::error::Error as StdError;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{
    future::FutureExt,
    stream::{BoxStream, StreamExt},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::time::{delay_for, timeout};

pub use crate::components::{
    streams::{BatchLine, BatchLineError, NakadiFrame},
    IoError,
};

pub use crate::api::BytesStream;
pub use crate::api::{NakadiApiError, SubscriptionStreamApi, SubscriptionStreamChunks};
use crate::instrumentation::{Instrumentation, Instruments};
pub use crate::nakadi_types::{
    subscription::{StreamId, StreamParameters, SubscriptionCursor, SubscriptionId},
    Error, FlowId, RandomFlowId,
};

pub type EventsStream<E> = BoxStream<'static, Result<(StreamedBatchMeta, Option<Vec<E>>), Error>>;
pub type FramesStream = BoxStream<'static, Result<NakadiFrame, IoError>>;
pub type BatchStream = BoxStream<'static, Result<BatchLine, BatchLineError>>;

new_type! {
    #[doc="If `true` abort the consumer when an auth error occurs while connecting to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectorAbortOnAuthError(bool, env="CONNECTOR_ABORT_ON_AUTH_ERROR");
}
impl Default for ConnectorAbortOnAuthError {
    fn default() -> Self {
        false.into()
    }
}
new_type! {
    #[doc="If `true` abort the consumer when a subscription does not exist when connection to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectorAbortOnSubscriptionNotFound(bool, env="CONNECTOR_ABORT_ON_SUBSCRIPTION_NOT_FOUND");
}
impl Default for ConnectorAbortOnSubscriptionNotFound {
    fn default() -> Self {
        true.into()
    }
}
new_type! {
    #[doc="The maximum time for connecting to a stream.\n\n\
    Default is infinite."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectorTimeoutSecs(u64, env="CONNECTOR_TIMEOUT_SECS");
}

new_type! {
    #[doc="The maximum retry delay between failed attempts to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectorMaxRetryDelaySecs(u64, env="CONNECTOR_MAX_RETRY_DELAY_SECS");
}
impl Default for ConnectorMaxRetryDelaySecs {
    fn default() -> Self {
        300.into()
    }
}
new_type! {
    #[doc="The timeout for a request made to Nakadi to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectorAttemptTimeoutSecs(u64, env="CONNECTOR_ATTEMPT_TIMEOUT_SECS");
}
impl Default for ConnectorAttemptTimeoutSecs {
    fn default() -> Self {
        10.into()
    }
}

/// The timeout for a request made to Nakadi to connect to a stream including retries
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConnectorTimeout {
    Infinite,
    Seconds(u64),
}

impl ConnectorTimeout {
    env_funs!("CONNECTOR_TIMEOUT");

    pub fn into_duration_opt(self) -> Option<Duration> {
        match self {
            ConnectorTimeout::Infinite => None,
            ConnectorTimeout::Seconds(secs) => Some(Duration::from_secs(secs)),
        }
    }
}

impl Default for ConnectorTimeout {
    fn default() -> Self {
        Self::Infinite
    }
}

impl<T> From<T> for ConnectorTimeout
where
    T: Into<u64>,
{
    fn from(v: T) -> Self {
        Self::Seconds(v.into())
    }
}

impl fmt::Display for ConnectorTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorTimeout::Infinite => write!(f, "infinite")?,
            ConnectorTimeout::Seconds(secs) => write!(f, "{}", secs)?,
        }

        Ok(())
    }
}

impl FromStr for ConnectorTimeout {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_after(s: &str) -> Result<ConnectorTimeout, Error> {
            let s = s.trim();

            if s.starts_with('{') {
                return Ok(serde_json::from_str(s)?);
            }

            match s {
                "infinite" => Ok(ConnectorTimeout::Infinite),
                x => {
                    let seconds: u64 = x.parse().map_err(|err| {
                        Error::new(format!("{} is not a connector timeout", s)).caused_by(err)
                    })?;
                    Ok(ConnectorTimeout::Seconds(seconds))
                }
            }
        }
    }
}

/// Parameters to configure the `Connector`
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub stream_params: Option<StreamParameters>,
    pub abort_on_auth_error: Option<ConnectorAbortOnAuthError>,
    /// If `true` abort the consumer when a subscription does not exist when connection to a stream.
    pub abort_connect_on_subscription_not_found: Option<ConnectorAbortOnSubscriptionNotFound>,
    /// The maximum time for until a connection to a stream has to be established.
    ///
    /// Default is to retry indefinitely
    pub timeout_secs: Option<ConnectorTimeout>,
    /// The maximum retry delay between failed attempts to connect to a stream.
    pub max_retry_delay_secs: Option<ConnectorMaxRetryDelaySecs>,
    /// The timeout for a request made to Nakadi to connect to a stream.
    pub attempt_timeout_secs: Option<ConnectorAttemptTimeoutSecs>,
}

impl ConnectorConfig {
    pub fn from_env() -> Result<Self, Error> {
        let mut me = Self::default();
        me.update_from_env()?;
        Ok(me)
    }

    pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, Error> {
        let mut me = Self::default();
        me.update_from_env_prefixed(prefix)?;
        Ok(me)
    }

    pub fn update_from_env(&mut self) -> Result<(), Error> {
        self.update_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    pub fn update_from_env_prefixed<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.timeout_millis.is_none() {
            self.stream_params(StreamParameters::from_env_prefixed(prefix)?);
        }
        if self.abort_on_auth_error.is_none() {
            self.abort_on_auth_error(ConnectorAbortOnAuthError::from_env_prefixed(prefix)?);
        }
        if self.abort_connect_on_subscription_not_found.is_none() {
            self.abort_connect_on_subscription_not_found(
                ConnectorAbortOnSubscriptionNotFound::from_env_prefixed(prefix)?,
            );
        }
        if self.timeout_secs.is_none() {
            self.timeout_secs(ConnectorTimeout::from_env_prefixed(prefix)?);
        }
        if self.max_retry_delay_secs.is_none() {
            self.max_retry_delay_secs(ConnectorMaxRetryDelaySecs::from_env_prefixed(prefix)?);
        }
        if self.attempt_timeout_secs.is_none() {
            self.attempt_timeout_secs(ConnectorAttemptTimeoutSecs::from_env_prefixed(prefix)?);
        }

        Ok(())
    }

    pub fn stream_params<T: Into<StreamParameters>>(mut self, v: T) -> Self {
        self.stream_params = Some(v.into());
        self
    }
    pub fn abort_on_auth_error<T: Into<ConnectorAbortOnAuthError>>(mut self, v: T) -> Self {
        self.abort_on_auth_error = Some(v.into());
        self
    }
    pub fn abort_connect_on_subscription_not_found<
        T: Into<ConnectorAbortOnSubscriptionNotFound>,
    >(
        mut self,
        v: T,
    ) -> Self {
        self.abort_connect_on_subscription_not_found = Some(v.into());
        self
    }
    pub fn timeout_secs<T: Into<ConnectorTimeout>>(mut self, v: T) -> Self {
        self.timeout_secs = Some(v.into());
        self
    }
    pub fn max_retry_delay_secs<T: Into<ConnectorMaxRetryDelaySecs>>(mut self, v: T) -> Self {
        self.max_retry_delay_secs = Some(v.into());
        self
    }
    pub fn attempt_timeout_secs<T: Into<ConnectorAttemptTimeoutSecs>>(mut self, v: T) -> Self {
        self.attempt_timeout_secs = Some(v.into());
        self
    }
}

/// A `Connector` to connect to a Nakadi Stream
///
/// ## `on_retry`
///
/// A closure to be called before a retry. The error which caused the retry and
/// the time until the retry will be made is passed. This closure overrides the current one
/// and will be used for all subsequent clones of this instance. This allows
/// users to give context on the call site.
pub struct Connector<C> {
    stream_client: C,
    config: ConnectorConfig,
    flow_id: Option<FlowId>,
    on_retry_callback: Arc<dyn Fn(&ConnectError, Duration) + Send + Sync + 'static>,
    instrumentation: Instrumentation,
}

impl<C> Connector<C>
where
    C: SubscriptionStreamApi + Send + Sync + 'static,
{
    pub fn new_with_config(stream_client: C, config: ConnectorConfig) -> Self {
        Self {
            stream_client,
            config,
            flow_id: None,
            on_retry_callback: Arc::new(|_, _| {}),
            instrumentation: Instrumentation::default(),
        }
    }

    pub fn new(stream_client: C) -> Self {
        Self {
            stream_client,
            config: ConnectorConfig::default(),
            flow_id: None,
            on_retry_callback: Arc::new(|_, _| {}),
            instrumentation: Instrumentation::default(),
        }
    }

    pub fn instrumentation(&self) -> Instrumentation {
        self.params.instrumentation.clone()
    }

    pub async fn connect(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<SubscriptionStreamChunks, ConnectError> {
        self.connect_abortable(subscription_id, || false).await
    }

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

    pub fn set_on_retry<F: Fn(&ConnectError, Duration) + Send + Sync + 'static>(
        &mut self,
        on_retry: F,
    ) {
        self.on_retry_callback = Arc::new(on_retry);
    }

    pub fn on_retry<F: Fn(&ConnectError, Duration) + Send + Sync + 'static>(
        mut self,
        on_retry: F,
    ) -> Self {
        self.set_on_retry(on_retry);
        self
    }

    fn set_flow_id(&mut self, flow_id: FlowId) {
        self.flow_id = Some(flow_id);
    }

    fn configure(mut self, config: ConnectorConfig) -> Self {
        self.set_config(config);
        self
    }

    fn set_config(&mut self, config: ConnectorConfig) {
        self.config = config;
    }

    fn set_instrumentation(&mut self, instrumentation: Instrumentation) {
        self.params.instrumentation = instrumentation;
    }

    fn stream_parameters_mut(&mut self) -> &mut StreamParameters {
        &mut self.config.stream_params
    }

    fn config(&self) -> &ConnectorConfig {
        &self.config
    }

    fn config_mut(&self) -> &mut ConnectorConfig {
        &mut self.config
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
            timeout(
                connect_timeout,
                self.connect_to_stream_with_retries(subscription_id, flow_id, check_abort),
            )
            .await?
        } else {
            self.connect_to_stream_with_retries(subscription_id, flow_id, check_abort)
                .await
        }
    }

    async fn connect_to_stream_with_retries<F>(
        &self,
        subscription_id: SubscriptionId,
        flow_id: FlowId,
        check_abort: F,
    ) -> Result<SubscriptionStreamChunks, ConnectError>
    where
        F: FnMut() -> bool + Send,
    {
        let instrumentation = self.instrumentation.clone();
        let stream_params = self.config.stream_params.unwrap_or_default();
        let abort_connect_on_subscription_not_found: bool = self
            .config()
            .abort_connect_on_subscription_not_found
            .unwrap_or_default()
            .into();
        let abort_on_auth_error: bool = self.config.abort_on_auth_error.unwrap_or_default().into();

        let mut backoff = Backoff::new(self.config.max_retry_delay_secs.unwrap_or_default().into());

        let connect_started_at = Instant::now();
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
                    };
                    if retry_allowed {
                        let delay = backoff.next();
                        (self.on_retry_callback)(&err, delay);
                        delay_for(delay).await;
                        continue;
                    } else {
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
        let stream_params = self.config.stream_params.unwrap_or_default();
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
                        "Connecting to Nakadi for a stream timed ot after {:?}.",
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
            ConnectErrorKind::ServerError => write!(f, "nakadi error")?,
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
