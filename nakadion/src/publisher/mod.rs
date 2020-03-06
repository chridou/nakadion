//! Publish events to Nakadi
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backoff::{backoff::Backoff, ExponentialBackoff};
pub use bytes::Bytes;
use futures::future::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::time::{delay_for, timeout};

pub use crate::api::{NakadiApiError, PublishApi, PublishFailure, PublishFuture};
pub use crate::nakadi_types::{
    Error, FlowId,
    {
        event_type::EventTypeName,
        publishing::{BatchResponse, BatchStats},
    },
};

use crate::logging::{DevNullLogger, Logger};
use crate::nakadi_types::publishing::PublishingStatus;

#[cfg(feature = "partitioner")]
pub mod partitioner;

mod instrumentation;
pub use instrumentation::*;

/// Strategy for handling partial submit failures
///
/// The default is `PartialFailureStrategy::Abort`
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialFailureStrategy {
    /// Always abort. Never retry on partial failures.
    Abort,
    /// Always retry all events
    RetryAll,
    /// Only retry those events where the publishing status is not `PublishingStatus::Submitted`
    RetryNotSubmitted,
}

impl PartialFailureStrategy {
    env_funs!("PUBLISH_PARTIAL_FAILURE_STRATEGY");
}

impl Default for PartialFailureStrategy {
    fn default() -> Self {
        Self::Abort
    }
}

impl fmt::Display for PartialFailureStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartialFailureStrategy::Abort => write!(f, "abort")?,
            PartialFailureStrategy::RetryAll => write!(f, "retry_all")?,
            PartialFailureStrategy::RetryNotSubmitted => write!(f, "retry_not_submitted")?,
        }

        Ok(())
    }
}

impl FromStr for PartialFailureStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.starts_with('\"') {
            return Ok(serde_json::from_str(s)?);
        }

        match s {
            "abort" => Ok(PartialFailureStrategy::Abort),
            "retry_all" => Ok(PartialFailureStrategy::RetryAll),
            "retry_not_submitted" => Ok(PartialFailureStrategy::RetryNotSubmitted),
            _ => Err(Error::new(format!(
                "not a valid partial failure strategy: {}",
                s
            ))),
        }
    }
}

new_type! {
    #[doc="The time a publish attempt for an events batch may take.\n\n\
    Default is 30 seconds\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct PublishAttemptTimeoutMillis(u64, env="PUBLISH_ATTEMPT_TIMEOUT_MILLIS");
}

impl Default for PublishAttemptTimeoutMillis {
    fn default() -> Self {
        Self(30_000)
    }
}

/// The timeout for a complete publishing of events to Nakadi including retries
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum PublishTimeout {
    Infinite,
    Millis(u64),
}

impl PublishTimeout {
    env_funs!("PUBLISH_TIMEOUT");

    pub fn into_duration_opt(self) -> Option<Duration> {
        match self {
            PublishTimeout::Infinite => None,
            PublishTimeout::Millis(millis) => Some(Duration::from_millis(millis)),
        }
    }
}

impl Default for PublishTimeout {
    fn default() -> Self {
        Self::Infinite
    }
}

impl<T> From<T> for PublishTimeout
where
    T: Into<u64>,
{
    fn from(v: T) -> Self {
        Self::Millis(v.into())
    }
}

impl fmt::Display for PublishTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PublishTimeout::Infinite => write!(f, "infinite")?,
            PublishTimeout::Millis(millis) => write!(f, "{} ms", millis)?,
        }

        Ok(())
    }
}

impl FromStr for PublishTimeout {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.starts_with('{') {
            return Ok(serde_json::from_str(s)?);
        }

        match s {
            "infinite" => Ok(PublishTimeout::Infinite),
            x => {
                let millis: u64 = x.parse().map_err(|err| {
                    Error::new(format!("{} is not a publish timeout: {}", s, err))
                })?;
                Ok(PublishTimeout::Millis(millis))
            }
        }
    }
}

new_type! {
    #[doc="The initial delay between retry attempts.\n\n\
    Default is 100 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct PublishInitialRetryIntervalMillis(u64, env="PUBLISH_RETRY_INITIAL_INTERVAL_MILLIS");
}
impl Default for PublishInitialRetryIntervalMillis {
    fn default() -> Self {
        Self(100)
    }
}
new_type! {
    #[doc="The multiplier for the delay increase between retries.\n\n\
    Default is 1.5 (+50%).\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
    pub copy struct PublishRetryIntervalMultiplier(f64, env="PUBLISH_RETRY_INTERVAL_MULTIPLIER");
}
impl Default for PublishRetryIntervalMultiplier {
    fn default() -> Self {
        Self(1.5)
    }
}
new_type! {
    #[doc="The maximum interval between retries.\n\n\
    Default is 1000 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct PublishMaxRetryIntervalMillis(u64, env="PUBLISH_MAX_RETRY_INTERVAL_MILLIS");
}
impl Default for PublishMaxRetryIntervalMillis {
    fn default() -> Self {
        Self(1000)
    }
}
new_type! {
    #[doc="If true, retries are done on auth errors.\n\n\
    Default is false.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct PublishRetryOnAuthError(bool, env="PUBLISH_RETRY_ON_AUTH_ERROR");
}
impl Default for PublishRetryOnAuthError {
    fn default() -> Self {
        Self(false)
    }
}

/// Configuration for a publisher
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct PublisherConfig {
    /// Timeout for a complete publishing including potential retries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<PublishTimeout>,
    /// Timeout for a single publish request with Nakadi
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_timeout_millis: Option<PublishAttemptTimeoutMillis>,
    /// Interval length before the first retry attempt
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_retry_interval_millis: Option<PublishInitialRetryIntervalMillis>,
    /// Multiplier for the length of of the next retry interval
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_interval_multiplier: Option<PublishRetryIntervalMultiplier>,
    /// Maximum length of an interval before a retry
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retry_interval_millis: Option<PublishMaxRetryIntervalMillis>,
    /// Retry on authentication/authorization errors if `true`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_on_auth_error: Option<PublishRetryOnAuthError>,
    /// Strategy for handling partial failures
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_failure_strategy: Option<PartialFailureStrategy>,
}

impl PublisherConfig {
    /// Creates a new `Config` from the environment where all the env vars
    /// are prefixed with `NAKADION_`.
    pub fn try_from_env() -> Result<Self, Error> {
        Self::try_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    /// Creates a new `Config` from the environment where all the env vars
    /// are prefixed with `<prefix>_`.
    pub fn try_from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, Error> {
        let mut me = Self::default();
        me.fill_from_env_prefixed(prefix)?;
        Ok(me)
    }

    /// Sets all values that have not been set so far from the environment.
    ///
    /// All the env vars are prefixed with `NAKADION_`.
    pub fn fill_from_env(&mut self) -> Result<(), Error> {
        self.fill_from_env_prefixed(crate::helpers::NAKADION_PREFIX)
    }

    /// Sets all values that have not been set so far from the environment.
    ///
    /// All the env vars are prefixed with `<prefix>_`.
    pub fn fill_from_env_prefixed<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.timeout.is_none() {
            self.timeout = PublishTimeout::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.attempt_timeout_millis.is_none() {
            self.attempt_timeout_millis =
                PublishAttemptTimeoutMillis::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.initial_retry_interval_millis.is_none() {
            self.initial_retry_interval_millis =
                PublishInitialRetryIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.retry_interval_multiplier.is_none() {
            self.retry_interval_multiplier =
                PublishRetryIntervalMultiplier::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.max_retry_interval_millis.is_none() {
            self.max_retry_interval_millis =
                PublishMaxRetryIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }

        if self.partial_failure_strategy.is_none() {
            self.partial_failure_strategy =
                PartialFailureStrategy::try_from_env_prefixed(prefix.as_ref())?;
        }

        Ok(())
    }

    /// Timeout for a complete publishing including potential retries
    pub fn timeout<T: Into<PublishTimeout>>(mut self, v: T) -> Self {
        self.timeout = Some(v.into());
        self
    }
    /// Timeout for a single publish request with Nakadi
    pub fn attempt_timeout_millis<T: Into<PublishAttemptTimeoutMillis>>(mut self, v: T) -> Self {
        self.attempt_timeout_millis = Some(v.into());
        self
    }
    /// Interval length before the first retry attempt
    pub fn initial_retry_interval_millis<T: Into<PublishInitialRetryIntervalMillis>>(
        mut self,
        v: T,
    ) -> Self {
        self.initial_retry_interval_millis = Some(v.into());
        self
    }
    /// Multiplier for the length of of the next retry interval
    pub fn retry_interval_multiplier<T: Into<PublishRetryIntervalMultiplier>>(
        mut self,
        v: T,
    ) -> Self {
        self.retry_interval_multiplier = Some(v.into());
        self
    }
    /// Maximum length of an interval before a retry
    pub fn max_retry_interval_millis<T: Into<PublishMaxRetryIntervalMillis>>(
        mut self,
        v: T,
    ) -> Self {
        self.max_retry_interval_millis = Some(v.into());
        self
    }
    /// Retry on authentication/authorization errors if `true`
    pub fn retry_on_auth_error<T: Into<PublishRetryOnAuthError>>(mut self, v: T) -> Self {
        self.retry_on_auth_error = Some(v.into());
        self
    }
    /// Strategy for handling partial failures
    pub fn partial_failure_strategy<T: Into<PartialFailureStrategy>>(mut self, v: T) -> Self {
        self.partial_failure_strategy = Some(v.into());
        self
    }
}

/// Publishes events that have been serialized before
///
/// This trait can be made a trait object
pub trait PublishesSerializedEvents {
    /// Publishes the serialized events.
    fn publish_serialized_events<'a>(
        &'a self,
        event_type: &'a EventTypeName,
        events: &[Bytes],
        flow_id: FlowId,
    ) -> PublishFuture<'a>;
}

/// Publish non serialized events.
///
/// This trait is implemented for all types which implement `PublishesSerializedEvents`.
pub trait PublishesEvents {
    fn publish_events<'a, E: Serialize + Sync, T: Into<FlowId>>(
        &'a self,
        event_type: &'a EventTypeName,
        events: &'a [E],
        flow_id: T,
    ) -> PublishFuture<'a>;
}

/// Publishes events with retries
///
/// ## `PublishApi`
///
/// The publisher implements `PublishApi`. If the trait method is used
/// for publishing no retries are done on partial successes. Retries are
/// only done on io errors and server errors or on auth errors if
/// `retry_on_auth_errors` is set to `true`.
///
#[derive(Clone)]
pub struct Publisher<C> {
    config: PublisherConfig,
    api_client: Arc<C>,
    logger: Arc<dyn Logger>,
    instrumentation: Instrumentation,
}

impl<C> Publisher<C>
where
    C: PublishApi + Send + Sync + 'static,
{
    pub fn new(api_client: C) -> Self {
        Self::with_config(api_client, PublisherConfig::default())
    }

    pub fn with_config(api_client: C, config: PublisherConfig) -> Self {
        Self {
            config,
            api_client: Arc::new(api_client),
            logger: Arc::new(DevNullLogger),
            instrumentation: Default::default(),
        }
    }

    pub fn set_logger<L: Logger>(&mut self, logger: L) {
        self.logger = Arc::new(logger);
    }

    pub fn logger<L: Logger>(mut self, logger: L) -> Self {
        self.set_logger(logger);
        self
    }

    pub fn instrumentation(mut self, instr: Instrumentation) -> Self {
        self.set_instrumentation(instr);
        self
    }

    pub fn set_instrumentation(&mut self, instr: Instrumentation) {
        self.instrumentation = instr;
    }
}

impl<C> PublishesSerializedEvents for Publisher<C>
where
    C: PublishApi + Send + Sync + 'static,
{
    fn publish_serialized_events<'a>(
        &'a self,
        event_type: &'a EventTypeName,
        events: &[Bytes],
        flow_id: FlowId,
    ) -> PublishFuture<'a> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = self.config.timeout.unwrap_or_default().into_duration_opt();
        backoff.max_interval = self
            .config
            .max_retry_interval_millis
            .unwrap_or_default()
            .into();
        backoff.multiplier = self
            .config
            .retry_interval_multiplier
            .unwrap_or_default()
            .into();
        backoff.initial_interval = self
            .config
            .initial_retry_interval_millis
            .unwrap_or_default()
            .into();
        let attempt_timeout = self
            .config
            .attempt_timeout_millis
            .unwrap_or_default()
            .into_duration();
        let retry_on_auth_errors = self.config.retry_on_auth_error.unwrap_or_default().into();

        let strategy = self.config.partial_failure_strategy.unwrap_or_default();

        let mut bytes_to_publish = assemble_bytes_to_publish(events);
        let mut events: Vec<Bytes> = events.to_vec();
        let started = Instant::now();
        async move {
            let api_client = Arc::clone(&self.api_client);
            let api_client: &C = &api_client;
            loop {
                let publish_failure = match single_attempt(
                    api_client,
                    event_type,
                    bytes_to_publish.clone(),
                    flow_id.clone(),
                    attempt_timeout,
                )
                .await
                {
                    Ok(()) => {
                        self.instrumentation.published(started.elapsed());
                        self.instrumentation
                            .batch_stats(BatchStats::all_submitted(events.len()));
                        break Ok(());
                    }
                    Err(publish_failure) => publish_failure,
                };

                match publish_failure {
                    PublishFailure::Other(api_error) => {
                        self.instrumentation
                            .batch_stats(BatchStats::all_not_submitted(events.len()));

                        let retry_allowed =
                            is_retry_on_api_error_allowed(&api_error, retry_on_auth_errors);
                        if retry_allowed {
                            if let Some(delay) = backoff.next_backoff() {
                                self.logger.warn(format_args!(
                                    "publish attempt failed (retry in {:?}: {}",
                                    delay, api_error
                                ));
                                delay_for(delay).await;
                                continue;
                            } else {
                                self.instrumentation.publish_failed(started.elapsed());
                                break Err(api_error.into());
                            }
                        } else {
                            self.instrumentation.publish_failed(started.elapsed());
                            break Err(api_error.into());
                        }
                    }
                    PublishFailure::Unprocessable(batch_response) => {
                        self.instrumentation.batch_stats(batch_response.stats());
                        self.instrumentation.publish_failed(started.elapsed());
                        break Err(PublishFailure::Unprocessable(batch_response));
                    }
                    PublishFailure::PartialFailure(batch_response) => {
                        self.instrumentation.batch_stats(batch_response.stats());
                        if let Some(delay) = backoff.next_backoff() {
                            match get_events_for_retry(&batch_response, &events, strategy) {
                                Ok(Some(to_retry)) => {
                                    if to_retry.is_empty() {
                                        break Err(PublishFailure::PartialFailure(batch_response));
                                    }

                                    events = to_retry;
                                    bytes_to_publish = assemble_bytes_to_publish(&events);

                                    delay_for(delay).await;
                                    continue;
                                }
                                Ok(None) => {
                                    self.instrumentation.publish_failed(started.elapsed());
                                    break Err(PublishFailure::PartialFailure(batch_response));
                                }
                                Err(_err) => {
                                    self.instrumentation.publish_failed(started.elapsed());
                                    break Err(PublishFailure::PartialFailure(batch_response));
                                }
                            }
                        } else {
                            self.instrumentation.publish_failed(started.elapsed());
                            break Err(PublishFailure::PartialFailure(batch_response));
                        }
                    }
                }
            }
        }
        .boxed()
    }
}

impl<T> PublishesEvents for T
where
    T: PublishesSerializedEvents + Send + Sync,
{
    fn publish_events<'a, E: Serialize + Sync, F: Into<FlowId>>(
        &'a self,
        event_type: &'a EventTypeName,
        events: &'a [E],
        flow_id: F,
    ) -> PublishFuture<'a> {
        let flow_id = flow_id.into();
        async move {
            let mut serialized_events = Vec::new();
            for e in events {
                let serialized = serde_json::to_vec(e).map_err(|err| {
                    PublishFailure::Other(
                        NakadiApiError::other()
                            .with_context("Could not serialize event to publish")
                            .caused_by(err),
                    )
                })?;
                serialized_events.push(serialized.into());
            }

            self.publish_serialized_events(event_type, &serialized_events, flow_id)
                .await
        }
        .boxed()
    }
}

impl<C> PublishApi for Publisher<C>
where
    C: PublishApi + Send + Sync + 'static,
{
    fn publish_events_batch<'a, B: Into<Bytes>, T: Into<FlowId>>(
        &'a self,
        event_type: &'a EventTypeName,
        events: B,
        flow_id: T,
    ) -> PublishFuture<'a> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = self.config.timeout.unwrap_or_default().into_duration_opt();
        backoff.max_interval = self
            .config
            .max_retry_interval_millis
            .unwrap_or_default()
            .into();
        backoff.multiplier = self
            .config
            .retry_interval_multiplier
            .unwrap_or_default()
            .into();
        backoff.initial_interval = self
            .config
            .initial_retry_interval_millis
            .unwrap_or_default()
            .into();
        let attempt_timeout = self
            .config
            .attempt_timeout_millis
            .unwrap_or_default()
            .into_duration();
        let retry_on_auth_errors = self.config.retry_on_auth_error.unwrap_or_default().into();
        let bytes = events.into();
        let flow_id = flow_id.into();
        async move {
            let api_client = Arc::clone(&self.api_client);
            let api_client: &C = &api_client;
            loop {
                let publish_failure = match single_attempt(
                    api_client,
                    event_type,
                    bytes.clone(),
                    flow_id.clone(),
                    attempt_timeout,
                )
                .await
                {
                    Ok(()) => break Ok(()),
                    Err(publish_failure) => publish_failure,
                };

                match publish_failure {
                    PublishFailure::Other(api_error) => {
                        let retry_allowed =
                            is_retry_on_api_error_allowed(&api_error, retry_on_auth_errors);
                        if retry_allowed {
                            if let Some(delay) = backoff.next_backoff() {
                                self.logger.warn(format_args!(
                                    "connect publish failed (retry in {:?}: {}",
                                    delay, api_error
                                ));
                                delay_for(delay).await;
                                continue;
                            } else {
                                break Err(api_error.into());
                            }
                        } else {
                            break Err(api_error.into());
                        }
                    }
                    x => break Err(x),
                }
            }
        }
        .boxed()
    }
}

async fn single_attempt<C>(
    api_client: &C,
    event_type: &EventTypeName,
    events: Bytes,
    flow_id: FlowId,
    attempt_timeout: Duration,
) -> Result<(), PublishFailure>
where
    C: PublishApi + Send + 'static,
{
    let attempt = api_client.publish_events_batch(event_type, events.clone(), flow_id);
    timeout(attempt_timeout, attempt)
        .await
        .map_err(|elapsed| PublishFailure::Other(elapsed.into()))?
}

fn is_retry_on_api_error_allowed(api_error: &NakadiApiError, retry_on_auth_errors: bool) -> bool {
    if api_error.is_io_error() || api_error.is_server_error() {
        true
    } else {
        api_error.is_auth_error() && retry_on_auth_errors
    }
}

fn get_events_for_retry(
    batch_response: &BatchResponse,
    events: &[Bytes],
    strategy: PartialFailureStrategy,
) -> Result<Option<Vec<Bytes>>, Error> {
    match strategy {
        PartialFailureStrategy::Abort => Ok(None),
        PartialFailureStrategy::RetryNotSubmitted => {
            if events.len() != batch_response.len() {
                return Err(Error::new(
                    "The number of events did not match the number of batch response items",
                ));
            }

            let mut to_retry = Vec::new();
            for (batch_rsp, event_bytes) in batch_response.batch_items.iter().zip(events.iter()) {
                if batch_rsp.publishing_status != PublishingStatus::Submitted {
                    to_retry.push(event_bytes.clone());
                }
            }
            Ok(Some(to_retry))
        }
        PartialFailureStrategy::RetryAll => Ok(Some(events.to_vec())),
    }
}

fn assemble_bytes_to_publish(events: &[Bytes]) -> Bytes {
    let mut size = events.iter().map(|b| b.len()).sum();
    if events.is_empty() || size == 0 {
        return Bytes::default();
    }
    size += (events.len() - 1) + 2; // commas plus outer braces
    let mut buffer = Vec::with_capacity(size);
    buffer.push(b'[');
    let last_idx = events.len() - 1;
    for (i, event) in events.iter().enumerate() {
        buffer.extend_from_slice(event);
        if i != last_idx {
            buffer.push(b',');
        }
    }

    buffer.push(b']');
    buffer.into()
}
