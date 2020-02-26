use std::time::Duration;

use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use futures::future::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::time::{delay_for, timeout};

use crate::api::{NakadiApiError, PublishApi, PublishFailure, PublishFuture};
use crate::nakadi_types::{
    model::{event_type::EventTypeName, publishing::BatchResponse},
    FlowId,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialFailureStrategy {
    Abort,
    RetryAll,
    RetryFailed,
}

impl Default for PartialFailureStrategy {
    fn default() -> Self {
        Self::Abort
    }
}

new_type! {
    #[doc="The time a publish attempt for the events batch may take.\n\n\
    Default is 1000 ms\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct PublishAttemptTimeoutMillis(u64, env="PUBLISH_ATTEMPT_TIMEOUT_MILLIS");
}

impl Default for PublishAttemptTimeoutMillis {
    fn default() -> Self {
        Self(1_000)
    }
}

new_type! {
    #[doc="The a publishing the events batch including retries may take.\n\n\
    Default is 5000 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct PublishTimeoutMillis(u64, env="PUBLISH_TIMEOUT_MILLIS");
}
impl Default for PublishTimeoutMillis {
    fn default() -> Self {
        Self(5_000)
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
    pub copy struct PublishRetryOnAuthErrors(bool, env="PUBLISH_RETRY_ON_AUTH_ERRORS");
}
impl Default for PublishRetryOnAuthErrors {
    fn default() -> Self {
        Self(false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_millis: Option<PublishTimeoutMillis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_timeout_millis: Option<PublishAttemptTimeoutMillis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_retry_interval_millis: Option<PublishInitialRetryIntervalMillis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_interval_multiplier: Option<PublishRetryIntervalMultiplier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retry_interval_millis: Option<PublishMaxRetryIntervalMillis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_on_auth_errors: Option<PublishRetryOnAuthErrors>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_failure_strategy: Option<PartialFailureStrategy>,
}

/// blah
///
/// ## `PublishApi`
///
/// The publisher implements `PublishApi`. If the trait method is used
/// for publishing no retries are done on partial successes. Retries are
/// only done on io errors and server errors or on auth errors if
/// `retry_on_auth_errors` is set to `true`.
pub struct Publisher<C> {
    config: Config,
    api_client: C,
}

impl<C> PublishApi for Publisher<C>
where
    C: PublishApi + Send + Sync + 'static,
{
    fn publish_events<'a, B: Into<Bytes>, T: Into<FlowId>>(
        &'a self,
        event_type: &'a EventTypeName,
        events: B,
        flow_id: T,
    ) -> PublishFuture<'a> {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(self.config.timeout_millis.unwrap_or_default().into());
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
        let retry_on_auth_errors = self.config.retry_on_auth_errors.unwrap_or_default().into();
        let bytes = events.into();
        let flow_id = flow_id.into();
        async move {
            loop {
                let publish_failure = match single_attempt(
                    &self.api_client,
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
                        let mut retry_allowed = false;
                        if api_error.is_io_error() || api_error.is_server_error() {
                            retry_allowed |= true;
                        }
                        if api_error.is_auth_error() && retry_on_auth_errors {
                            retry_allowed |= true;
                        }
                        if retry_allowed {
                            if let Some(delay) = backoff.next_backoff() {
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
    let attempt = api_client.publish_events(event_type, events.clone(), flow_id);
    timeout(attempt_timeout, attempt)
        .await
        .map_err(|elapsed| PublishFailure::Other(elapsed.into()))?
}

fn is_retry_on_api_error_allowed(error: &NakadiApiError) -> bool {
    true
}

fn is_retry_on_batch_response_allowed(batch_response: &BatchResponse) -> bool {
    true
}
