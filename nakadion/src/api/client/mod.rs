use std::sync::Arc;
use std::time::{Duration, Instant};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use http::{
    header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
    Method, Request, Response,
};
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::{delay_for, timeout};
use url::Url;

use crate::helpers::NAKADION_PREFIX;
use crate::nakadi_types::{Error, FlowId, NakadiBaseUrl};

use crate::auth::{AccessTokenProvider, ProvidesAccessToken};

use super::dispatch_http_request::DispatchHttpRequest;
use super::*;
use urls::Urls;

#[cfg(feature = "reqwest")]
use crate::api::dispatch_http_request::ReqwestDispatchHttpRequest;

mod get_subscriptions;
mod trait_impls;

const SCHEME_BEARER: &str = "Bearer";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestMode {
    RetryAndTimeout,
    Timeout,
    Simple,
}

new_type! {
    #[doc="The time a request attempt to Nakadi may take.\n\n\
    Default is 1000 ms\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct ApiClientAttemptTimeoutMillis(u64, env="API_CLIENT_ATTEMPT_TIMEOUT_MILLIS");
}

impl Default for ApiClientAttemptTimeoutMillis {
    fn default() -> Self {
        Self(1_000)
    }
}

new_type! {
    #[doc="The a timeout for a complete request including retries.\n\n\
    Default is 5000 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct ApiClientTimeoutMillis(u64, env="API_CLIENT_TIMEOUT_MILLIS");
}
impl Default for ApiClientTimeoutMillis {
    fn default() -> Self {
        Self(5_000)
    }
}

new_type! {
    #[doc="The initial delay between retry attempts.\n\n\
    Default is 100 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct ApiClientInitialRetryIntervalMillis(u64, env="API_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS");
}
impl Default for ApiClientInitialRetryIntervalMillis {
    fn default() -> Self {
        Self(100)
    }
}
new_type! {
    #[doc="The multiplier for the delay increase between retries.\n\n\
    Default is 1.5 (+50%).\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
    pub copy struct ApiClientRetryIntervalMultiplier(f64, env="API_CLIENT_RETRY_INTERVAL_MULTIPLIER");
}
impl Default for ApiClientRetryIntervalMultiplier {
    fn default() -> Self {
        Self(1.5)
    }
}
new_type! {
    #[doc="The maximum interval between retries.\n\n\
    Default is 1000 ms.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct ApiClientMaxRetryIntervalMillis(u64, env="API_CLIENT_MAX_RETRY_INTERVAL_MILLIS");
}
impl Default for ApiClientMaxRetryIntervalMillis {
    fn default() -> Self {
        Self(1000)
    }
}
new_type! {
    #[doc="If true, retries are done on auth errors.\n\n\
    Default is false.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ApiClientRetryOnAuthErrors(bool, env="API_CLIENT_RETRY_ON_AUTH_ERRORS");
}
impl Default for ApiClientRetryOnAuthErrors {
    fn default() -> Self {
        Self(false)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Builder {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nakadi_base_url: Option<NakadiBaseUrl>,
    /// Timeout for a complete publishing including potential retries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_millis: Option<ApiClientTimeoutMillis>,
    /// Timeout for a single request attempt to Nakadi
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_timeout_millis: Option<ApiClientAttemptTimeoutMillis>,
    /// Interval length before the first retry attempt
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_retry_interval_millis: Option<ApiClientInitialRetryIntervalMillis>,
    /// Multiplier for the length of of the next retry interval
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_interval_multiplier: Option<ApiClientRetryIntervalMultiplier>,
    /// Maximum length of an interval before a retry
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retry_interval_millis: Option<ApiClientMaxRetryIntervalMillis>,
    /// Retry on authentication/authorization errors if `true`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_on_auth_errors: Option<ApiClientRetryOnAuthErrors>,
}

impl Builder {
    env_ctors!();
    fn fill_from_env_prefixed_internal<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), Error> {
        if self.nakadi_base_url.is_none() {
            self.nakadi_base_url = NakadiBaseUrl::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.timeout_millis.is_none() {
            self.timeout_millis = ApiClientTimeoutMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.attempt_timeout_millis.is_none() {
            self.attempt_timeout_millis =
                ApiClientAttemptTimeoutMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.initial_retry_interval_millis.is_none() {
            self.initial_retry_interval_millis =
                ApiClientInitialRetryIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.retry_interval_multiplier.is_none() {
            self.retry_interval_multiplier =
                ApiClientRetryIntervalMultiplier::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.max_retry_interval_millis.is_none() {
            self.max_retry_interval_millis =
                ApiClientMaxRetryIntervalMillis::try_from_env_prefixed(prefix.as_ref())?;
        }
        if self.retry_on_auth_errors.is_none() {
            self.retry_on_auth_errors =
                ApiClientRetryOnAuthErrors::try_from_env_prefixed(prefix.as_ref())?;
        }
        Ok(())
    }

    pub fn nakadi_base_url(mut self, url: NakadiBaseUrl) -> Self {
        self.nakadi_base_url = Some(url);
        self
    }

    /// Timeout for a complete request including potential retries
    pub fn timeout_millis<T: Into<ApiClientTimeoutMillis>>(mut self, v: T) -> Self {
        self.timeout_millis = Some(v.into());
        self
    }
    /// Timeout for a single attempt to get a response from Nakadi
    pub fn attempt_timeout_millis<T: Into<ApiClientAttemptTimeoutMillis>>(mut self, v: T) -> Self {
        self.attempt_timeout_millis = Some(v.into());
        self
    }
    /// Interval length before the first retry attempt
    pub fn initial_retry_interval_millis<T: Into<ApiClientInitialRetryIntervalMillis>>(
        mut self,
        v: T,
    ) -> Self {
        self.initial_retry_interval_millis = Some(v.into());
        self
    }
    /// Multiplier for the length of of the next retry interval
    pub fn retry_interval_multiplier<T: Into<ApiClientRetryIntervalMultiplier>>(
        mut self,
        v: T,
    ) -> Self {
        self.retry_interval_multiplier = Some(v.into());
        self
    }
    /// Maximum length of an interval before a retry
    pub fn max_retry_interval_millis<T: Into<ApiClientMaxRetryIntervalMillis>>(
        mut self,
        v: T,
    ) -> Self {
        self.max_retry_interval_millis = Some(v.into());
        self
    }
    /// Retry on authentication/authorization errors if `true`
    pub fn retry_on_auth_errors<T: Into<ApiClientRetryOnAuthErrors>>(mut self, v: T) -> Self {
        self.retry_on_auth_errors = Some(v.into());
        self
    }
}

impl Builder {
    /// Build an `ApiClient` from this `Builder`
    ///
    /// No environment variables are read
    pub fn build_with<D, P>(
        self,
        dispatch_http_request: D,
        access_token_provider: P,
    ) -> Result<ApiClient, Error>
    where
        D: DispatchHttpRequest + Send + Sync + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
    {
        let nakadi_base_url = crate::helpers::mandatory(self.nakadi_base_url, "nakadi_base_url")?;
        let timeout_millis = self.timeout_millis;
        let attempt_timeout_millis = self.attempt_timeout_millis.unwrap_or_default();
        let initial_retry_interval_millis = self.initial_retry_interval_millis.unwrap_or_default();
        let retry_interval_multiplier = self.retry_interval_multiplier.unwrap_or_default();
        let max_retry_interval_millis = self.max_retry_interval_millis.unwrap_or_default();
        let retry_on_auth_errors = self.retry_on_auth_errors.unwrap_or_default();

        Ok(ApiClient {
            inner: Arc::new(Inner {
                dispatch_http_request: Box::new(dispatch_http_request),
                access_token_provider: Box::new(access_token_provider),
                urls: Urls::new(nakadi_base_url.into_inner()),
                timeout_millis,
                attempt_timeout_millis,
                initial_retry_interval_millis,
                retry_interval_multiplier,
                max_retry_interval_millis,
                retry_on_auth_errors,
            }),
            on_retry_callback: Arc::new(|_, _| {}),
        })
    }

    /// Build an `ApiClient` from this `Builder`
    ///
    /// Unset fields of this `Builder` will be set from the environment
    /// including the settings for the `ProvidesAccessToken`
    ///
    /// Environment variables must be prefixed with `NAKADION`
    pub fn build_from_env_with_dispatcher<D>(
        self,
        dispatch_http_request: D,
    ) -> Result<ApiClient, Error>
    where
        D: DispatchHttpRequest + Send + Sync + 'static,
    {
        self.build_from_env_prefixed_with_dispatcher(NAKADION_PREFIX, dispatch_http_request)
    }

    /// Build an `ApiClient` from this `Builder`
    ///
    /// Unset fields of this `Builder` will be set from the environment
    /// including the settings for the `ProvidesAccessToken`
    pub fn build_from_env_prefixed_with_dispatcher<T, D>(
        mut self,
        prefix: T,
        dispatch_http_request: D,
    ) -> Result<ApiClient, Error>
    where
        T: AsRef<str>,
        D: DispatchHttpRequest + Send + Sync + 'static,
    {
        self.fill_from_env_prefixed_internal(prefix.as_ref())?;

        let access_token_provider = AccessTokenProvider::from_env_prefixed(prefix.as_ref())?;

        self.build_with(dispatch_http_request, access_token_provider)
    }
}

#[cfg(feature = "reqwest")]
impl Builder {
    pub fn finish<P>(self, access_token_provider: P) -> Result<ApiClient, Error>
    where
        P: ProvidesAccessToken + Send + Sync + 'static,
    {
        let dispatch_http_request = ReqwestDispatchHttpRequest::default();
        self.build_with(dispatch_http_request, access_token_provider)
    }

    pub fn finish_from_env(self) -> Result<ApiClient, Error> {
        self.finish_from_env_prefixed(NAKADION_PREFIX)
    }

    pub fn finish_from_env_prefixed<T>(self, prefix: T) -> Result<ApiClient, Error>
    where
        T: AsRef<str>,
    {
        let dispatch_http_request = ReqwestDispatchHttpRequest::default();
        self.build_from_env_prefixed_with_dispatcher(prefix, dispatch_http_request)
    }
}

/// A client to connect to the API of `Nakadi`.
///
/// The actual HTTP client is pluggable via the `DispatchHttpRequest` trait.
///
/// The `ApiClient` does retries with exponential backoff and jitter except
/// for the following trait methods:
///
/// * `SubscriptionApi::request_stream`
/// * `SubscriptionApi::commit_cursors`
/// * `PublishApi::publish_events_batch`
///
/// ## `on_retry`
///
/// A closure to be called before a retry. The error which caused the retry and
/// the time until the retry will be made is passed. This closure overrides the current one
/// and will be used for all subsequent clones of this instance. This allows
/// users to give context on the call site.
#[derive(Clone)]
pub struct ApiClient {
    inner: Arc<Inner>,
    on_retry_callback: Arc<dyn Fn(&NakadiApiError, Duration) + Send + Sync + 'static>,
}

impl ApiClient {
    #[deprecated(
        since = "0.28.12",
        note = "misleading name. builder is just the empty defaulte. use default_builder"
    )]
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Get a default builder
    ///
    /// There are also methods to get a `Builder` which is initialized from the
    /// environment:
    ///
    /// * `Builder::builder_from_env`
    /// * `Builder::builder_from_env_prefixed`
    pub fn default_builder() -> Builder {
        Builder::default()
    }

    /// Get a builde filled from the environment
    ///
    /// Values are filled from prefixed environment variables
    pub fn builder_from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Builder, Error> {
        let mut builder = Builder::default();
        builder.fill_from_env_prefixed_internal(prefix)?;
        Ok(builder)
    }

    /// Get a builde filled from the environment
    ///
    /// Values must be prefixed with `NAKADION`
    pub fn builder_from_env() -> Result<Builder, Error> {
        Self::builder_from_env_prefixed(NAKADION_PREFIX)
    }

    pub fn set_on_retry<F: Fn(&NakadiApiError, Duration) + Send + Sync + 'static>(
        &mut self,
        on_retry: F,
    ) {
        self.on_retry_callback = Arc::new(on_retry);
    }

    pub fn on_retry<F: Fn(&NakadiApiError, Duration) + Send + Sync + 'static>(
        mut self,
        on_retry: F,
    ) -> Self {
        self.set_on_retry(on_retry);
        self
    }

    pub(crate) async fn send_receive_payload<R: DeserializeOwned + 'static>(
        &self,
        url: Url,
        method: Method,
        payload: Bytes,
        mode: RequestMode,
        flow_id: FlowId,
    ) -> Result<R, NakadiApiError> {
        let response = self
            .request(&url, method, payload, HeaderMap::default(), mode, flow_id)
            .await?;

        deserialize_stream(response.into_body()).await
    }

    pub(crate) async fn get<R: DeserializeOwned>(
        &self,
        url: Url,
        mode: RequestMode,
        flow_id: FlowId,
    ) -> Result<R, NakadiApiError> {
        let response = self
            .request(
                &url,
                Method::GET,
                Bytes::default(),
                HeaderMap::default(),
                mode,
                flow_id,
            )
            .await?;

        deserialize_stream(response.into_body()).await
    }

    async fn send_payload(
        &self,
        url: Url,
        method: Method,
        payload: Bytes,
        mode: RequestMode,
        flow_id: FlowId,
    ) -> Result<(), NakadiApiError> {
        self.request(&url, method, payload, HeaderMap::default(), mode, flow_id)
            .map_ok(|_| ())
            .await
    }

    async fn delete(
        &self,
        url: Url,
        mode: RequestMode,
        flow_id: FlowId,
    ) -> Result<(), NakadiApiError> {
        self.request(
            &url,
            Method::DELETE,
            Bytes::default(),
            HeaderMap::default(),
            mode,
            flow_id,
        )
        .map_ok(|_| ())
        .await
    }

    async fn request(
        &self,
        url: &Url,
        method: Method,
        body_bytes: Bytes,
        headers: HeaderMap,
        mode: RequestMode,
        flow_id: FlowId,
    ) -> Result<Response<BytesStream>, NakadiApiError> {
        let attempt_timeout = self.inner.attempt_timeout_millis.into();
        if mode == RequestMode::RetryAndTimeout {
            let mut backoff = ExponentialBackoff::default();
            backoff.max_elapsed_time = self.inner.timeout_millis.map(|t| t.into());
            backoff.max_interval = self.inner.max_retry_interval_millis.into();
            backoff.multiplier = self.inner.retry_interval_multiplier.into();
            backoff.initial_interval = self.inner.initial_retry_interval_millis.into();
            let retry_on_auth_errors: bool = self.inner.retry_on_auth_errors.into();

            loop {
                let result: Result<Response<BytesStream>, RemoteCallError> = self
                    .remote(
                        url,
                        method.clone(),
                        body_bytes.clone(),
                        headers.clone(),
                        flow_id.clone(),
                        Some(attempt_timeout),
                    )
                    .await;

                let response = match result {
                    Ok(response) => response,
                    Err(err) => {
                        if err.is_io() {
                            if let Some(delay) = backoff.next_backoff() {
                                let as_api_error = err.into();
                                (self.on_retry_callback)(&as_api_error, delay);
                                delay_for(delay).await;
                                continue;
                            } else {
                                return Err(err.into());
                            }
                        } else {
                            return Err(err.into());
                        }
                    }
                };

                if response.status().is_success() {
                    return Ok(response);
                } else {
                    let api_error = evaluate_error_for_problem(response).await;

                    let retry = api_error.is_io_error()
                        || api_error.is_server_error()
                        || (api_error.is_auth_error() && retry_on_auth_errors);

                    if retry {
                        if let Some(delay) = backoff.next_backoff() {
                            (self.on_retry_callback)(&api_error, delay);
                            delay_for(delay).await;
                            continue;
                        } else {
                            return Err(api_error);
                        }
                    } else {
                        return Err(api_error);
                    }
                }
            }
        } else {
            let timeout = if mode == RequestMode::Timeout {
                Some(attempt_timeout)
            } else {
                None
            };
            let rsp = self
                .remote(url, method, body_bytes, headers, flow_id, timeout)
                .await?;

            if rsp.status().is_success() {
                Ok(rsp)
            } else {
                evaluate_error_for_problem(rsp).map(Err).await
            }
        }
    }

    async fn remote(
        &self,
        url: &Url,
        method: Method,
        body_bytes: Bytes,
        headers: HeaderMap,
        flow_id: FlowId,
        remote_call_timeout: Option<Duration>,
    ) -> Result<Response<BytesStream>, RemoteCallError> {
        let content_length = body_bytes.len();
        let mut request = Request::new(body_bytes);

        *request.method_mut() = method;

        if let Some(token) = self
            .inner
            .access_token_provider
            .get_token()
            .await
            .map_err(|err| {
                RemoteCallError::new_other()
                    .with_message("failed to get a token")
                    .with_cause(err)
            })?
        {
            request
                .headers_mut()
                .append(AUTHORIZATION, construct_authorization_bearer_value(token)?);
        }

        request.headers_mut().append(
            HeaderName::from_static("x-flow-id"),
            HeaderValue::from_str(flow_id.as_ref())?,
        );

        for (k, v) in headers.into_iter() {
            if let Some(k) = k {
                let _ = request.headers_mut().append(k, v);
            }
        }

        if content_length > 0 {
            request
                .headers_mut()
                .append(CONTENT_TYPE, HeaderValue::from_str("application/json")?);
        }

        request
            .headers_mut()
            .append(CONTENT_LENGTH, content_length.into());

        *request.uri_mut() = url.as_str().parse()?;

        if let Some(remote_call_timeout) = remote_call_timeout {
            let started = Instant::now();
            match timeout(
                remote_call_timeout,
                self.inner.dispatch_http_request.dispatch(request),
            )
            .await
            {
                Ok(r) => r,
                Err(elapsed) => Err(RemoteCallError::new_io()
                    .with_message(format!(
                        "call to Nakadi timed out after {:?}",
                        started.elapsed()
                    ))
                    .with_cause(elapsed)),
            }
        } else {
            self.inner.dispatch_http_request.dispatch(request).await
        }
    }

    pub(crate) fn urls(&self) -> &Urls {
        &self.inner.urls
    }
}

fn construct_authorization_bearer_value<T: AsRef<str>>(
    token: T,
) -> Result<HeaderValue, RemoteCallError> {
    let value_string = format!("{} {}", SCHEME_BEARER, token.as_ref());
    let value = HeaderValue::from_str(&value_string)?;
    Ok(value)
}

async fn deserialize_stream<'a, T: DeserializeOwned>(
    mut stream: BytesStream,
) -> Result<T, NakadiApiError> {
    let mut bytes = Vec::new();
    while let Some(next) = stream.try_next().await? {
        bytes.extend(next);
    }

    match serde_json::from_slice(&bytes) {
        Ok(deserialized) => Ok(deserialized),
        Err(err) => {
            let not_deserialized: String = std::str::from_utf8(&bytes)
                .map_err(|err| {
                    NakadiApiError::other()
                        .with_context("Response to deserialize was not UTF-8")
                        .caused_by(err)
                })?
                .chars()
                .take(50)
                .collect();
            let message = format!("Could not deserialize '{}' (maybe more)", not_deserialized);
            Err(NakadiApiError::other().with_context(message).caused_by(err))
        }
    }
}

async fn evaluate_error_for_problem(response: Response<BytesStream>) -> NakadiApiError {
    let (parts, body) = response.into_parts();

    let flow_id = match parts.headers.get("x-flow-id") {
        Some(header_value) => {
            let header_bytes = header_value.as_bytes();
            let header_str = String::from_utf8_lossy(header_bytes);
            Some(FlowId::new(header_str))
        }
        None => None,
    };

    let err = match deserialize_stream::<HttpApiProblem>(body).await {
        Ok(problem) => NakadiApiError::http_problem(problem),
        Err(err) => NakadiApiError::http(parts.status)
            .with_context("There was an error parsing the response into a problem")
            .caused_by(err),
    };

    err.with_maybe_flow_id(flow_id)
}

impl fmt::Debug for ApiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApiClient({:?})", self.inner)?;
        Ok(())
    }
}

struct Inner {
    urls: Urls,
    dispatch_http_request: Box<dyn DispatchHttpRequest + Send + Sync + 'static>,
    access_token_provider: Box<dyn ProvidesAccessToken + Send + Sync + 'static>,
    timeout_millis: Option<ApiClientTimeoutMillis>,
    attempt_timeout_millis: ApiClientAttemptTimeoutMillis,
    initial_retry_interval_millis: ApiClientInitialRetryIntervalMillis,
    retry_interval_multiplier: ApiClientRetryIntervalMultiplier,
    max_retry_interval_millis: ApiClientMaxRetryIntervalMillis,
    retry_on_auth_errors: ApiClientRetryOnAuthErrors,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.urls.base_url)?;
        Ok(())
    }
}

mod urls {
    use url::Url;

    use nakadi_types::event_type::EventTypeName;
    use nakadi_types::subscription::SubscriptionId;

    pub struct Urls {
        pub base_url: Url,
        event_types: Url,
        subscriptions: Url,
    }

    impl Urls {
        pub fn new(base_url: Url) -> Self {
            Self {
                base_url: base_url.clone(),
                event_types: base_url.join("event-types/").unwrap(),
                subscriptions: base_url.join("subscriptions/").unwrap(),
            }
        }

        pub fn monitoring_cursor_distances(&self, event_type: &EventTypeName) -> Url {
            self.event_types
                .join(event_type.as_ref())
                .unwrap()
                .join("cursor-distances")
                .unwrap()
        }

        pub fn monitoring_cursor_lag(&self, event_type: &EventTypeName) -> Url {
            self.event_types
                .join(event_type.as_ref())
                .unwrap()
                .join("cursor-lag")
                .unwrap()
        }

        pub fn monitoring_event_type_partitions(&self, event_type: &EventTypeName) -> Url {
            self.event_types
                .join(event_type.as_ref())
                .unwrap()
                .join(&format!("{}/", event_type))
                .unwrap()
                .join("partitions")
                .unwrap()
        }

        pub fn schema_registry_list_event_types(&self) -> Url {
            self.event_types.clone()
        }

        pub fn schema_registry_create_event_type(&self) -> Url {
            self.event_types.clone()
        }

        pub fn schema_registry_get_event_type(&self, event_type: &EventTypeName) -> Url {
            self.event_types.join(event_type.as_ref()).unwrap()
        }

        pub fn schema_registry_update_event_type(&self, event_type: &EventTypeName) -> Url {
            self.event_types.join(event_type.as_ref()).unwrap()
        }

        pub fn schema_registry_delete_event_type(&self, event_type: &EventTypeName) -> Url {
            self.event_types.join(event_type.as_ref()).unwrap()
        }

        pub fn subscriptions_create_subscription(&self) -> &Url {
            &self.subscriptions
        }

        pub fn subscriptions_delete_subscription(&self, id: SubscriptionId) -> Url {
            self.subscriptions.join(&id.to_string()).unwrap()
        }

        pub fn subscriptions_get_subscription(&self, id: SubscriptionId) -> Url {
            self.subscriptions.join(&id.to_string()).unwrap()
        }

        pub fn subscriptions_update_auth(&self, id: SubscriptionId) -> Url {
            self.subscriptions.join(&id.to_string()).unwrap()
        }

        pub fn subscriptions_list_subscriptions(&self) -> Url {
            self.subscriptions.clone()
        }

        pub fn subscriptions_get_committed_offsets(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join("cursors/")
                .unwrap()
        }

        pub fn subscriptions_reset_subscription_cursors(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join("cursors")
                .unwrap()
        }

        pub fn subscriptions_stats(&self, id: SubscriptionId, show_time_lag: bool) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join(&format!("stats/?show_time_lag={}", show_time_lag))
                .unwrap()
        }

        pub fn subscriptions_commit_cursors(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join("cursors")
                .unwrap()
        }

        pub fn subscriptions_request_stream(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join("events")
                .unwrap()
        }

        pub fn publish_events(&self, event_type: &EventTypeName) -> Url {
            self.event_types
                .join(&format!("{}/", event_type))
                .unwrap()
                .join("events")
                .unwrap()
        }
    }
}
