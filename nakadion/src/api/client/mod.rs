use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use http::{
    header::{HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
    Method, Request, Response,
};
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use url::Url;

use crate::helpers::NAKADION_PREFIX;
use crate::nakadi_types::{Error, FlowId, NakadiBaseUrl};

use crate::auth::{AccessTokenProvider, ProvidesAccessToken};

use super::dispatch_http_request::{DispatchHttpRequest, ResponseFuture};
use super::*;
use urls::Urls;

#[cfg(feature = "reqwest")]
use crate::api::dispatch_http_request::ReqwestDispatchHttpRequest;

mod get_subscriptions;
mod trait_impls;

const SCHEME_BEARER: &str = "Bearer";

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
    pub fn from_env() -> Result<Self, Error> {
        let mut me = Self::default();
        me.nakadi_base_url = NakadiBaseUrl::try_from_env()?;

        Ok(me)
    }
    pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, Error> {
        let mut me = Self::default();
        me.nakadi_base_url = NakadiBaseUrl::try_from_env_prefixed(prefix.as_ref())?;

        Ok(me)
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
        let timeout_millis = self.timeout_millis.unwrap_or_default();
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
        })
    }

    pub fn build_from_env_with_dispatcher<D>(
        self,
        dispatch_http_request: D,
    ) -> Result<ApiClient, Error>
    where
        D: DispatchHttpRequest + Send + Sync + 'static,
    {
        self.build_from_env_prefixed_with_dispatcher(NAKADION_PREFIX, dispatch_http_request)
    }

    pub fn build_from_env_prefixed_with_dispatcher<T, D>(
        mut self,
        prefix: T,
        dispatch_http_request: D,
    ) -> Result<ApiClient, Error>
    where
        T: AsRef<str>,
        D: DispatchHttpRequest + Send + Sync + 'static,
    {
        if self.nakadi_base_url.is_none() {
            self.nakadi_base_url = NakadiBaseUrl::try_from_env_prefixed(prefix.as_ref())?;
        }
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
/// The `ApiClient` does not do any retry or timeout management.
#[derive(Clone)]
pub struct ApiClient {
    inner: Arc<Inner>,
}

impl ApiClient {
    pub fn builder() -> Builder {
        Builder::default()
    }

    fn dispatch(&self, req: Request<Bytes>) -> ResponseFuture {
        self.inner.dispatch_http_request.dispatch(req)
    }

    pub(crate) async fn send_receive_payload<R: DeserializeOwned + 'static>(
        &self,
        url: Url,
        method: Method,
        payload: Vec<u8>,
        flow_id: FlowId,
    ) -> Result<R, NakadiApiError> {
        let mut request = self.create_request(&url, payload, flow_id).await?;
        *request.method_mut() = method;

        let response = self.dispatch(request).await?;

        if response.status().is_success() {
            let deserialized = deserialize_stream(response.into_body()).await?;
            Ok(deserialized)
        } else {
            evaluate_error_for_problem(response).map(Err).await
        }
    }

    pub(crate) fn get<'a, R: DeserializeOwned>(
        &'a self,
        url: Url,
        flow_id: FlowId,
    ) -> impl Future<Output = Result<R, NakadiApiError>> + Send + 'a {
        async move {
            let mut request = self.create_request(&url, Bytes::default(), flow_id).await?;
            *request.method_mut() = Method::GET;

            let response = self.dispatch(request).await?;

            if response.status().is_success() {
                let deserialized = deserialize_stream(response.into_body()).await?;
                Ok(deserialized)
            } else {
                evaluate_error_for_problem(response).map(Err).await
            }
        }
    }

    fn send_payload<'a>(
        &'a self,
        url: Url,
        method: Method,
        payload: Vec<u8>,
        flow_id: FlowId,
    ) -> impl Future<Output = Result<(), NakadiApiError>> + Send + 'a {
        async move {
            let mut request = self.create_request(&url, payload, flow_id).await?;
            *request.method_mut() = method;

            let response = self.dispatch(request).await?;

            if response.status().is_success() {
                Ok(())
            } else {
                evaluate_error_for_problem(response).map(Err).await
            }
        }
    }

    fn delete<'a>(
        &'a self,
        url: Url,
        flow_id: FlowId,
    ) -> impl Future<Output = Result<(), NakadiApiError>> + Send + 'a {
        async move {
            let mut request = self.create_request(&url, Bytes::default(), flow_id).await?;
            *request.method_mut() = Method::DELETE;

            let response = self.dispatch(request).await?;

            if response.status().is_success() {
                Ok(())
            } else {
                evaluate_error_for_problem(response).map(Err).await
            }
        }
    }

    async fn create_request<B: Into<Bytes>>(
        &self,
        url: &Url,
        body_bytes: B,
        flow_id: FlowId,
    ) -> Result<Request<Bytes>, NakadiApiError> {
        let body_bytes = body_bytes.into();
        let content_length = body_bytes.len();
        let mut request = Request::new(body_bytes);

        if let Some(token) = self.inner.access_token_provider.get_token().await? {
            request
                .headers_mut()
                .append(AUTHORIZATION, construct_authorization_bearer_value(token)?);
        }

        request.headers_mut().append(
            HeaderName::from_static("x-flow-id"),
            HeaderValue::from_str(flow_id.as_ref())?,
        );

        if content_length > 0 {
            request
                .headers_mut()
                .append(CONTENT_TYPE, HeaderValue::from_str("application/json")?);
        }

        request
            .headers_mut()
            .append(CONTENT_LENGTH, content_length.into());

        *request.uri_mut() = url.as_str().parse()?;

        Ok(request)
    }

    pub(crate) fn urls(&self) -> &Urls {
        &self.inner.urls
    }
}

fn construct_authorization_bearer_value<T: AsRef<str>>(
    token: T,
) -> Result<HeaderValue, NakadiApiError> {
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
                .take(20)
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
    timeout_millis: ApiClientTimeoutMillis,
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
