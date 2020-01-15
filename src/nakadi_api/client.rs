use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::{future, FutureExt, Stream, TryFutureExt, TryStreamExt};
use http::{
    header::{HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH},
    Error as HttpError, Method, Request, Response, StatusCode, Uri,
};
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use url::Url;

use crate::auth::{AccessToken, AccessTokenProvider, ProvidesAccessToken, TokenError};
pub use crate::env_vars::*;
use crate::model::*;
use must_env_parsed;

use super::dispatch_http_request::{BytesStream, DispatchHttpRequest, IoError};
use super::*;
use urls::Urls;

const SCHEME_BEARER: &str = "Bearer";

#[derive(Clone)]
pub struct ApiClient {
    inner: Arc<Inner>,
}

struct Inner {
    urls: Urls,
    http_client: Box<dyn DispatchHttpRequest + Send + Sync + 'static>,
    access_token_provider: Box<dyn ProvidesAccessToken + Send + Sync + 'static>,
}

impl ApiClient {
    pub fn new_from_env() -> Result<Self, Box<dyn Error + 'static>> {
        let nakadi_host: NakadiHost = must_env_parsed!(NAKADION_NAKADI_HOST_ENV_VAR)?;
        let access_token_provider = AccessTokenProvider::from_env()?;
        let http_client =
            crate::nakadi_api::dispatch_http_request::ReqwestDispatchHttpRequest::default();

        Ok(Self::with_dispatcher(
            nakadi_host,
            http_client,
            access_token_provider,
        ))
    }
}

impl ApiClient {
    pub fn with_dispatcher<D, P>(
        nakadi_host: NakadiHost,
        http_client: D,
        access_token_provider: P,
    ) -> Self
    where
        D: DispatchHttpRequest + Send + Sync + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(Inner {
                http_client: Box::new(http_client),
                access_token_provider: Box::new(access_token_provider),
                urls: Urls::new(nakadi_host.into_inner()),
            }),
        }
    }

    fn send_receive_payload<'a, P: Serialize, R: DeserializeOwned>(
        &'a self,
        url: Url,
        method: Method,
        payload: &P,
        flow_id: FlowId,
    ) -> impl Future<Output = Result<R, NakadiApiError>> + Send + 'a {
        let serialized = serde_json::to_vec(&payload).unwrap();

        async move {
            let mut request = self.create_request(&url, serialized, flow_id).await?;
            *request.method_mut() = method;

            let response = self.http_client().dispatch(request).await?;

            if response.status().is_success() {
                let deserialized = deserialize_stream(response.into_body()).await?;
                Ok(deserialized)
            } else {
                evaluate_error_for_problem(response).map(Err).await
            }
        }
    }

    fn get<'a, R: DeserializeOwned>(
        &'a self,
        url: Url,
        flow_id: FlowId,
    ) -> impl Future<Output = Result<R, NakadiApiError>> + Send + 'a {
        async move {
            let mut request = self.create_request(&url, Bytes::default(), flow_id).await?;
            *request.method_mut() = Method::GET;

            let response = self.http_client().dispatch(request).await?;

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

            let response = self.http_client().dispatch(request).await?;

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

            let response = self.http_client().dispatch(request).await?;

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

        request
            .headers_mut()
            .append(CONTENT_LENGTH, content_length.into());

        *request.uri_mut() = url.as_str().parse()?;

        Ok(request)
    }

    fn http_client(&self) -> &(dyn DispatchHttpRequest + Send + Sync + 'static) {
        &*self.inner.http_client
    }

    fn urls(&self) -> &Urls {
        &self.inner.urls
    }
}

impl MonitoringApi for ApiClient {
    fn get_cursor_distances(
        &self,
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: FlowId,
    ) -> ApiFuture<CursorDistanceResult> {
        self.send_receive_payload(
            self.urls().monitoring_cursor_distances(name),
            Method::POST,
            query,
            flow_id,
        )
        .boxed()
    }

    fn get_cursor_lag(
        &self,
        name: &EventTypeName,
        cursors: &Vec<Cursor>,
        flow_id: FlowId,
    ) -> ApiFuture<CursorLagResult> {
        self.send_receive_payload(
            self.urls().monitoring_cursor_lag(name),
            Method::POST,
            cursors,
            flow_id,
        )
        .boxed()
    }
}

impl SchemaRegistryApi for ApiClient {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types(&self, flow_id: FlowId) -> ApiFuture<Vec<EventType>> {
        self.get(self.urls().schema_registry_list_event_types(), flow_id)
            .boxed()
    }
    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type(&self, event_type: &EventType, flow_id: FlowId) -> ApiFuture<()> {
        self.send_receive_payload(
            self.urls().schema_registry_create_event_type(),
            Method::POST,
            event_type,
            flow_id,
        )
        .boxed()
    }

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type(&self, name: &EventTypeName, flow_id: FlowId) -> ApiFuture<EventType> {
        let url = self.urls().schema_registry_get_event_type(name);
        self.get(url, flow_id).boxed()
    }

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type(
        &self,
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: FlowId,
    ) -> ApiFuture<()> {
        self.send_payload(
            self.urls().schema_registry_update_event_type(name),
            Method::PUT,
            serde_json::to_vec(event_type).unwrap(),
            flow_id,
        )
        .boxed()
    }

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type(&self, name: &EventTypeName, flow_id: FlowId) -> ApiFuture<()> {
        self.delete(self.urls().schema_registry_delete_event_type(name), flow_id)
            .boxed()
    }
}

impl SubscriptionApi for ApiClient {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription(
        &self,
        input: &SubscriptionInput,
        flow_id: FlowId,
    ) -> ApiFuture<Subscription> {
        let url = self.urls().subscriptions_create_subscription();
        let serialized = serde_json::to_vec(input).unwrap();

        async move {
            let mut request = self
                .create_request(&url, serialized, flow_id.clone())
                .await?;
            *request.method_mut() = Method::POST;

            let response = self.http_client().dispatch(request).await?;

            let status = response.status();
            if status.is_success() {
                let deserialized: Subscription = deserialize_stream(response.into_body()).await?;
                if status == StatusCode::OK {
                    Ok(deserialized)
                } else {
                    self.get_subscription(deserialized.id, flow_id).await
                }
            } else {
                evaluate_error_for_problem(response).map(Err).await
            }
        }
        .boxed()
    }

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription(&self, id: SubscriptionId, flow_id: FlowId) -> ApiFuture<Subscription> {
        let url = self.urls().subscriptions_get_subscription(id);
        self.get(url, flow_id).boxed()
    }

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth(&self, input: &SubscriptionInput, flow_id: FlowId) -> ApiFuture<()> {
        let url = self.urls().subscriptions_update_auth(input.id);
        self.send_payload(
            url,
            Method::PUT,
            serde_json::to_vec(input).unwrap(),
            flow_id,
        )
        .boxed()
    }

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription(&self, id: SubscriptionId, flow_id: FlowId) -> ApiFuture<()> {
        let url = self.urls().subscriptions_delete_subscription(id);
        self.delete(url, flow_id).boxed()
    }

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_committed_offsets(
        &self,
        id: SubscriptionId,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionCursor>> {
        #[derive(Deserialize)]
        struct EntityWrapper {
            #[serde(default)]
            items: Vec<SubscriptionCursor>,
        };

        let url = self.urls().subscriptions_get_committed_offsets(id);
        self.get::<EntityWrapper>(url, flow_id)
            .map_ok(|wrapper| wrapper.items)
            .boxed()
    }

    fn get_subscription_stats(
        &self,
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionEventTypeStats>> {
        #[derive(Deserialize)]
        struct EntityWrapper {
            #[serde(default)]
            items: Vec<SubscriptionEventTypeStats>,
        };
        let url = self.urls().subscriptions_stats(id, show_time_lag);
        self.get::<EntityWrapper>(url, flow_id)
            .map_ok(|wrapper| wrapper.items)
            .boxed()
    }

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors(
        &self,
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: FlowId,
    ) -> ApiFuture<()> {
        #[derive(Serialize)]
        struct EntityWrapper<'b> {
            items: &'b [SubscriptionCursor],
        };
        let data = EntityWrapper { items: cursors };
        let url = self.urls().subscriptions_reset_subscription_cursors(id);
        self.send_payload(
            url,
            Method::PATCH,
            serde_json::to_vec(&data).unwrap(),
            flow_id,
        )
        .boxed()
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
    mut stream: BytesStream<'a>,
) -> Result<T, NakadiApiError> {
    let mut bytes = Vec::new();
    while let Some(next) = stream.try_next().await? {
        bytes.extend(next);
    }

    let deserialized = serde_json::from_slice(&bytes)?;

    Ok(deserialized)
}

async fn evaluate_error_for_problem<'a>(response: Response<BytesStream<'a>>) -> NakadiApiError {
    let (parts, body) = response.into_parts();
    let kind = match parts.status {
        StatusCode::NOT_FOUND => NakadiApiErrorKind::NotFound,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => NakadiApiErrorKind::AccessDenied,
        status => {
            if status.is_server_error() {
                NakadiApiErrorKind::ServerError
            } else if status.is_client_error() {
                NakadiApiErrorKind::BadRequest
            } else {
                NakadiApiErrorKind::Other
            }
        }
    };

    match deserialize_stream::<HttpApiProblem>(body).await {
        Ok(problem) => NakadiApiError::new(kind, problem.to_string()).with_payload(problem),
        Err(err) => NakadiApiError::new(kind, "failed to deserialize problem JSON from response")
            .caused_by(err),
    }
}

impl From<http::header::InvalidHeaderValue> for NakadiApiError {
    fn from(err: http::header::InvalidHeaderValue) -> Self {
        NakadiApiError::new(NakadiApiErrorKind::Other, "invalid header value").caused_by(err)
    }
}

impl From<http::uri::InvalidUri> for NakadiApiError {
    fn from(err: http::uri::InvalidUri) -> Self {
        NakadiApiError::new(NakadiApiErrorKind::Other, "invalid URI").caused_by(err)
    }
}

impl From<TokenError> for NakadiApiError {
    fn from(err: TokenError) -> Self {
        NakadiApiError::new(NakadiApiErrorKind::Other, "failed to get access token").caused_by(err)
    }
}

impl From<serde_json::error::Error> for NakadiApiError {
    fn from(err: serde_json::error::Error) -> Self {
        NakadiApiError::new(NakadiApiErrorKind::Other, "serialization failure").caused_by(err)
    }
}

impl From<RemoteCallError> for NakadiApiError {
    fn from(err: RemoteCallError) -> Self {
        let kind = if err.is_io() {
            NakadiApiErrorKind::Io
        } else {
            NakadiApiErrorKind::Other
        };

        let message = if let Some(msg) = err.message() {
            msg
        } else {
            "remote call error"
        };

        NakadiApiError::new(kind, message).caused_by(err)
    }
}

impl From<IoError> for NakadiApiError {
    fn from(err: IoError) -> Self {
        NakadiApiError::new(NakadiApiErrorKind::Io, err.0)
    }
}

mod urls {
    use crate::model::*;
    use url::Url;

    pub struct Urls {
        event_types: Url,
        subscriptions: Url,
    }

    impl Urls {
        pub fn new(base_url: Url) -> Self {
            Self {
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

        pub fn subscriptions_get_committed_offsets(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join("cursors/")
                .unwrap()
        }

        /*       pub fn subscriptions_get_commit_cursors(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&id.to_string())
                .unwrap()
                .join("cursors")
                .unwrap()
        }*/

        pub fn subscriptions_reset_subscription_cursors(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join("cursors")
                .unwrap()
        }

        pub fn subscriptions_events(&self, id: SubscriptionId) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join("events")
                .unwrap()
        }

        pub fn subscriptions_stats(&self, id: SubscriptionId, show_time_lag: bool) -> Url {
            self.subscriptions
                .join(&format!("{}/", id))
                .unwrap()
                .join(&format!("stats/?show_time_lag={}", show_time_lag))
                .unwrap()
        }
    }
}