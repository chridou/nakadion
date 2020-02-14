use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use http::{
    header::{HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
    Method, Request, Response, StatusCode,
};
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use url::Url;

use crate::helpers::NAKADION_PREFIX;
use crate::nakadi_types::model::{event_type::*, partition::*, publishing::*, subscription::*};
use crate::nakadi_types::{Error, FlowId, NakadiBaseUrl};

use crate::auth::{AccessTokenProvider, ProvidesAccessToken};

use super::dispatch_http_request::{DispatchHttpRequest, ResponseFuture};
use super::*;
use urls::Urls;

#[cfg(feature = "reqwest")]
use crate::api::dispatch_http_request::ReqwestDispatchHttpRequest;

const SCHEME_BEARER: &str = "Bearer";

#[derive(Debug, Default)]
#[non_exhaustive]
pub struct Builder {
    pub nakadi_base_url: Option<NakadiBaseUrl>,
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
}

impl Builder {
    pub fn build_with<D, P>(
        mut self,
        dispatch_http_request: D,
        access_token_provider: P,
    ) -> Result<ApiClient, Error>
    where
        D: DispatchHttpRequest + Send + Sync + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
    {
        let nakadi_base_url = if let Some(base_url) = self.nakadi_base_url.take() {
            base_url
        } else {
            return Err(Error::new("'nakadi_base_url' is missing"));
        };

        Ok(ApiClient::new(
            nakadi_base_url,
            dispatch_http_request,
            access_token_provider,
        ))
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

#[derive(Clone)]
pub struct ApiClient {
    inner: Arc<Inner>,
}

impl ApiClient {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn new<D, P>(
        nakadi_base_url: NakadiBaseUrl,
        dispatch_http_request: D,
        access_token_provider: P,
    ) -> Self
    where
        D: DispatchHttpRequest + Send + Sync + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(Inner {
                dispatch_http_request: Box::new(dispatch_http_request),
                access_token_provider: Box::new(access_token_provider),
                urls: Urls::new(nakadi_base_url.into_inner()),
            }),
        }
    }

    fn dispatch(&self, req: Request<Bytes>) -> ResponseFuture {
        self.inner.dispatch_http_request.dispatch(req)
    }

    fn send_receive_payload<'a, R: DeserializeOwned>(
        &'a self,
        url: Url,
        method: Method,
        payload: Vec<u8>,
        flow_id: FlowId,
    ) -> impl Future<Output = Result<R, NakadiApiError>> + Send + 'a {
        async move {
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
    }

    fn get<'a, R: DeserializeOwned>(
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

    fn urls(&self) -> &Urls {
        &self.inner.urls
    }
}

impl fmt::Debug for ApiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApiClient({:?})", self.inner)?;
        Ok(())
    }
}

impl MonitoringApi for ApiClient {
    fn get_cursor_distances<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: T,
    ) -> ApiFuture<CursorDistanceResult> {
        let payload_to_send = serde_json::to_vec(query).unwrap();
        self.send_receive_payload(
            self.urls().monitoring_cursor_distances(name),
            Method::POST,
            payload_to_send,
            flow_id.into(),
        )
        .boxed()
    }

    fn get_cursor_lag<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: T,
    ) -> ApiFuture<Vec<Partition>> {
        let payload_to_send = serde_json::to_vec(cursors).unwrap();
        self.send_receive_payload(
            self.urls().monitoring_cursor_lag(name),
            Method::POST,
            payload_to_send,
            flow_id.into(),
        )
        .boxed()
    }

    fn get_event_type_partitions<T: Into<FlowId>>(
        &self,
        event_type: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<Vec<Partition>> {
        let url = self.urls().monitoring_event_type_partitions(event_type);
        self.get(url, flow_id.into()).boxed()
    }
}

impl SchemaRegistryApi for ApiClient {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types<T: Into<FlowId>>(&self, flow_id: FlowId) -> ApiFuture<Vec<EventType>> {
        self.get(self.urls().schema_registry_list_event_types(), flow_id)
            .boxed()
    }
    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type<T: Into<FlowId>>(
        &self,
        event_type: &EventType,
        flow_id: T,
    ) -> ApiFuture<()> {
        let payload_to_send = serde_json::to_vec(event_type).unwrap();
        self.send_receive_payload(
            self.urls().schema_registry_create_event_type(),
            Method::POST,
            payload_to_send,
            flow_id.into(),
        )
        .boxed()
    }

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<EventType> {
        let url = self.urls().schema_registry_get_event_type(name);
        self.get(url, flow_id.into()).boxed()
    }

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: T,
    ) -> ApiFuture<()> {
        self.send_payload(
            self.urls().schema_registry_update_event_type(name),
            Method::PUT,
            serde_json::to_vec(event_type).unwrap(),
            flow_id.into(),
        )
        .boxed()
    }

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type<T: Into<FlowId>>(
        &self,
        name: &EventTypeName,
        flow_id: T,
    ) -> ApiFuture<()> {
        self.delete(
            self.urls().schema_registry_delete_event_type(name),
            flow_id.into(),
        )
        .boxed()
    }
}

impl PublishApi for ApiClient {
    fn publish_events<E: Serialize, T: Into<FlowId>>(
        &self,
        event_type: &EventTypeName,
        events: &[E],
        flow_id: T,
    ) -> PublishFuture {
        let serialized = serde_json::to_vec(events).unwrap();
        let url = self.urls().publish_events(event_type);

        let flow_id = flow_id.into();
        async move {
            let mut request = self.create_request(&url, serialized, flow_id).await?;
            *request.method_mut() = Method::POST;

            let response = self.inner.dispatch_http_request.dispatch(request).await?;

            let flow_id = match response.headers().get("x-flow-id") {
                Some(header_value) => {
                    let header_bytes = header_value.as_bytes();
                    let header_str = String::from_utf8_lossy(header_bytes);
                    Some(FlowId::new(header_str))
                }
                None => None,
            };

            let status = response.status();
            match status {
                StatusCode::OK => Ok(()),
                StatusCode::MULTI_STATUS | StatusCode::UNPROCESSABLE_ENTITY => {
                    let batch_items = deserialize_stream(response.into_body()).await?;
                    if status == StatusCode::MULTI_STATUS {
                        Err(PublishFailure::PartialFailure(BatchResponse {
                            flow_id,
                            batch_items,
                        }))
                    } else {
                        Err(PublishFailure::Unprocessable(BatchResponse {
                            flow_id,
                            batch_items,
                        }))
                    }
                }
                _ => {
                    evaluate_error_for_problem(response)
                        .map(|err| Err(PublishFailure::Other(err)))
                        .await
                }
            }
        }
        .boxed()
    }
}

impl SubscriptionApi for ApiClient {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription<T: Into<FlowId>>(
        &self,
        input: &SubscriptionInput,
        flow_id: T,
    ) -> ApiFuture<Subscription> {
        if let Some(subscription_id) = input.id {
            return async move {
                Err(NakadiApiError::other().with_context(format!(
                    "to create a subscription `input` must not have a `SubscriptionId`(id={}) set",
                    subscription_id
                )))
            }
            .boxed();
        }
        let url = self.urls().subscriptions_create_subscription();
        let serialized = serde_json::to_vec(input).unwrap();

        let flow_id = flow_id.into();
        async move {
            let mut request = self
                .create_request(&url, serialized, flow_id.clone())
                .await?;
            *request.method_mut() = Method::POST;

            let response = self.dispatch(request).await?;

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
    fn get_subscription<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<Subscription> {
        let url = self.urls().subscriptions_get_subscription(id);
        self.get(url, flow_id.into()).boxed()
    }

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth<T: Into<FlowId>>(&self, input: &SubscriptionInput, flow_id: T) -> ApiFuture<()> {
        if let Some(id) = input.id {
            let url = self.urls().subscriptions_update_auth(id);
            self.send_payload(
                url,
                Method::PUT,
                serde_json::to_vec(input).unwrap(),
                flow_id.into(),
            )
            .boxed()
        } else {
            async {
                Err(NakadiApiError::other().with_context(
                    "to update the subscription `input` must have a `SubscriptionId`(id) set",
                ))
            }
            .boxed()
        }
    }

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<()> {
        let url = self.urls().subscriptions_delete_subscription(id);
        self.delete(url, flow_id.into()).boxed()
    }

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_committed_offsets<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        flow_id: T,
    ) -> ApiFuture<Vec<SubscriptionCursor>> {
        #[derive(Deserialize)]
        struct EntityWrapper {
            #[serde(default)]
            items: Vec<SubscriptionCursor>,
        };

        let url = self.urls().subscriptions_get_committed_offsets(id);
        self.get::<EntityWrapper>(url, flow_id.into())
            .map_ok(|wrapper| wrapper.items)
            .boxed()
    }

    fn get_subscription_stats<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: T,
    ) -> ApiFuture<Vec<SubscriptionEventTypeStats>> {
        #[derive(Deserialize)]
        struct EntityWrapper {
            #[serde(default)]
            items: Vec<SubscriptionEventTypeStats>,
        };
        let url = self.urls().subscriptions_stats(id, show_time_lag);
        self.get::<EntityWrapper>(url, flow_id.into())
            .map_ok(|wrapper| wrapper.items)
            .boxed()
    }

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
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
            flow_id.into(),
        )
        .boxed()
    }
}

impl SubscriptionCommitApi for ApiClient {
    fn commit_cursors<T: Into<FlowId>>(
        &self,
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> ApiFuture<CursorCommitResults> {
        #[derive(Serialize)]
        struct ItemsWrapper<'a> {
            items: &'a [SubscriptionCursor],
        };

        let wrapped = ItemsWrapper { items: cursors };

        let serialized = serde_json::to_vec(&wrapped).unwrap();

        let flow_id = flow_id.into();
        async move {
            let url = self.urls().subscriptions_commit_cursors(id);
            let mut request = self.create_request(&url, serialized, flow_id).await?;
            *request.method_mut() = Method::POST;

            request.headers_mut().append(
                HeaderName::from_static("x-nakadi-streamid"),
                HeaderValue::from_str(stream.to_string().as_ref())?,
            );

            let response = self.inner.dispatch_http_request.dispatch(request).await?;

            let status = response.status();
            match status {
                StatusCode::NO_CONTENT => Ok(CursorCommitResults::default()),
                StatusCode::OK => {
                    let commit_results = deserialize_stream(response.into_body()).await?;
                    Ok(CursorCommitResults { commit_results })
                }
                _ => evaluate_error_for_problem(response).map(Err).await,
            }
        }
        .boxed()
    }
}

impl SubscriptionStreamApi for ApiClient {
    fn request_stream<T: Into<FlowId>>(
        &self,
        subscription_id: SubscriptionId,
        parameters: &StreamParameters,
        flow_id: T,
    ) -> ApiFuture<SubscriptionStream> {
        let url = self.urls().subscriptions_request_stream(subscription_id);
        let parameters = serde_json::to_vec(parameters).unwrap();
        let flow_id = flow_id.into();
        async move {
            let mut request = self
                .create_request(&url, parameters, flow_id.clone())
                .await?;
            *request.method_mut() = Method::POST;

            let response = self.inner.dispatch_http_request.dispatch(request).await?;

            let status = response.status();
            if status == StatusCode::OK {
                match response.headers().get("x-nakadi-streamid") {
                    Some(header_value) => {
                        let header_bytes = header_value.as_bytes();
                        let header_str = std::str::from_utf8(header_bytes).map_err(|err| {
                            NakadiApiError::other().with_context(format!(
                                "the bytes of header 'x-nakadi-streamid' \
                                 were not a valid string: {}",
                                err
                            ))
                        })?;
                        let stream_id = header_str.parse().map_err(|err| {
                            NakadiApiError::other().with_context(format!(
                                "the value '{}' of header 'x-nakadi-streamid' \
                                 was not a valid stream id (UUID): {}",
                                header_str, err
                            ))
                        })?;
                        Ok(SubscriptionStream {
                            stream_id,
                            stream: response.into_body(),
                        })
                    }
                    None => {
                        return Err(NakadiApiError::other().with_context(
                            "response did not contain the 'x-nakadi-streamid' header",
                        ))
                    }
                }
            } else {
                evaluate_error_for_problem(response).map(Err).await
            }
        }
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

async fn deserialize_stream<T: DeserializeOwned>(
    mut stream: BytesStream,
) -> Result<T, NakadiApiError> {
    let mut bytes = Vec::new();
    while let Some(next) = stream.try_next().await? {
        bytes.extend(next);
    }

    let deserialized = serde_json::from_slice(&bytes)?;

    Ok(deserialized)
}

async fn evaluate_error_for_problem<'a>(response: Response<BytesStream>) -> NakadiApiError {
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

struct Inner {
    urls: Urls,
    dispatch_http_request: Box<dyn DispatchHttpRequest + Send + Sync + 'static>,
    access_token_provider: Box<dyn ProvidesAccessToken + Send + Sync + 'static>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.urls.base_url)?;
        Ok(())
    }
}

mod urls {
    use url::Url;

    use nakadi_types::model::event_type::EventTypeName;
    use nakadi_types::model::subscription::SubscriptionId;

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
