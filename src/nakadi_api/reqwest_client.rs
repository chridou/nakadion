use std::sync::Arc;

use reqwest::{Client, Url, RequestBuilder};

use crate::auth::{ProvidesAccessToken, TokenError};

use super::*;

#[derive(Clone)]
pub struct ReqwestNakadiApiClient {
    client: Client,
    urls: Arc<Urls>,
    token_provider: Arc<dyn ProvidesAccessToken + Send + Sync + 'static>
}

impl ReqwestNakadiApiClient {
    pub fn new<P>(client: Client, base_url: Url, token_provider: P) -> Self where P: ProvidesAccessToken + Send + Sync + 'static{
        Self {
            client,
            urls: Arc::new(Urls::new(base_url)),
            token_provider: Arc::new(token_provider)
        }
    }

    fn enrich_with_token(&self, b: RequestBuilder) -> Result<RequestBuilder, TokenError> {
        if let Some(token) = self.token_provider.get_token()? {
            Ok(b.bearer_auth(token))
        } else {
            Ok(b)
        }
    }
}

impl MonitoringApi for ReqwestNakadiApiClient {
    fn get_cursor_distances<T: Into<FlowId>>(
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: T,
    ) -> NakadiApiResult<CursorDistanceResult> {
        unimplemented!()
    }

    fn get_cursor_lag<T: Into<FlowId>>(
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: T,
    ) -> NakadiApiResult<CursorLagResult> {
        unimplemented!()
    }
}

impl SchemaRegistryApi for ReqwestNakadiApiClient {
    fn list_event_types<T: Into<FlowId>>(flow_id: T) -> NakadiApiResult<Vec<EventType>> {
        unimplemented!()
    }

    fn create_event_type<T: Into<FlowId>>(
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()> {
        unimplemented!()
    }

    fn get_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        flow_id: T,
    ) -> NakadiApiResult<EventType> {
        unimplemented!()
    }

    fn update_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()> {
        unimplemented!()
    }

    fn delete_event_type<T: Into<FlowId>>(name: &EventTypeName, flow_id: T) -> NakadiApiResult<()> {
        unimplemented!()
    }
}

impl StreamApi for ReqwestNakadiApiClient {
    fn publish_raw_events<T: Into<FlowId>>(
        name: &EventTypeName,
        raw_events: &[u8],
        flow_id: T,
    ) -> Result<(), PublishError> {
        unimplemented!()
    }
}

impl SubscriptionApi for ReqwestNakadiApiClient {
    fn create_subscription<T: Into<FlowId>>(
        input: &SubscriptionInput,
        flow_id: T,
    ) -> NakadiApiResult<Subcription> {
        unimplemented!()
    }

    fn get_subscription<T: Into<FlowId>>(
        name: SubscriptionId,
        flow_id: T,
    ) -> NakadiApiResult<Subcription> {
        unimplemented!()
    }

    fn update_auth<T: Into<FlowId>>(
        name: &SubscriptionInput,
        flow_id: T,
    ) -> NakadiApiResult<Subcription> {
        unimplemented!()
    }

    fn delete_subscription<T: Into<FlowId>>(id: SubscriptionId, flow_id: T) -> NakadiApiResult<()> {
        unimplemented!()
    }

    fn get_committed_offsets<T: Into<FlowId>>(
        id: SubscriptionId,
        flow_id: T,
    ) -> NakadiApiResult<Vec<SubscriptionCursor>> {
        unimplemented!()
    }

    fn commit_cursors<T: Into<FlowId>>(
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> Result<Committed, CommitError> {
        unimplemented!()
    }

    fn reset_subscription_cursors<T: Into<FlowId>>(
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> Result<Committed, CommitError> {
        unimplemented!()
    }

    fn events<T: Into<FlowId>>(
        id: SubscriptionId,
        parameters: &StreamParameters,
        flow_id: T,
    ) -> Result<EventStream, ConnectError> {
        unimplemented!()
    }

    fn subscription_stats<T: Into<FlowId>>(
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: T,
    ) -> Result<Vec<SubscriptionEventTypeStats>, NakadiApiError> {
        unimplemented!()
    }
}

struct Urls {
    event_types: Url,
    subscriptions: Url,
}

impl Urls {
    pub fn new(base_url: Url) -> Self {
        Self {
            event_types: base_url.join("event-types").unwrap(),
            subscriptions: base_url.join("subscriptions").unwrap(),
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

    pub fn schema_registry_list_event_types(&self) -> &Url {
        &self.event_types
    }

    pub fn schema_registry_create_event_type(&self) -> &Url {
        &self.event_types
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

    pub fn stream_api_publish(&self, event_type: &EventTypeName) -> Url {
        self.event_types
            .join(event_type.as_ref())
            .unwrap()
            .join("events")
            .unwrap()
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
            .join(&id.to_string())
            .unwrap()
            .join("cursors")
            .unwrap()
    }

    pub fn subscriptions_get_commit_cursors(&self, id: SubscriptionId) -> Url {
        self.subscriptions
            .join(&id.to_string())
            .unwrap()
            .join("cursors")
            .unwrap()
    }

    pub fn subscriptions_reset_subscription_cursors(&self, id: SubscriptionId) -> Url {
        self.subscriptions
            .join(&id.to_string())
            .unwrap()
            .join("cursors")
            .unwrap()
    }

    pub fn subscriptions_events(&self, id: SubscriptionId) -> Url {
        self.subscriptions
            .join(&id.to_string())
            .unwrap()
            .join("events")
            .unwrap()
    }

    pub fn subscriptions_stats(&self, id: SubscriptionId) -> Url {
        self.subscriptions
            .join(&id.to_string())
            .unwrap()
            .join("stats")
            .unwrap()
    }
}
