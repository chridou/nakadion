use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::Stream;
use http::StatusCode;
use http_api_problem::HttpApiProblem;
use serde::{de::DeserializeOwned, Serialize};
use url::Url;

use crate::event_stream::EventStream;
use crate::model::*;

use dispatch_http_request::RemoteCallError;

pub mod client;
pub mod dispatch_http_request;

struct ApiFuture<T> {
    inner: Box<dyn Future<Output = Result<T, NakadiApiError>> + Send>,
}

trait MonitoringApi {
    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
    fn get_cursor_distances(
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: FlowId,
    ) -> ApiFuture<CursorDistanceResult>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursors-lag_post)
    fn get_cursor_lag(
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: FlowId,
    ) -> ApiFuture<CursorLagResult>;
}

trait SchemaRegistryApi {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types(flow_id: FlowId) -> ApiFuture<Vec<EventType>>;

    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type(event_type: &EventType, flow_id: FlowId) -> ApiFuture<()>;

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type(name: &EventTypeName, flow_id: FlowId) -> ApiFuture<EventType>;

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type(
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: FlowId,
    ) -> ApiFuture<()>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type(name: &EventTypeName, flow_id: FlowId) -> ApiFuture<()>;
}

struct PublishFuture {
    inner: Box<dyn Future<Output = Result<(), PublishError>> + Send>,
}

trait PublishApi {
    /// Publishes a batch of Events of this EventType. All items must be of the EventType
    /// identified by name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
    fn publish_events<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        events: &[E],
        flow_id: FlowId,
    ) -> PublishFuture {
        unimplemented!()
    }

    fn publish_event<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        event: &E,
        flow_id: FlowId,
    ) -> PublishFuture {
        unimplemented!()
    }

    fn publish_raw_events(
        name: &EventTypeName,
        raw_events: &[u8],
        flow_id: FlowId,
    ) -> PublishFuture;
}

trait SubscriptionApi {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription(input: &SubscriptionInput, flow_id: FlowId) -> ApiFuture<Subscription>;

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription(name: SubscriptionId, flow_id: FlowId) -> ApiFuture<Subscription>;

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth(name: &SubscriptionInput, flow_id: FlowId) -> ApiFuture<Subscription>;

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription(id: SubscriptionId, flow_id: FlowId) -> ApiFuture<()>;

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_committed_offsets(
        id: SubscriptionId,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionCursor>>;

    fn subscription_stats(
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: FlowId,
    ) -> ApiFuture<Vec<SubscriptionEventTypeStats>>;

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors(
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: FlowId,
    ) -> ApiFuture<()>;
}

pub struct CommitFuture {
    inner: Box<dyn Future<Output = Result<Committed, CommitError>> + Send + 'static>,
}

pub trait CommitApi {
    /// Endpoint for committing offsets of the subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_post)
    fn commit_cursors(
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: FlowId,
    ) -> CommitFuture;
}

pub struct ConnectFuture {
    inner: Box<dyn Future<Output = Result<EventStream, ConnectError>> + Send + 'static>,
}

pub trait ConnectApi {
    /// Starts a new stream for reading events from this subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
    fn connect(id: SubscriptionId, parameters: &StreamParameters, flow_id: FlowId)
        -> ConnectFuture;
}

pub struct StreamParameters {
    partitions: Vec<Partition>,
    max_uncommitted_events: u32,
    batch_limit: u32,
    stream_limit: u32,
    batch_flush_timeout: u32,
    stream_timeout: u32,
    commit_timeout: u32,
}

pub enum Committed {
    AllCommitted,
    NotAllCommitted(Vec<CommitResult>),
}

type NakadiApiError = RemoteCallError;

pub struct PublishError;

pub struct CommitError;

pub struct ConnectError;

/*
struct Urls {
    event_types: Url,
    subscriptions: Url,
}

mod urls {
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
*/
