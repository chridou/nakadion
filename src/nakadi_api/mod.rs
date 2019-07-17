use serde::Serialize;

use crate::event_stream::EventStream;
use crate::model::*;

mod reqwest_client;

pub type NakadiApiResult<T> = Result<T, NakadiApiError>;

trait MonitoringApi {
    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
    fn get_cursor_distances<T: Into<FlowId>>(
        name: &EventTypeName,
        query: &CursorDistanceQuery,
        flow_id: T,
    ) -> NakadiApiResult<CursorDistanceResult>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursors-lag_post)
    fn get_cursor_lag<T: Into<FlowId>>(
        name: &EventTypeName,
        cursors: &[Cursor],
        flow_id: T,
    ) -> NakadiApiResult<CursorLagResult>;
}

trait SchemaRegistryApi {
    /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types<T: Into<FlowId>>(flow_id: T) -> NakadiApiResult<Vec<EventType>>;

    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type<T: Into<FlowId>>(
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()>;

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        flow_id: T,
    ) -> NakadiApiResult<EventType>;

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()>;

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type<T: Into<FlowId>>(name: &EventTypeName, flow_id: T) -> NakadiApiResult<()>;
}

trait StreamApi {
    /// Publishes a batch of Events of this EventType. All items must be of the EventType
    /// identified by name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/events_post)
    fn publish_events<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        events: &[E],
        flow_id: T,
    ) -> Result<(), PublishError> {
        unimplemented!()
    }

    fn publish_event<T: Into<FlowId>, E: Serialize>(
        name: &EventTypeName,
        event: &E,
        flow_id: T,
    ) -> Result<(), PublishError> {
        unimplemented!()
    }

    fn publish_raw_events<T: Into<FlowId>>(
        name: &EventTypeName,
        raw_events: &[u8],
        flow_id: T,
    ) -> Result<(), PublishError>;
}

trait SubscriptionApi {
    /// This endpoint creates a subscription for EventTypes.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions_post)
    fn create_subscription<T: Into<FlowId>>(
        input: &SubscriptionInput,
        flow_id: T,
    ) -> NakadiApiResult<Subcription>;

    /// Returns a subscription identified by id.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_get)
    fn get_subscription<T: Into<FlowId>>(
        name: SubscriptionId,
        flow_id: T,
    ) -> NakadiApiResult<Subcription>;

    /// This endpoint only allows to update the authorization section of a subscription.
    ///
    /// All other properties are immutable.
    /// This operation is restricted to subjects with administrative role.
    /// This call captures the timestamp of the update request.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_put)
    fn update_auth<T: Into<FlowId>>(
        name: &SubscriptionInput,
        flow_id: T,
    ) -> NakadiApiResult<Subcription>;

    /// Deletes a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id_delete)
    fn delete_subscription<T: Into<FlowId>>(id: SubscriptionId, flow_id: T) -> NakadiApiResult<()>;

    /// Exposes the currently committed offsets of a subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_get)
    fn get_committed_offsets<T: Into<FlowId>>(
        id: SubscriptionId,
        flow_id: T,
    ) -> NakadiApiResult<Vec<SubscriptionCursor>>;

    /// Endpoint for committing offsets of the subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_post)
    fn commit_cursors<T: Into<FlowId>>(
        id: SubscriptionId,
        stream: StreamId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> Result<Committed, CommitError>;

    /// Reset subscription offsets to specified values.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/cursors_patch)
    fn reset_subscription_cursors<T: Into<FlowId>>(
        id: SubscriptionId,
        cursors: &[SubscriptionCursor],
        flow_id: T,
    ) -> Result<Committed, CommitError>;

    /// Starts a new stream for reading events from this subscription.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
    fn events<T: Into<FlowId>>(
        id: SubscriptionId,
        parameters: &StreamParameters,
        flow_id: T,
    ) -> Result<EventStream, ConnectError>;

    fn subscription_stats<T: Into<FlowId>>(
        id: SubscriptionId,
        show_time_lag: bool,
        flow_id: T,
    ) -> Result<Vec<SubscriptionEventTypeStats>, NakadiApiError>;
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
    SomeAlreadyCommitted(Vec<CommitResult>),
}

pub struct NakadiApiError;

pub struct PublishError;

pub struct CommitError;

pub struct ConnectError;
