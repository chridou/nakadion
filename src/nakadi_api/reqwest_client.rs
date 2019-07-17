use reqwest::{Client, Url};

use super::*;

pub struct ReqwestNakadiApiClient {
    client: Client,
}

struct Urls {
    event_types: Url,
}

impl Urls {
    pub fn new(base_url: Url) -> Self {
        Self {
            event_types: base_url.join("event-types").unwrap(),
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
     /// Returns a list of all registered EventTypes
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get)
    fn list_event_types<T: Into<FlowId>>(flow_id: T) -> NakadiApiResult<Vec<EventType>> {
        unimplemented!()
    }

    /// Creates a new EventType.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_post)
    fn create_event_type<T: Into<FlowId>>(
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()> {
        unimplemented!()
    }

    /// Returns the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_get)
    fn get_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        flow_id: T,
    ) -> NakadiApiResult<EventType> {
        unimplemented!()
    }

    /// Updates the EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_put)
    fn update_event_type<T: Into<FlowId>>(
        name: &EventTypeName,
        event_type: &EventType,
        flow_id: T,
    ) -> NakadiApiResult<()> {
        unimplemented!()
    }

    /// Deletes an EventType identified by its name.
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name_delete)
    fn delete_event_type<T: Into<FlowId>>(name: &EventTypeName, flow_id: T) -> NakadiApiResult<()> {
        unimplemented!()
    }
}
