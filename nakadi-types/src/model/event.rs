//! Types for handling events
//!
//! Consumable and publishable event templates

pub use crate::{
    model::{event_type::EventTypeName, partition::PartitionId},
    FlowId,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

new_type! {
    #[doc="Identifier for an event.\n\nSometimes also called EID."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct EventId(Uuid);
}

impl EventId {
    pub fn random() -> Self {
        Self::new(Uuid::new_v4())
    }
}

new_type! {
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct DataType(String);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataOp {
    #[serde(rename = "C")]
    Creation,
    #[serde(rename = "U")]
    Update,
    #[serde(rename = "D")]
    Deletion,
    #[serde(rename = "S")]
    Snapshot,
}

/// A `DataChangeEvent` template for consumption of events
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_AuthorizationAttribute)
#[derive(Debug, Clone, Deserialize)]
pub struct DataChangeEvent<T> {
    #[serde(flatten)]
    data: T,
    data_type: DataType,
    data_op: DataOp,
    metadata: EventMetaData,
}

/// A `BusinessEvent` template for consumption of events
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_DataChangeEvent)
#[derive(Debug, Clone, Deserialize)]
pub struct BusinessEvent<T> {
    #[serde(flatten)]
    payload: T,
    metadata: EventMetaData,
}

/// Metadata of an event
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventMetadata)
#[derive(Debug, Clone, Deserialize)]
pub struct EventMetaData {
    /// Identifier of this Event.
    ///
    /// Clients MUST generate this value and it SHOULD be guaranteed to be unique from the
    /// perspective of the producer. Consumers MIGHT use this value to assert uniqueness of
    /// reception of the Event.
    eid: EventId,
    /// The EventType of this Event
    event_type: EventTypeName,
    /// Timestamp of creation of the Event generated by the producer.
    occurred_at: DateTime<Utc>,
    /// Timestamp of the reception of the Event by Nakadi. This is enriched upon reception of
    /// the Event.
    received_at: DateTime<Utc>,
    #[serde(default)]
    /// Event identifier of the Event that caused the generation of this Event.
    /// Set by the producer.
    parent_eids: Vec<EventId>,
    /// Indicates the partition assigned to this Event.
    partition: PartitionId,
    #[serde(default)]
    version: String,
    flow_id: FlowId,
}

pub mod publishable {
    //! Publishable events
    pub use crate::{
        model::{event_type::EventTypeName, partition::PartitionId},
        FlowId,
    };

    use chrono::{DateTime, Utc};
    use serde::Serialize;

    pub use super::{DataOp, DataType, EventId};

    /// A `DataChangeEvent` template for publishing of events
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_AuthorizationAttribute)
    #[derive(Debug, Clone, Serialize)]
    pub struct DataChangeEventPub<T> {
        #[serde(flatten)]
        data: T,
        data_type: DataType,
        data_op: DataOp,
        metadata: EventMetaDataPub,
    }

    impl<T> From<super::DataChangeEvent<T>> for DataChangeEventPub<T> {
        fn from(e: super::DataChangeEvent<T>) -> Self {
            Self {
                data: e.data,
                data_type: e.data_type,
                data_op: e.data_op,
                metadata: e.metadata.into(),
            }
        }
    }

    /// A `BusinessEvent` template for publishing of events
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_DataChangeEvent)
    #[derive(Debug, Clone, Serialize)]
    pub struct BusinessEventPub<T> {
        #[serde(flatten)]
        payload: T,
        metadata: EventMetaDataPub,
    }

    impl<T> From<super::BusinessEvent<T>> for BusinessEventPub<T> {
        fn from(e: super::BusinessEvent<T>) -> Self {
            Self {
                payload: e.payload,
                metadata: e.metadata.into(),
            }
        }
    }

    /// Metadata of an event
    ///
    /// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventMetadata)
    #[derive(Debug, Clone, Serialize)]
    pub struct EventMetaDataPub {
        /// Identifier of this Event.
        eid: EventId,
        /// The EventType of this Event. This is enriched by Nakadi on reception of the Event
        /// based on the endpoint where the Producer sent the Event to.
        ///
        /// If provided MUST match the endpoint. Failure to do so will cause rejection of the
        /// Event.
        event_type: Option<EventTypeName>,
        /// Timestamp of creation of the Event generated by the producer.
        occurred_at: DateTime<Utc>,
        /// Event identifier of the Event that caused the generation of this Event.
        /// Set by the producer.
        #[serde(default)]
        parent_eids: Vec<EventId>,
        /// Indicates the partition assigned to this Event.
        ///
        /// Required to be set by the client if partition strategy of the EventType is
        /// ‘user_defined’.
        partition: Option<PartitionId>,
        flow_id: Option<FlowId>,
    }

    impl From<super::EventMetaData> for EventMetaDataPub {
        fn from(m: super::EventMetaData) -> Self {
            Self {
                eid: m.eid,
                event_type: Some(m.event_type),
                occurred_at: m.occurred_at,
                parent_eids: m.parent_eids,
                partition: None,
                flow_id: Some(m.flow_id),
            }
        }
    }
}
