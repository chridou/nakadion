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

/// Shortcut for creating a new random `EventId`
pub struct Eid;

impl From<Eid> for EventId {
    fn from(_: Eid) -> Self {
        Self::random()
    }
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
    pub data: T,
    pub data_type: DataType,
    pub data_op: DataOp,
    pub metadata: EventMetaData,
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
    pub eid: EventId,
    /// The EventType of this Event
    pub event_type: EventTypeName,
    /// Timestamp of creation of the Event generated by the producer.
    pub occurred_at: DateTime<Utc>,
    /// Timestamp of the reception of the Event by Nakadi. This is enriched upon reception of
    /// the Event.
    pub received_at: DateTime<Utc>,
    #[serde(default)]
    /// Event identifier of the Event that caused the generation of this Event.
    /// Set by the producer.
    pub parent_eids: Vec<EventId>,
    /// Indicates the partition assigned to this Event.
    pub partition: PartitionId,
    #[serde(default)]
    pub version: String,
    pub flow_id: FlowId,
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
        pub data: T,
        pub data_type: DataType,
        pub data_op: DataOp,
        pub metadata: EventMetaDataPub,
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
        pub payload: T,
        pub metadata: EventMetaDataPub,
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
        pub eid: EventId,
        /// The EventType of this Event. This is enriched by Nakadi on reception of the Event
        /// based on the endpoint where the Producer sent the Event to.
        ///
        /// If provided MUST match the endpoint. Failure to do so will cause rejection of the
        /// Event.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub event_type: Option<EventTypeName>,
        /// Timestamp of creation of the Event generated by the producer.
        pub occurred_at: DateTime<Utc>,
        /// Event identifier of the Event that caused the generation of this Event.
        /// Set by the producer.
        #[serde(default)]
        pub parent_eids: Vec<EventId>,
        /// Indicates the partition assigned to this Event.
        ///
        /// Required to be set by the client if partition strategy of the EventType is
        /// ‘user_defined’.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub partition: Option<PartitionId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub flow_id: Option<FlowId>,
    }

    impl EventMetaDataPub {
        pub fn new<T: Into<EventId>>(eid: T) -> Self {
            Self {
                eid: eid.into(),
                event_type: None,
                occurred_at: Utc::now(),
                parent_eids: Vec::new(),
                partition: None,
                flow_id: None,
            }
        }

        pub fn event_type<T: Into<EventTypeName>>(mut self, v: T) -> Self {
            self.event_type = Some(v.into());
            self
        }

        pub fn occurred_at<T: Into<DateTime<Utc>>>(mut self, v: T) -> Self {
            self.occurred_at = v.into();
            self
        }

        pub fn parent_eid<T: Into<EventId>>(mut self, v: T) -> Self {
            self.parent_eids.push(v.into());
            self
        }

        pub fn partition<T: Into<PartitionId>>(mut self, v: T) -> Self {
            self.partition = Some(v.into());
            self
        }

        pub fn flow_id<T: Into<FlowId>>(mut self, v: T) -> Self {
            self.flow_id = Some(v.into());
            self
        }
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
