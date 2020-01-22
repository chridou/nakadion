//! Helpers for defining events
use chrono::offset::Utc;
use chrono::DateTime;
use uuid::Uuid;

use crate::nakadi::model::{FlowId, PartitionId};

/// Metadata sent with an outgoing event
///
/// See [Event Metadata](http://nakadi.io/manual.html#definition_EventMetadata)
#[derive(Debug, Clone, Serialize)]
pub struct OutgoingMetadata {
    pub eid: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub parent_eids: Vec<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<PartitionId>,
}

/// Metadata retrievable from an incoming event.
///
/// See [Event Metadata](http://nakadi.io/manual.html#definition_EventMetadata)
#[derive(Debug, Clone, Deserialize)]
pub struct IncomingMetadata {
    pub eid: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub version: String,
    #[serde(default)]
    pub parent_eids: Vec<Uuid>,
    pub partition: PartitionId,
    pub flow_id: FlowId,
}
