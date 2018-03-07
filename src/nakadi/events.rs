use chrono::DateTime;
use chrono::offset::Utc;
use uuid::Uuid;

use nakadi::model::{FlowId, PartitionId};

#[derive(Debug, Clone, Serialize)]
pub struct OutgoingMetadata {
    pub eid: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")] pub event_type: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub parent_eids: Vec<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")] pub partition: Option<PartitionId>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IncomingMetadata {
    pub eid: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub version: String,
    pub parent_eids: Vec<Uuid>,
    pub partition: PartitionId,
    pub flow_id: FlowId,
}
