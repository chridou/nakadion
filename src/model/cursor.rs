use serde::{Deserialize, Serialize};

use crate::model::{EventTypeName, PartitionId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    pub partition: PartitionId,
    pub offset: CursorOffset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CursorOffset {
    Begin,
    N(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorDistanceQuery {
    pub initial_cursor: Cursor,
    pub final_cursor: Cursor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorDistanceResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorLagResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionCursorWithoutToken {
    pub partition: PartitionId,
    pub offset: CursorOffset,
    pub event_type: EventTypeName,
}

/// An opaque value defined by the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorToken(String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionCursor {
    pub partition: PartitionId,
    pub offset: CursorOffset,
    pub event_type: EventTypeName,
    pub cursor_token: CursorToken,
}
