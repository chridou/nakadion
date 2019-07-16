use serde::{Deserialize, Serialize};

use crate::model::{PartitionId};

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


