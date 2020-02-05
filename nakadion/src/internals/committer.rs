use std::time::Instant;

use crate::nakadi_types::model::subscription::SubscriptionCursor;

pub struct CommitData {
    pub cursor: SubscriptionCursor,
    pub received_at: Instant,
    pub batch_id: usize,
    pub events_hint: Option<usize>,
}
