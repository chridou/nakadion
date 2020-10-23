use std::{
    collections::{hash_map::Entry, HashMap},
    time::{Duration, Instant},
};

use nakadi_types::subscription::{EventTypePartition, StreamCommitTimeoutSecs};

use crate::consumer::CommitStrategy;

use super::{CommitEntry, CommitItem, CommitTrigger};

pub struct PendingCursors {
    stream_commit_timeout: Duration,
    current_deadline: Option<Instant>,
    collected_events: usize,
    collected_cursors: usize,
    commit_strategy: CommitStrategy,
    pending: HashMap<EventTypePartition, CommitEntry>,
}

impl PendingCursors {
    pub fn new(
        commit_strategy: CommitStrategy,
        stream_commit_timeout_secs: StreamCommitTimeoutSecs,
    ) -> Self {
        let stream_commit_timeout = safe_commit_timeout(stream_commit_timeout_secs.into());
        Self {
            stream_commit_timeout,
            current_deadline: None,
            collected_events: 0,
            collected_cursors: 0,
            commit_strategy,
            pending: HashMap::new(),
        }
    }

    pub fn add(&mut self, item: CommitItem) {
        let key = item.etp();

        self.collected_cursors += 1;
        self.collected_events += item.n_events;

        let deadline = match self.commit_strategy {
            CommitStrategy::Immediately => calc_effective_deadline(
                self.current_deadline,
                Some(Duration::from_secs(0)),
                self.stream_commit_timeout,
                item.frame_started_at,
            ),
            CommitStrategy::LatestPossible => calc_effective_deadline(
                self.current_deadline,
                None,
                self.stream_commit_timeout,
                item.frame_started_at,
            ),
            CommitStrategy::After {
                seconds: Some(seconds),
                ..
            } => calc_effective_deadline(
                self.current_deadline,
                Some(Duration::from_secs(u64::from(seconds))),
                self.stream_commit_timeout,
                item.frame_started_at,
            ),
            CommitStrategy::After { seconds: None, .. } => calc_effective_deadline(
                self.current_deadline,
                None,
                self.stream_commit_timeout,
                item.frame_started_at,
            ),
        };

        self.current_deadline = Some(deadline);

        match self.pending.entry(key) {
            Entry::Vacant(e) => {
                e.insert(CommitEntry {
                    first_frame_started_at: item.frame_started_at,
                    first_frame_id: item.frame_id,
                    n_batches: 1,
                    n_events: item.n_events,
                    item_to_commit: item,
                });
            }
            Entry::Occupied(mut e) => {
                let e = e.get_mut();
                e.n_batches += 1;
                e.n_events += item.n_events;
                e.item_to_commit = item
            }
        }
    }

    pub fn commit_required(&self, now: Instant) -> Option<CommitTrigger> {
        if self.pending.is_empty() {
            return None;
        }

        if let Some(deadline) = self.current_deadline {
            if deadline <= now {
                return Some(CommitTrigger::Deadline {
                    n_batches: self.collected_cursors,
                    n_events: self.collected_events,
                });
            }
        }

        match self.commit_strategy {
            CommitStrategy::Immediately => Some(CommitTrigger::Deadline {
                n_batches: self.collected_cursors,
                n_events: self.collected_events,
            }),
            CommitStrategy::LatestPossible => None,
            CommitStrategy::After {
                cursors, events, ..
            } => {
                if let Some(events) = events {
                    if self.collected_events >= events as usize {
                        return Some(CommitTrigger::Events {
                            n_batches: self.collected_cursors,
                            n_events: self.collected_events,
                        });
                    }
                }
                if let Some(cursors) = cursors {
                    if self.collected_cursors >= cursors as usize {
                        return Some(CommitTrigger::Batches {
                            n_batches: self.collected_cursors,
                            n_events: self.collected_events,
                        });
                    }
                }
                None
            }
        }
    }

    pub fn drain_reset(&mut self) -> Vec<(EventTypePartition, CommitEntry)> {
        let items = self.pending.drain().collect();

        self.current_deadline = None;
        self.collected_events = 0;
        self.collected_cursors = 0;

        items
    }

    /// Creates a `CommitTrigger::Deadline` if there are any events to commit.
    pub fn create_deadline_trigger(&self) -> Option<CommitTrigger> {
        if self.collected_cursors > 0 || self.collected_events > 0 {
            Some(CommitTrigger::Deadline {
                n_batches: self.collected_cursors,
                n_events: self.collected_events,
            })
        } else {
            None
        }
    }
}

fn calc_effective_deadline(
    current_deadline: Option<Instant>,
    commit_after: Option<Duration>,
    stream_commit_timeout: Duration,
    frame_started_at: Instant,
) -> Instant {
    let deadline_for_cursor = if let Some(commit_after) = commit_after {
        frame_started_at + commit_after.min(stream_commit_timeout)
    } else {
        frame_started_at + stream_commit_timeout
    };

    if let Some(current_deadline) = current_deadline {
        deadline_for_cursor.min(current_deadline)
    } else {
        deadline_for_cursor
    }
}

pub fn safe_commit_timeout(secs: u32) -> Duration {
    if secs > 1 {
        Duration::from_secs(u64::from(secs - 1))
    } else {
        Duration::from_millis(500)
    }
}
