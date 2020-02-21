use std::collections::{hash_map::Entry, HashMap};
use std::time::{Duration, Instant};

use tokio::{
    spawn,
    sync::mpsc::{error::TryRecvError, unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::delay_for,
};

use crate::nakadi_types::{
    model::subscription::{EventTypePartition, SubscriptionCursor},
    Error, FlowId,
};

use crate::components::committer::ProvidesCommitter;

use crate::consumer::CommitStrategy;
use crate::internals::StreamState;
use crate::logging::Logs;

pub struct CommitData {
    pub cursor: SubscriptionCursor,
    pub received_at: Instant,
    pub frame_id: usize,
    pub n_events: Option<usize>,
}

impl CommitData {
    fn etp(&self) -> EventTypePartition {
        EventTypePartition::new(
            self.cursor.event_type.clone(),
            self.cursor.cursor.partition.clone(),
        )
    }
}

pub(crate) struct Committer;

impl Committer {
    pub fn start<C>(
        api_client: C,
        stream_state: StreamState,
    ) -> (UnboundedSender<CommitData>, JoinHandle<Result<(), Error>>)
    where
        C: ProvidesCommitter + Send + Sync + 'static,
    {
        let (tx, to_commit) = unbounded_channel();

        let join_handle = spawn(run_committer(to_commit, stream_state, api_client));
        (tx, join_handle)
    }
}

struct PendingCursors {
    stream_commit_timeout: Duration,
    current_deadline: Option<Instant>,
    collected_events: usize,
    collected_cursors: usize,
    commit_strategy: CommitStrategy,
    pending: HashMap<EventTypePartition, SubscriptionCursor>,
}

impl PendingCursors {
    pub fn new(stream_state: &StreamState) -> Self {
        let stream_commit_timeout = safe_commit_timeout(
            stream_state
                .config()
                .stream_parameters
                .effective_commit_timeout_secs(),
        );
        let commit_strategy = stream_state.config().commit_strategy;

        Self {
            stream_commit_timeout,
            current_deadline: None,
            collected_events: 0,
            collected_cursors: 0,
            commit_strategy,
            pending: HashMap::new(),
        }
    }

    pub fn add(&mut self, data: CommitData, now: Instant) {
        let key = data.etp();

        self.collected_cursors += 1;
        if let Some(n_events) = data.n_events {
            self.collected_events += n_events
        }

        let deadline = match self.commit_strategy {
            CommitStrategy::Immediately => calc_effective_deadline(
                self.current_deadline,
                Some(Duration::from_secs(0)),
                self.stream_commit_timeout,
                data.received_at,
                now,
            ),
            CommitStrategy::LatestPossible => calc_effective_deadline(
                self.current_deadline,
                None,
                self.stream_commit_timeout,
                data.received_at,
                now,
            ),
            CommitStrategy::After {
                seconds: Some(seconds),
                ..
            } => calc_effective_deadline(
                self.current_deadline,
                Some(Duration::from_secs(u64::from(seconds))),
                self.stream_commit_timeout,
                data.received_at,
                now,
            ),
            CommitStrategy::After { seconds: None, .. } => calc_effective_deadline(
                self.current_deadline,
                None,
                self.stream_commit_timeout,
                data.received_at,
                now,
            ),
        };

        self.current_deadline = Some(deadline);

        match self.pending.entry(key) {
            Entry::Vacant(e) => {
                e.insert(data.cursor);
            }
            Entry::Occupied(mut e) => *e.get_mut() = data.cursor,
        }
    }

    pub fn commit_required(&self, now: Instant) -> bool {
        if self.pending.is_empty() {
            return false;
        }

        if let Some(deadline) = self.current_deadline {
            if deadline <= now {
                return true;
            }
        }

        match self.commit_strategy {
            CommitStrategy::Immediately => true,
            CommitStrategy::LatestPossible => false,
            CommitStrategy::After {
                cursors, events, ..
            } => {
                if let Some(cursors) = cursors {
                    if self.collected_cursors >= cursors as usize {
                        return true;
                    }
                }
                if let Some(events) = events {
                    if self.collected_events >= events as usize {
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn reset(&mut self) {
        self.current_deadline = None;
        self.collected_events = 0;
        self.collected_cursors = 0;
        self.pending.clear();
    }

    pub fn cursors<'a>(&'a self) -> impl Iterator<Item = SubscriptionCursor> + 'a {
        self.pending.values().cloned()
    }

    pub fn into_cursors(self) -> impl Iterator<Item = SubscriptionCursor> {
        self.pending.into_iter().map(|(_, v)| v)
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }
}

async fn run_committer<C>(
    mut to_commit: UnboundedReceiver<CommitData>,
    stream_state: StreamState,
    provides_committer: C,
) -> Result<(), Error>
where
    C: ProvidesCommitter + Send + 'static,
{
    stream_state
        .logger()
        .debug(format_args!("Committer starting"));

    let config = stream_state.config().clone();

    let mut pending = PendingCursors::new(&stream_state);
    let delay_on_no_cursor = Duration::from_millis(50);
    let subscription_id = stream_state.subscription_id();
    let stream_id = stream_state.stream_id();

    let mut committer = provides_committer.committer(subscription_id, stream_id);
    committer.set_timeout(config.commit_attempt_timeout);

    let mut next_commit_earliest_at = Instant::now();
    loop {
        if stream_state.cancellation_requested() {
            to_commit.close();
        }

        let now = Instant::now();
        let cursor_received = match to_commit.try_recv() {
            Ok(next) => {
                pending.add(next, now);
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Closed) => {
                stream_state
                    .logger()
                    .debug(format_args!("Exiting committer. Channel closed."));
                break;
            }
        };

        if next_commit_earliest_at > now {
            continue;
        }

        if !pending.commit_required(now) {
            if !cursor_received {
                delay_for(delay_on_no_cursor).await;
            }
            continue;
        }

        let cursors: Vec<_> = pending.cursors().collect();
        committer.set_flow_id(FlowId::random());
        match committer.commit(&cursors).await {
            Ok(_) => {
                pending.reset();
            }
            Err(err) => {
                if err.is_recoverable() {
                    stream_state.logger().warn(format_args!(
                        "Failed to commit cursors (recoverable): {}",
                        err
                    ));
                    next_commit_earliest_at =
                        Instant::now() + config.commit_retry_delay.into_duration()
                } else {
                    stream_state.logger().error(format_args!(
                        "Failed to commit cursors (unrecoverable): {}",
                        err
                    ));
                    stream_state.request_stream_cancellation();
                    return Err(Error::from_error(err));
                }
            }
        };
    }

    if !pending.is_empty() {
        // try to commit the rest
        let cursors: Vec<_> = pending.into_cursors().collect();
        let n_to_commit = cursors.len();

        committer.set_flow_id(FlowId::random());
        match committer.commit(&cursors).await {
            Ok(_) => {
                stream_state.debug(format_args!("Committed {} final cursors.", n_to_commit));
            }
            Err(err) => {
                stream_state.warn(format_args!(
                    "Failed to commit {} final cursors: {}",
                    n_to_commit, err
                ));
            }
        };
    }

    stream_state.debug(format_args!("Committer stopped"));

    Ok(())
}

fn calc_effective_deadline(
    current_deadline: Option<Instant>,
    commit_after: Option<Duration>,
    stream_commit_timeout: Duration,
    cursor_received_at: Instant,
    now: Instant,
) -> Instant {
    let deadline_for_cursor = if let Some(commit_after) = commit_after {
        cursor_received_at + std::cmp::min(commit_after, stream_commit_timeout)
    } else {
        cursor_received_at + stream_commit_timeout
    };
    let deadline_for_cursor = if now >= deadline_for_cursor {
        now
    } else {
        deadline_for_cursor
    };
    if let Some(current_deadline) = current_deadline {
        std::cmp::min(deadline_for_cursor, current_deadline)
    } else {
        deadline_for_cursor
    }
}

fn safe_commit_timeout(secs: u32) -> Duration {
    if secs > 1 {
        Duration::from_secs(u64::from(secs - 1))
    } else {
        Duration::from_millis(100)
    }
}
