use std::collections::{hash_map::Entry, HashMap};
use std::time::{Duration, Instant};

use tokio::{
    spawn,
    sync::mpsc::{error::TryRecvError, unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::delay_for,
};

use crate::nakadi_types::{
    model::{
        event_type::EventTypeName,
        partition::PartitionId,
        subscription::{StreamId, SubscriptionCursor, SubscriptionId},
    },
    Error, FlowId,
};

use crate::api::{NakadiApiError, SubscriptionCommitApi};
use crate::consumer::ConsumerError;
use crate::internals::StreamState;

pub struct CommitData {
    pub cursor: SubscriptionCursor,
    pub received_at: Instant,
    pub batch_id: usize,
    pub events_hint: Option<usize>,
}

pub struct Committer<C> {
    api_client: C,
}

impl<C> Committer<C>
where
    C: SubscriptionCommitApi + Send + Sync + 'static,
{
    pub fn start(
        api_client: C,
        stream_state: StreamState,
    ) -> (UnboundedSender<CommitData>, JoinHandle<Result<(), Error>>) {
        let (tx, to_commit) = unbounded_channel();

        let join_handle = spawn(run_committer(
            to_commit,
            stream_state,
            api_client,
            Duration::from_secs(1),
        ));
        (tx, join_handle)
    }
}

struct PendingCursor {
    pub deadline: Instant,
    pub cursor: SubscriptionCursor,
}

type PendingCursors = HashMap<(PartitionId, EventTypeName), PendingCursor>;

async fn run_committer<C>(
    mut to_commit: UnboundedReceiver<CommitData>,
    stream_state: StreamState,
    api_client: C,
    commit_after: Duration,
) -> Result<(), Error>
where
    C: SubscriptionCommitApi + Send + 'static,
{
    stream_state
        .logger()
        .debug(format_args!("Committer starting"));

    let commit_timeout =
        safe_commit_timeout(stream_state.stream_params.effective_commit_timeout_secs());
    let commit_interval = Duration::from_millis(200);
    let mut pending = PendingCursors::new();
    let mut next_commit_attempt_at = Instant::now() + commit_interval;

    loop {
        if stream_state.cancellation_requested() {
            to_commit.close();
        }

        let cursor_added = match to_commit.try_recv() {
            Ok(next) => {
                add_cursor(&mut pending, next, commit_after, commit_timeout);
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

        let now = Instant::now();
        if next_commit_attempt_at <= now {
            pending = match commit_due_cursors(
                &api_client,
                stream_state.subscription_id(),
                stream_state.stream_id(),
                pending,
            )
            .await
            {
                Ok(pending) => {
                    next_commit_attempt_at = now + commit_interval;
                    pending
                }
                Err(err) => {
                    stream_state
                        .logger()
                        .warn(format_args!("Failed to commit cursors: {}", err));
                    stream_state.request_stream_cancellation();
                    return Err(err);
                }
            };
        }
        if !cursor_added {
            delay_for(Duration::from_millis(50)).await
        }
    }

    if !pending.is_empty() {
        // try to commit the rest
        let cursors: Vec<_> = pending
            .into_iter()
            .map(|(_, pending)| pending.cursor)
            .collect();

        match commit(
            &api_client,
            stream_state.subscription_id(),
            stream_state.stream_id(),
            &cursors,
            FlowId::default(),
        )
        .await
        {
            Ok(_) => {
                stream_state
                    .logger()
                    .debug(format_args!("Committed final cursors."));
            }
            Err(err) => {
                stream_state
                    .logger()
                    .warn(format_args!("Failed to commit final cursors: {}", err));
            }
        };
    }

    stream_state
        .logger()
        .debug(format_args!("Committer stopped"));

    Ok(())
}

fn add_cursor(
    pending: &mut PendingCursors,
    data: CommitData,
    commit_after: Duration,
    commit_timeout: Duration,
) {
    let key = (
        data.cursor.cursor.partition.clone(),
        data.cursor.event_type.clone(),
    );

    match pending.entry(key) {
        Entry::Vacant(e) => {
            let deadline = calc_effective_deadline(&data, commit_after, commit_timeout);
            e.insert(PendingCursor {
                cursor: data.cursor,
                deadline,
            });
        }
        Entry::Occupied(mut e) => e.get_mut().cursor = data.cursor,
    }
}

async fn commit_due_cursors<C>(
    client: &C,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    mut pending: PendingCursors,
) -> Result<PendingCursors, Error>
where
    C: SubscriptionCommitApi + Send + 'static,
{
    let mut due = Vec::new();
    let mut not_due = PendingCursors::new();

    let now = Instant::now();
    for (k, v) in pending.drain() {
        if v.deadline >= now {
            due.push((k, v));
        } else {
            not_due.insert(k, v);
        }
    }

    if due.is_empty() {
        return Ok(not_due);
    }

    let cursors: Vec<_> = due
        .iter()
        .map(|(_, pending)| pending.cursor.clone())
        .collect();

    if commit(
        client,
        subscription_id,
        stream_id,
        &cursors,
        FlowId::default(),
    )
    .await?
    {
        Ok(not_due)
    } else {
        due.into_iter().for_each(|(k, v)| {
            not_due.insert(k, v);
        });
        Ok(not_due)
    }
}

async fn commit<C>(
    client: &C,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    cursors: &[SubscriptionCursor],
    flow_id: FlowId,
) -> Result<bool, Error>
where
    C: SubscriptionCommitApi + Send + 'static,
{
    match client
        .commit_cursors(subscription_id, stream_id, cursors, flow_id)
        .await
    {
        Ok(results) => Ok(true),
        Err(err) => {
            if err.is_client_error() {
                Err(Error::new(err))
            } else {
                Ok(false)
            }
        }
    }
}

fn calc_effective_deadline(
    data: &CommitData,
    commit_after: Duration,
    commit_timeout: Duration,
) -> Instant {
    let now = Instant::now();
    let deadline_by_timeout = now + commit_timeout;
    let deadline_by_after = data.received_at + commit_after;

    std::cmp::min(deadline_by_after, deadline_by_timeout)
}

fn safe_commit_timeout(secs: u32) -> Duration {
    let effective: u64 = if secs > 0 { u64::from(secs - 1) } else { 0 };
    Duration::from_secs(effective)
}
