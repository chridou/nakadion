use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use cancellation_token::*;
use failure::{Error, Fail};

use nakadi::api::{ApiClient, CommitError, CommitStatus};
use nakadi::batch::Batch;
use nakadi::metrics::MetricsCollector;
use nakadi::model::{FlowId, StreamId, SubscriptionId};
use nakadi::CommitStrategy;

const CURSOR_COMMIT_OFFSET: u64 = 55;

/// The `Committer` keeps track of the cursors
/// and commits them according to a
/// `CommitStrategy`.
///
/// This basically means the `Committer` receives all cursors
/// and most probably commits them delayed.
///
/// The `Committer` creates a background thread and works
/// asynchronously to the event processing.
#[derive(Clone)]
pub struct Committer {
    sender: mpsc::Sender<CommitterMessage>,
    stream_id: StreamId,
    lifecycle: Arc<CancellationTokenSource>,
    subscription_id: SubscriptionId,
    metrics_collector: Arc<dyn MetricsCollector + Send + Sync + 'static>,
}

enum CommitterMessage {
    Commit(Batch, Option<usize>),
}

impl Committer {
    /// Start a new `Committer`. The committer uses
    /// an `ApiClient` to commit cursors.
    pub fn start<C, M>(
        client: C,
        strategy: CommitStrategy,
        subscription_id: SubscriptionId,
        stream_id: StreamId,
        metrics_collector: M,
    ) -> Self
    where
        C: ApiClient + Send + 'static,
        M: MetricsCollector + Clone + Sync + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();

        let lifecycle = Arc::new(CancellationTokenSource::new(metrics_collector.clone()));

        start_commit_loop(
            receiver,
            strategy,
            subscription_id.clone(),
            stream_id.clone(),
            client,
            lifecycle.auto_token(),
            metrics_collector.clone(),
        );

        Committer {
            sender,
            stream_id,
            lifecycle,
            subscription_id,
            metrics_collector: Arc::new(metrics_collector),
        }
    }

    /// Schedule a batch to be committed. The batch contains the cursor
    /// and the `num_events_hint` is used to schedule commits based on
    /// certain `CommitStrategy`s
    pub fn request_commit(
        &self,
        batch: Batch,
        num_events_hint: Option<usize>,
    ) -> Result<(), Error> {
        self.sender
            .send(CommitterMessage::Commit(batch, num_events_hint))
            .map_err(|err| {
                self.metrics_collector.other_committer_gone();
                err.context(format!(
                    "[Committer, stream={}] Could not send commit message to the commit worker",
                    self.stream_id
                ))
                .into()
            })
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    /// Is the committer still running?
    pub fn is_running(&self) -> bool {
        !(self.lifecycle.is_any_cancelled())
    }

    /// Order the committer to stop.
    pub fn stop(&self) {
        self.lifecycle.request_cancellation()
    }
}

fn start_commit_loop<C, M>(
    receiver: mpsc::Receiver<CommitterMessage>,
    strategy: CommitStrategy,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    connector: C,
    lifecycle: AutoCancellationToken,
    metrics_collector: M,
) where
    C: ApiClient + Send + 'static,
    M: MetricsCollector + Send + 'static,
{
    let builder = thread::Builder::new().name("nakadion-committer".into());
    builder
        .spawn(move || {
            run_commit_loop(
                receiver,
                strategy,
                subscription_id,
                stream_id,
                connector,
                lifecycle,
                metrics_collector,
            );
        })
        .unwrap();
}

struct CommitEntry {
    // timestamp when this entry was created
    created_at: Instant,
    // No matter how many batches/cursors are added, this is the deadline
    commit_deadline: Instant,
    // The number of batches(cursors) that have been "updated" into this entry
    num_batches: usize,
    // The number of events that have been updated into this entry
    num_events: usize,
    // The last batch that created this entry
    batch: Batch,
    // timestamp when the first cursor was received by Nakadion
    first_cursor_received_at: Instant,
    // timestamp when the current cursor was received by Nakadion
    current_cursor_received_at: Instant,
}

impl CommitEntry {
    pub fn new(
        batch: Batch,
        strategy: CommitStrategy,
        num_events_hint: Option<usize>,
    ) -> CommitEntry {
        let commit_deadline = match strategy {
            CommitStrategy::AllBatches => Instant::now(),
            CommitStrategy::Batches {
                after_seconds: Some(after_seconds),
                ..
            } => {
                let by_strategy = batch.received_at + Duration::from_secs(u64::from(after_seconds));
                ::std::cmp::min(
                    by_strategy,
                    batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET),
                )
            }
            CommitStrategy::Events {
                after_seconds: Some(after_seconds),
                ..
            } => {
                let by_strategy = batch.received_at + Duration::from_secs(u64::from(after_seconds));
                ::std::cmp::min(
                    by_strategy,
                    batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET),
                )
            }
            CommitStrategy::AfterSeconds { seconds } => {
                let by_strategy = batch.received_at + Duration::from_secs(u64::from(seconds));
                ::std::cmp::min(
                    by_strategy,
                    batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET),
                )
            }
            _ => batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET),
        };
        let received_at = batch.received_at;
        CommitEntry {
            created_at: Instant::now(),
            commit_deadline,
            num_batches: 1,
            num_events: num_events_hint.unwrap_or(0),
            batch,
            first_cursor_received_at: received_at,
            current_cursor_received_at: received_at,
        }
    }

    pub fn update(&mut self, next_batch: Batch, num_events_hint: Option<usize>) {
        self.current_cursor_received_at = next_batch.received_at;
        self.batch = next_batch;
        self.num_events += num_events_hint.unwrap_or(0);
        self.num_batches += 1;
    }

    pub fn is_due_by_deadline(&self) -> bool {
        self.commit_deadline <= Instant::now()
    }
}

fn run_commit_loop<C, M>(
    receiver: mpsc::Receiver<CommitterMessage>,
    strategy: CommitStrategy,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    client: C,
    lifecycle: AutoCancellationToken,
    metrics_collector: M,
) where
    C: ApiClient,
    M: MetricsCollector,
{
    let mut cursors = HashMap::new();
    loop {
        if lifecycle.cancellation_requested() {
            info!(
                "[Committer, subscription={}, stream={}] Abort requested. Flushing cursors",
                subscription_id, stream_id
            );
            flush_all_cursors::<_>(cursors, &subscription_id, &stream_id, &client);
            break;
        }

        match receiver.recv_timeout(Duration::from_millis(50)) {
            Ok(CommitterMessage::Commit(next_batch, num_events_hint)) => {
                metrics_collector.committer_batch_received(next_batch.received_at);
                let key = (
                    next_batch.batch_line.partition().to_vec(),
                    next_batch.batch_line.event_type().to_vec(),
                );

                match cursors.entry(key) {
                    Entry::Vacant(entry) => {
                        entry.insert(CommitEntry::new(next_batch, strategy, num_events_hint));
                    }
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().update(next_batch, num_events_hint);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                warn!(
                    "[Committer, subscription={}, stream={}] Commit channel disconnected.\
                     Flushing all cursors before stopping.",
                    subscription_id, stream_id
                );
                flush_all_cursors::<_>(cursors, &subscription_id, &stream_id, &client);
                break;
            }
        }

        match flush_if_due(
            &mut cursors,
            &subscription_id,
            &stream_id,
            &client,
            strategy,
            &metrics_collector,
        ) {
            Ok(CommitStatus::NotAllOffsetsIncreased) => info!(
                "[Committer, subscription={}, stream={}] Not all cursors were increased.",
                subscription_id, stream_id
            ),
            Err(err) => {
                error!(
                    "[Committer, subscription={}, stream={}] Aborting. Failed to commit cursors: {}",
                    subscription_id, stream_id, err
                );
                break;
            }
            _ => {}
        }
    }

    info!(
        "[Committer, subscription={}, stream={}] Committer stopped.",
        subscription_id, stream_id
    );
}

fn flush_all_cursors<C>(
    all_cursors: HashMap<(Vec<u8>, Vec<u8>), CommitEntry>,
    subscription_id: &SubscriptionId,
    stream_id: &StreamId,
    connector: &C,
) where
    C: ApiClient,
{
    // We are not interested in metrics here

    if all_cursors.is_empty() {
        info!(
            "[Committer, subscription={}, stream={}] No cursors to finally commit.",
            subscription_id, stream_id
        )
    } else {
        let cursors_to_commit: Vec<_> = all_cursors
            .values()
            .map(|v| v.batch.batch_line.cursor())
            .collect();

        let flow_id = FlowId::default();

        match connector.commit_cursors(
            subscription_id,
            stream_id,
            &cursors_to_commit,
            flow_id.clone(),
        ) {
            Ok(CommitStatus::AllOffsetsIncreased) => info!(
                "[Committer, subscription={}, stream={}, flow id={}] All remaining offsets\
                 increased.",
                subscription_id, stream_id, flow_id
            ),
            Ok(CommitStatus::NotAllOffsetsIncreased) => info!(
                "[Committer, subscription={}, stream={}, flow id={}] Not all remaining\
                 offsets increased.",
                subscription_id, stream_id, flow_id
            ),
            Ok(CommitStatus::NothingToCommit) => info!(
                "[Committer, subscription={}, stream={}, flow id={}] There was nothing\
                 to be finally committed.",
                subscription_id, stream_id, flow_id
            ),
            Err(err) => error!(
                "[Committer, subscription={}, stream={}, flow id={}] Failed to commit all\
                 remaining cursors: {}",
                subscription_id, stream_id, flow_id, err
            ),
        }
    }
}

fn flush_if_due<C, M>(
    all_cursors: &mut HashMap<(Vec<u8>, Vec<u8>), CommitEntry>,
    subscription_id: &SubscriptionId,
    stream_id: &StreamId,
    client: &C,
    strategy: CommitStrategy,
    metrics_collector: &M,
) -> Result<CommitStatus, CommitError>
where
    C: ApiClient,
    M: MetricsCollector,
{
    let num_batches: usize = all_cursors.iter().map(|entry| entry.1.num_batches).sum();
    let num_events: usize = all_cursors.iter().map(|entry| entry.1.num_events).sum();

    let commit_by_other_than_deadline = match strategy {
        CommitStrategy::AllBatches => true,
        CommitStrategy::Batches { after_batches, .. } => num_batches >= after_batches as usize,
        CommitStrategy::Events { after_events, .. } => num_events >= after_events as usize,
        _ => false,
    };

    let commit_by_deadline = all_cursors.values().any(|entry| entry.is_due_by_deadline());

    let mut cursors_to_commit: Vec<Vec<u8>> = Vec::new();
    let mut num_batches_to_commit = 0;
    let mut num_events_to_commit = 0;
    if commit_by_deadline || commit_by_other_than_deadline {
        for entry in all_cursors.values() {
            num_batches_to_commit += entry.num_batches;
            num_events_to_commit += entry.num_events;
            update_cursor_metrics(metrics_collector, entry);
            cursors_to_commit.push(entry.batch.batch_line.cursor().to_vec());
        }
    }

    let flow_id = FlowId::default();

    let status = if !cursors_to_commit.is_empty() {
        let start = Instant::now();
        match client.commit_cursors_budgeted(
            subscription_id,
            stream_id,
            &cursors_to_commit,
            flow_id.clone(),
            Duration::from_secs(5),
        ) {
            Ok(s) => {
                metrics_collector.committer_cursor_commit_attempt(start);
                metrics_collector.committer_cursor_committed(start);
                metrics_collector.committer_batches_committed(num_batches_to_commit);
                metrics_collector.committer_events_committed(num_events_to_commit);
                all_cursors.clear();
                s
            }
            Err(err) => {
                metrics_collector.committer_cursor_commit_attempt(start);
                metrics_collector.committer_cursor_commit_failed(start);
                return Err(err);
            }
        }
    } else {
        CommitStatus::NothingToCommit
    };

    Ok(status)
}

fn update_cursor_metrics<M>(metrics_collector: &M, entry: &CommitEntry)
where
    M: MetricsCollector,
{
    metrics_collector
        .committer_first_cursor_age_on_commit(entry.first_cursor_received_at.elapsed());
    metrics_collector
        .committer_last_cursor_age_on_commit(entry.current_cursor_received_at.elapsed());
    metrics_collector.committer_cursor_buffer_time(entry.created_at.elapsed());

    let commit_deadline = entry.first_cursor_received_at + Duration::from_secs(60);
    let now = Instant::now();
    if commit_deadline >= now {
        metrics_collector
            .committer_time_left_on_commit_until_invalid(commit_deadline.duration_since(now));
    }
}
