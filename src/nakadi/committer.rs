use std::sync::mpsc;
use std::thread;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::{Duration, Instant};

use nakadi::CommitStrategy;
use nakadi::api_client::{ApiClient, CommitError, CommitStatus};
use nakadi::model::{FlowId, StreamId, SubscriptionId};
use nakadi::batch::Batch;
use nakadi::Lifecycle;

const CURSOR_COMMIT_OFFSET: u64 = 55;

#[derive(Clone)]
pub struct Committer {
    sender: mpsc::Sender<CommitterMessage>,
    stream_id: StreamId,
    lifecycle: Lifecycle,
    subscription_id: SubscriptionId,
}

enum CommitterMessage {
    Commit(Batch, Option<usize>),
}

impl Committer {
    pub fn start<C>(
        client: C,
        strategy: CommitStrategy,
        subscription_id: SubscriptionId,
        stream_id: StreamId,
    ) -> Self
    where
        C: ApiClient + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();

        let lifecycle = Lifecycle::default();

        start_commit_loop(
            receiver,
            strategy,
            subscription_id.clone(),
            stream_id.clone(),
            client,
            lifecycle.clone(),
        );

        Committer {
            sender,
            stream_id,
            lifecycle,
            subscription_id,
        }
    }

    pub fn commit(&self, batch: Batch, num_events_hint: Option<usize>) -> Result<(), String> {
        self.sender
            .send(CommitterMessage::Commit(batch, num_events_hint))
            .map_err(|err| {
                format!(
                    "Stream {} - Could not accept commit request: {}",
                    self.stream_id, err
                )
            })
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn running(&self) -> bool {
        self.lifecycle.running()
    }

    pub fn stop(&self) {
        self.lifecycle.request_abort()
    }
}

fn start_commit_loop<C>(
    receiver: mpsc::Receiver<CommitterMessage>,
    strategy: CommitStrategy,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    connector: C,
    lifecycle: Lifecycle,
) where
    C: ApiClient + Send + 'static,
{
    thread::spawn(move || {
        run_commit_loop(
            receiver,
            strategy,
            subscription_id,
            stream_id,
            connector,
            lifecycle,
        );
    });
}

struct CommitEntry {
    commit_deadline: Instant,
    num_batches: usize,
    num_events: usize,
    batch: Batch,
    first_cursor_received_at: Instant,
}

impl CommitEntry {
    pub fn new(
        batch: Batch,
        strategy: CommitStrategy,
        num_events_hint: Option<usize>,
    ) -> CommitEntry {
        let first_cursor_received_at = batch.received_at;
        let commit_deadline = match strategy {
            CommitStrategy::AllBatches => Instant::now(),
            CommitStrategy::EveryNBatches(_) => {
                batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET)
            }
            CommitStrategy::EveryNEvents(_) => {
                batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET)
            }
            CommitStrategy::MaxAge => batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET),
            CommitStrategy::EveryNSeconds(n) => {
                let by_strategy = Instant::now() + Duration::from_secs(n as u64);
                ::std::cmp::min(
                    by_strategy,
                    batch.received_at + Duration::from_secs(CURSOR_COMMIT_OFFSET),
                )
            }
        };
        CommitEntry {
            commit_deadline,
            num_batches: 1,
            num_events: num_events_hint.unwrap_or(0),
            batch,
            first_cursor_received_at,
        }
    }

    pub fn update(&mut self, next_batch: Batch, num_events_hint: Option<usize>) {
        self.batch = next_batch;
        self.num_events += num_events_hint.unwrap_or(0);
        self.num_batches += 1;
    }

    pub fn is_due_by_deadline(&self) -> bool {
        self.commit_deadline <= Instant::now()
    }
}

fn run_commit_loop<C>(
    receiver: mpsc::Receiver<CommitterMessage>,
    strategy: CommitStrategy,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    client: C,
    lifecycle: Lifecycle,
) where
    C: ApiClient,
{
    let mut cursors = HashMap::new();
    loop {
        if lifecycle.abort_requested() {
            info!("Stream {} - Abort requested. Flushing cursors", stream_id);
            flush_all_cursors::<_>(cursors, &subscription_id, &stream_id, &client);
            break;
        }

        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(CommitterMessage::Commit(next_batch, num_events_hint)) => {
                let mut key = (
                    next_batch.batch_line.partition().to_vec(),
                    next_batch.batch_line.event_type().to_vec(),
                );

                match cursors.entry(key) {
                    Entry::Vacant(mut entry) => {
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
                    "Stream {} - Commit channel disconnected. Flushing cursors.",
                    stream_id
                );
                flush_all_cursors::<_>(cursors, &subscription_id, &stream_id, &client);
                break;
            }
        }

        if let Err(err) = flush_due_cursors(
            &mut cursors,
            &subscription_id,
            &stream_id,
            &client,
            strategy,
        ) {
            error!("Stream {} - Failed to commit cursors: {}", stream_id, err);
            break;
        }
    }

    lifecycle.stopped();
    info!("Stream {} - Committer stopped.", stream_id);
}

fn flush_all_cursors<C>(
    all_cursors: HashMap<(Vec<u8>, Vec<u8>), CommitEntry>,
    subscription_id: &SubscriptionId,
    stream_id: &StreamId,
    connector: &C,
) where
    C: ApiClient,
{
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
        Ok(CommitStatus::AllOffsetsIncreased) => {
            info!("Stream {} - All remaining offstets increased.", stream_id)
        }
        Ok(CommitStatus::NotAllOffsetsIncreased) => info!(
            "Stream {} - Not all remaining offstets increased.",
            stream_id
        ),
        Ok(CommitStatus::NothingToCommit) => info!(
            "Stream {} - There was nothing to be finally committed.",
            stream_id
        ),
        Err(err) => error!(
            "Stream {} - FlowId {} - Failed to commit all remaining cursors: {}",
            stream_id, flow_id, err
        ),
    }
}

fn flush_due_cursors<C>(
    all_cursors: &mut HashMap<(Vec<u8>, Vec<u8>), CommitEntry>,
    subscription_id: &SubscriptionId,
    stream_id: &StreamId,
    client: &C,
    strategy: CommitStrategy,
) -> Result<CommitStatus, CommitError>
where
    C: ApiClient,
{
    let num_batches: usize = all_cursors.iter().map(|entry| entry.1.num_batches).sum();
    let num_events: usize = all_cursors.iter().map(|entry| entry.1.num_events).sum();

    let commit_all = match strategy {
        CommitStrategy::EveryNBatches(n) => num_batches >= n as usize,
        CommitStrategy::EveryNEvents(n) => num_events >= n as usize,
        _ => false,
    };

    let mut cursors_to_commit: Vec<Vec<u8>> = Vec::new();
    let mut keys_to_commit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    if commit_all {
        for (key, entry) in &*all_cursors {
            cursors_to_commit.push(entry.batch.batch_line.cursor().to_vec());
            keys_to_commit.push(key.clone());
        }
    } else {
        for (key, entry) in &*all_cursors {
            if entry.is_due_by_deadline() {
                cursors_to_commit.push(entry.batch.batch_line.cursor().to_vec());
                keys_to_commit.push(key.clone());
            }
        }
    }

    let flow_id = FlowId::default();

    let status = if !cursors_to_commit.is_empty() {
        client.commit_cursors(
            subscription_id,
            stream_id,
            &cursors_to_commit,
            flow_id.clone(),
        )?
    } else {
        CommitStatus::NothingToCommit
    };

    for key in keys_to_commit {
        all_cursors.remove(&key);
    }

    Ok(status)
}
