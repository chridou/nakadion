use std::sync::mpsc;
use std::thread;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::{Duration, Instant};

use nakadi::CommitStrategy;
use nakadi::client::CommitError;
use nakadi::model::{StreamId, SubscriptionId};
use nakadi::batch::Batch;
use nakadi::Lifecycle;
use nakadi::client::StreamingClient;

#[derive(Clone)]
pub struct Committer {
    sender: mpsc::Sender<CommitterMessage>,
    stream_id: StreamId,
    lifecycle: Lifecycle,
    subscription_id: SubscriptionId,
}

enum CommitterMessage {
    Commit(Batch),
}

impl Committer {
    pub fn start<C>(client: C, strategy: CommitStrategy, stream_id: StreamId) -> Self
    where
        C: StreamingClient + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();

        let lifecycle = Lifecycle::default();

        let subscription_id = client.subscription_id().clone();

        start_commit_loop(
            receiver,
            strategy,
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

    pub fn commit(&self, batch: Batch) -> Result<(), String> {
        self.sender
            .send(CommitterMessage::Commit(batch))
            .map_err(|err| {
                format!(
                    "Stream {} - Could not accept commit request: {}",
                    self.stream_id, err
                )
            })
    }

    pub fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
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
    stream_id: StreamId,
    connector: C,
    lifecycle: Lifecycle,
) where
    C: StreamingClient + Send + 'static,
{
    thread::spawn(move || {
        run_commit_loop(receiver, strategy, stream_id, connector, lifecycle);
    });
}

struct CommitEntry {
    commit_deadline: Instant,
    batch: Batch,
}

impl CommitEntry {
    pub fn new(batch: Batch, strategy: CommitStrategy) -> CommitEntry {
        let commit_deadline = match strategy {
            CommitStrategy::AllBatches => Instant::now(),
            CommitStrategy::MaxAge => batch.commit_deadline,
            CommitStrategy::EveryNSeconds(n) => {
                let by_strategy = Instant::now() + Duration::from_secs(n as u64);
                ::std::cmp::min(by_strategy, batch.commit_deadline)
            }
        };
        CommitEntry {
            commit_deadline,
            batch,
        }
    }

    pub fn update(&mut self, next_batch: Batch) {
        self.batch = next_batch;
    }

    pub fn is_due(&self) -> bool {
        self.commit_deadline <= Instant::now()
    }
}

fn run_commit_loop<C>(
    receiver: mpsc::Receiver<CommitterMessage>,
    strategy: CommitStrategy,
    stream_id: StreamId,
    client: C,
    lifecycle: Lifecycle,
) where
    C: StreamingClient,
{
    let mut cursors = HashMap::new();
    loop {
        if lifecycle.abort_requested() {
            info!("Stream {} - Abort requested. Flushing cursors", stream_id);
            flush_all_cursors::<_>(cursors, &stream_id, &client);
            break;
        }

        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(CommitterMessage::Commit(next_batch)) => {
                let mut key = (
                    next_batch.batch_line.partition().to_vec(),
                    next_batch.batch_line.event_type().to_vec(),
                );

                match cursors.entry(key) {
                    Entry::Vacant(mut entry) => {
                        entry.insert(CommitEntry::new(next_batch, strategy));
                    }
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().update(next_batch);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                warn!(
                    "Stream {} - Commit channel disconnected. Flushing cursors.",
                    stream_id
                );
                flush_all_cursors::<_>(cursors, &stream_id, &client);
                break;
            }
        }

        if let Err(err) = flush_due_cursors(&mut cursors, &stream_id, &client) {
            error!("Stream {} - Failed to commit cursors: {}", stream_id, err);
            break;
        }
    }

    lifecycle.stopped();
    info!("Stream {} - Committer stopped.", stream_id);
}

fn flush_all_cursors<C>(
    all_cursors: HashMap<(Vec<u8>, Vec<u8>), CommitEntry>,
    stream_id: &StreamId,
    connector: &C,
) where
    C: StreamingClient,
{
    let cursors_to_commit: Vec<_> = all_cursors
        .values()
        .map(|v| v.batch.batch_line.cursor())
        .collect();
    match connector.commit(stream_id.clone(), &cursors_to_commit) {
        Ok(()) => info!("Stream {} - Committed all remaining cursors.", stream_id),
        Err(err) => error!(
            "Stream {} - Failed to commit all remaining cursors: {}",
            stream_id, err
        ),
    }
}

fn flush_due_cursors<C>(
    all_cursors: &mut HashMap<(Vec<u8>, Vec<u8>), CommitEntry>,
    stream_id: &StreamId,
    client: &C,
) -> Result<(), CommitError>
where
    C: StreamingClient,
{
    let mut cursors_to_commit: Vec<Vec<u8>> = Vec::new();
    let mut keys_to_commit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        for (key, entry) in &*all_cursors {
            if entry.is_due() {
                cursors_to_commit.push(entry.batch.batch_line.cursor().to_vec());
                keys_to_commit.push(key.clone());
            }
        }
    }

    if !cursors_to_commit.is_empty() {
        let _ = client.commit(stream_id.clone(), &cursors_to_commit)?;
    }

    for key in keys_to_commit {
        all_cursors.remove(&key);
    }

    Ok(())
}
