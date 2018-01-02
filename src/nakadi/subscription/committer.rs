use std::sync::mpsc;
use std::thread;
use std::collections::HashMap;
use std::time::{Instant, Duration};

use CommitStrategy;
use nakadi::subscription::model::StreamId;
use nakadi::batch::{Batch, BatchLine};
use nakadi::subscription::AbortHandle;
use nakadi::subscription::connector::StreamConnector;

pub struct Committer<B: BatchLine> {
    sender: mpsc::Sender<CommitterMessage<B>>,
    stream_id: StreamId,
}

enum CommitterMessage<L: BatchLine> {
    Commit(Batch<L>),
}

impl<L: BatchLine + Send + 'static> Committer<L> {
    pub fn new<C>(connector: C, 
    strategy: CommitStrategy, 
    stream_id: StreamId,
    abort_handle: AbortHandle) -> Self
    where
        C: StreamConnector,
    {
        let (sender, receiver) = mpsc::channel();

        start_commit_loop(receiver, strategy, stream_id.clone(), abort_handle.clone());

        Committer {
            sender,
            stream_id,
        }
    }

    pub fn commit(&self, batch: Batch<L>) -> Result<(), String> {
        self.sender
            .send(CommitterMessage::Commit(batch))
            .map_err(|err| format!("Stream {} - Could not accept commit request: {}", 
            self.stream_id, err))
    }
}

fn start_commit_loop<L>(
    receiver: mpsc::Receiver<CommitterMessage<L>>,
    strategy: CommitStrategy, 
    stream_id: StreamId,
    abort_handle: AbortHandle,
) where
    L: BatchLine + Send + 'static,
{
    thread::spawn(move || {
        run_commit_loop(receiver, strategy, stream_id, abort_handle);
    });
}

struct CommitEntry<B: BatchLine> {
    commit_deadline: Instant,
    batch: Batch<B>,
}

impl<B: BatchLine> CommitEntry<B> {
    pub fn new(batch: Batch<B>, strategy: CommitStrategy) -> CommitEntry<B> {
        let now = Instant::now();
        let commit_deadline = match strategy {
            CommitStrategy::AllBatches => now,
            CommitStrategy::MaxAge => now,
            CommitStrategy::EveryNSeconds(n) => {
                let by_strategy = now + Duration::from_secs(n as u64);
                let by_batch = ::std::cmp::max(batch.commit_deadline, now);
                ::std::cmp::min(by_strategy, by_batch)
            },
        };
        CommitEntry {
            commit_deadline,
            batch,
        }
    }

    pub fn update(&mut self, next_batch: Batch<B>) {
        self.batch = next_batch;
    }

    pub fn is_due(&self) -> bool {
        self.commit_deadline <= Instant::now()
    }
}

fn run_commit_loop<B>(
    receiver: mpsc::Receiver<CommitterMessage<B>>,
    strategy: CommitStrategy, 
    stream_id: StreamId,
    abort_handle: AbortHandle,
) where
    B: BatchLine,
{
    let mut cursors = HashMap::new();
    loop {
        if abort_handle.abort_requested() {
            info!("Stream {} - Abort requested.", stream_id);
            break;
        }

        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(next_batch) => {

            },
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                error!("Stream {} - Commit channel disconnected.", stream_id);
                abort_handle.request_abort();
                break;
            }
        }
    }

    info!("Stream {} - Committer stopped.", stream_id);
    abort_handle.mark_committer_stopped()
}
