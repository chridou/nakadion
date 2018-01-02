use std::sync::mpsc;
use std::thread;

use nakadi::batch::{Batch, BatchLine};
use nakadi::subscription::AbortHandle;
use nakadi::subscription::connector::StreamConnector;

pub struct Committer<B: BatchLine> {
    abort_handle: AbortHandle,
    sender: mpsc::Sender<CommitterMessage<B>>,
}

enum CommitterMessage<L: BatchLine> {
    Commit(Batch<L>),
}

#[derive(Clone, Copy)]
pub enum CommitStrategy {
    EveryNSeconds(u16),
}

impl<L: BatchLine + Send + 'static> Committer<L> {
    pub fn new<C>(connector: C, strategy: CommitStrategy, abort_handle: AbortHandle) -> Self
    where
        C: StreamConnector,
    {
        let (sender, receiver) = mpsc::channel();

        start_commit_loop(receiver, strategy, abort_handle.clone());

        Committer {
            abort_handle,
            sender,
        }
    }

    pub fn commit(&self, batch: Batch<L>) -> Result<(), String> {
        self.sender
            .send(CommitterMessage::Commit(batch))
            .map_err(|err| format!("Could not accept commit request: {}", err))
    }
}

fn start_commit_loop<L>(
    receiver: mpsc::Receiver<CommitterMessage<L>>,
    strategy: CommitStrategy,
    abort_handle: AbortHandle,
) where
    L: BatchLine + Send + 'static,
{
    thread::spawn(move || {
        run_commit_loop(receiver, strategy, &abort_handle);
    });
}

fn run_commit_loop<B>(
    receiver: mpsc::Receiver<CommitterMessage<B>>,
    strategy: CommitStrategy,
    abort_handle: &AbortHandle,
) where
    B: BatchLine,
{
    loop {
        if abort_handle.is_abort_requested() {
            break;
        }
    }
}
