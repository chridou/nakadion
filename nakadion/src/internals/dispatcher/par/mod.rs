use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use crate::internals::{
    background_committer::CommitHandle, worker::*, ConsumptionResult, StreamState,
};

pub mod et_par;
pub mod etp_par;

type BufferedWorkerJoin<'a> = BoxFuture<'a, ConsumptionResult<SleepingWorker>>;

/// A worker with a channel(buffer) in front of it
struct BufferedWorker {
    join: BufferedWorkerJoin<'static>,
    sender: UnboundedSender<WorkerMessage>,
}

impl BufferedWorker {
    fn new(
        sleeping_worker: SleepingWorker,
        stream_state: StreamState,
        committer: CommitHandle,
    ) -> BufferedWorker {
        let (tx, rx) = unbounded_channel::<WorkerMessage>();

        let active_worker = sleeping_worker.start(stream_state, committer, rx);

        let join = async move { active_worker.join().await }.boxed();

        BufferedWorker { join, sender: tx }
    }

    pub fn process(&self, msg: WorkerMessage) -> bool {
        if let Err(_err) = self.sender.send(msg) {
            false
        } else {
            true
        }
    }

    pub fn join(self) -> BufferedWorkerJoin<'static> {
        self.join
    }
}
