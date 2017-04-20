
use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;
use std::time::Duration;

use serde_json;

use ::{PartitionId, Handler, SubscriptionId, StreamId, Cursor, AfterBatchAction, BatchInfo};
use super::connector::Checkpoints;
use super::WorkerMessage;


const RETRY_MILLIS: &'static [u64] = &[10, 20, 50, 100, 200, 300, 400, 500, 1000, 2000, 5000,
                                       10000, 30000, 60000, 300000, 600000];

pub enum PartitionWorkerMessage {
    Stopped(PartitionId),
}

pub struct PartionWorker;

impl PartionWorker {
    pub fn start<C: Checkpoints + Send + Sync + 'static>(checkpointer: Arc<C>,
                                                         handler: Arc<Handler + Sync>,
                                                         batch_receiver: Receiver<WorkerMessage>,
                                                         sender: Sender<PartitionWorkerMessage>,
                                                         subscription_id: SubscriptionId,
                                                         stream_id: StreamId,
                                                         partition_id: PartitionId) {

        let handler = handler.clone();
        let checkpointer = checkpointer.clone();

        thread::spawn(move || {
            let worker = PartitionWorkerInternal {
                handler: &*handler,
                checkpointer: &*checkpointer,
                batch_receiver: batch_receiver,
                sender: sender,
                subscription_id: subscription_id,
                stream_id: stream_id,
                partition_id: partition_id,
            };

            worker.run_a_loop();
        });
    }
}

struct PartitionWorkerInternal<'a> {
    handler: &'a Handler,
    checkpointer: &'a Checkpoints,
    batch_receiver: Receiver<WorkerMessage>,
    sender: Sender<PartitionWorkerMessage>,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    partition_id: PartitionId,
}

impl<'a> PartitionWorkerInternal<'a> {
    fn run_a_loop(&self) {
        info!("Partition worker {} started", self.partition_id.0);

        loop {
            let msg = match self.batch_receiver.recv() {
                Ok(msg) => msg,
                Err(err) => {
                    error!("Partition {}: Worker disconnected. Sending stop to worker. Error: {}",
                           self.partition_id.0,
                           err);
                    if let Err(err) = self.sender
                        .send(PartitionWorkerMessage::Stopped(self.partition_id.clone())) {
                        error!("Partition {}: Failed to send 'Stopped' message to worker. \
                                Stopping anyways.",
                               self.partition_id.0);
                        break;
                    } else {
                        break;
                    }
                }
            };

            match msg {
                WorkerMessage::Batch(batch_info, batch) => {
                    match self.process_line(batch_info, batch) {
                        AfterBatchAction::Stop | AfterBatchAction::Abort => {
                            info!("Partition {}: Request to stop by handler. Forwarding to \
                                   worker.",
                                  self.partition_id.0);
                            // LOOP UNTIL STOP!!!!!!

                        }
                        _ => (),
                    }
                }
                WorkerMessage::Stop => {
                    if let Err(err) = self.sender
                        .send(PartitionWorkerMessage::Stopped(self.partition_id.clone())) {
                        error!("Partition {}: Failed to send 'Stopped' message to worker. \
                                Stopping anyways.",
                               self.partition_id.0);
                    }
                    break;
                }
            }
        }
        info!("Partition worker {} stopped", self.partition_id.0);
    }

    fn process_line(&self, batch_info: BatchInfo, batch: String) -> AfterBatchAction {
        let cursor = batch_info.cursor.clone();
        match self.handler.handle(batch, batch_info) {
            AfterBatchAction::Continue => {
                self.checkpoint(cursor);
                AfterBatchAction::Continue
            }
            AfterBatchAction::ContinueNoCheckpoint => AfterBatchAction::ContinueNoCheckpoint,
            AfterBatchAction::Stop => {
                self.checkpoint(cursor);
                AfterBatchAction::Stop
            }
            AfterBatchAction::Abort => {
                warn!("Partition {}: Abort. Skipping checkpointing on stream {}.",
                      self.partition_id.0,
                      self.stream_id.0);
                AfterBatchAction::Abort
            }
        }
    }



    fn checkpoint(&self, cursor: Cursor) {
        let cursors = &[cursor];
        let mut attempt = 0;
        while attempt < 5 {
            if attempt > 0 {
                let pause = retry_pause(attempt - 1);
                thread::sleep(pause)
            }
            attempt += 1;
            match self.checkpointer.checkpoint(&self.stream_id, &self.subscription_id, cursors) {
                Ok(()) => return,
                Err(err) => {
                    if attempt > 5 {
                        error!("Partition {}: Finally gave up to checkpoint cursor after {} \
                                attempts.",
                               self.partition_id.0,
                               err);
                        return;
                    } else {
                        warn!("Partition {}: Failed to checkpoint to Nakadi: {}",
                              self.partition_id.0,
                              err);
                    }
                }
            }
        }
        error!("Partition {}: Checkpointing aborted due to worker shutdown.",
               self.partition_id.0);
    }
}


fn retry_pause(retry: usize) -> Duration {
    let idx = ::std::cmp::min(retry, RETRY_MILLIS.len() - 1);
    ::std::time::Duration::from_millis(RETRY_MILLIS[idx])
}
