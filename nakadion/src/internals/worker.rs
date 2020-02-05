use std::sync::Arc;
use std::time::Duration;

use crossbeam::queue::SegQueue;
use tokio::{self, sync::mpsc::UnboundedSender};

use crate::nakadi_types::model::{event_type::EventTypeName, partition::PartitionId};

use crate::event_handler::{BatchHandler, BatchHandlerFactory, HandlerAssignment};
use crate::event_stream::BatchLine;
use crate::internals::{committer::CommitData, StreamState};

pub struct WorkerAssignment {
    pub event_type: Option<EventTypeName>,
    pub partition: Option<PartitionId>,
}

impl WorkerAssignment {
    pub fn handler_assignment(&self) -> HandlerAssignment {
        HandlerAssignment {
            event_type: self.event_type.as_ref(),
            partition: self.partition.as_ref(),
        }
    }
}

pub struct Worker<H> {
    queue: Arc<SegQueue<BatchLine>>,
    handler: Option<H>,
}

impl Worker<H>
where
    H: BatchHandler,
{
    pub fn new(
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        assignment: WorkerAssignment,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
    ) -> Self {
        let queue = Arc::new(SegQueue::new());
        processor::Processor::start(
            Arc::clone(&queue),
            handler_factory,
            Duration::from_millis(100),
            assignment,
            stream_state,
            committer,
        );

        Self { queue }
    }

    pub fn process(&self, batch: BatchLine) {
        self.queue.push(batch);
    }
}

mod processor {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use crossbeam::queue::SegQueue;
    use tokio::{self, sync::mpsc::UnboundedSender, task::JoinHandle, time::delay_for};

    use crate::nakadi_types::{model::subscription::SubscriptionCursor, GenericError};

    use crate::event_handler::{BatchHandler, BatchHandlerFactory, BatchMeta, BatchPostAction};
    use crate::event_stream::BatchLine;
    use crate::internals::{committer::CommitData, StreamState};

    use super::WorkerAssignment;

    pub struct Processor<H> {
        queue: Arc<SegQueue<BatchLine>>,
        handler: Option<H>,
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        last_event_processed: Instant,
        stream_state: StreamState,
        assignment: WorkerAssignment,
        committer: UnboundedSender<CommitData>,
    }

    impl<H> Processor<H>
    where
        H: BatchHandler,
    {
        pub fn start(
            queue: Arc<SegQueue<BatchLine>>,
            handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
            poll_interval: Duration,
            assignment: WorkerAssignment,
            stream_state: StreamState,
            committer: UnboundedSender<CommitData>,
        ) -> JoinHandle<Option<H>> {
            let mut processor = Processor {
                handler: None,
                handler_factory,
                queue,
                last_event_processed: Instant::now(),
                stream_state,
                assignment,
                committer,
            };

            let processor_loop = async move {
                loop {
                    if processor.stream_state.cancellation_requested() {
                        break;
                    }

                    match processor.process_queue().await {
                        Ok(true) => {}
                        Ok(false) => delay_for(Duration::from_millis(100)).await,
                        Err(err) => {
                            processor.stream_state.request_global_cancellation();
                            break;
                        }
                    }
                }
                processor.handler
            };

            tokio::spawn(processor_loop)
        }

        pub fn next_batch(&self) -> Option<BatchLine> {
            self.queue.pop().ok()
        }

        /// If this function returns with an error, treat is as a critical
        /// error that aborts the whole consumption
        ///
        /// Returns true if events have been processed
        pub async fn process_queue(&mut self) -> Result<bool, GenericError> {
            let mut processed_at_least_one_event = false;
            while let Some(batch) = self.next_batch() {
                if self.stream_state.cancellation_requested() {
                    break;
                }

                let events = if let Some(events) = batch.events_bytes() {
                    events
                } else {
                    // We do not expect info or keep alive lines here
                    continue;
                };

                let received_at = batch.received_at();
                let batch_id = batch.frame_id();
                let cursor = batch.cursor_deserialized::<SubscriptionCursor>()?;

                let meta = BatchMeta {
                    cursor: &cursor,
                    received_at,
                    batch_id,
                };

                match self.process_batch(events, meta).await? {
                    BatchPostAction::Commit { n_events } => {
                        let commit_data = CommitData {
                            cursor,
                            received_at,
                            batch_id,
                            events_hint: n_events,
                        };
                        self.try_commit(commit_data)?
                    }
                    BatchPostAction::DoNotCommit { n_events } => {}
                    BatchPostAction::AbortStream => {
                        self.stream_state.request_stream_cancellation();
                        break;
                    }
                    BatchPostAction::ShutDown => {
                        self.stream_state.request_global_cancellation();
                        break;
                    }
                }
                processed_at_least_one_event = true;
                self.last_event_processed = Instant::now();
            }
            Ok(processed_at_least_one_event)
        }

        async fn process_batch<'a>(
            &mut self,
            events: Bytes,
            meta: BatchMeta<'a>,
        ) -> Result<BatchPostAction, GenericError> {
            let handler = self.get_handler().await?;
            Ok(handler.handle(events, meta).await)
        }

        fn try_commit(&mut self, commit_data: CommitData) -> Result<(), GenericError> {
            if let Err(err) = self.committer.send(commit_data) {
                return Err(GenericError::new(format!(
                    "failed to enqueue commit data: {}",
                    err
                )));
            }

            Ok(())
        }

        async fn get_handler(&mut self) -> Result<&mut H, GenericError> {
            if self.handler.is_none() {
                let new_handler = self
                    .handler_factory
                    .handler(self.assignment.handler_assignment())
                    .await?;

                self.handler = Some(new_handler);
            }

            Ok(self.handler.as_mut().unwrap())
        }
    }
}
