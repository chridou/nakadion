use std::sync::Arc;
use std::time::Duration;

use crossbeam::queue::SegQueue;
use futures::FutureExt;
use tokio::{self, sync::mpsc::UnboundedSender, task::JoinHandle};

use crate::nakadi_types::{
    model::{event_type::EventTypeName, partition::PartitionId},
    GenericError,
};

use crate::consumer::{ConsumerError, ConsumerErrorKind};
use crate::event_handler::{BatchHandler, BatchHandlerFactory, HandlerAssignment};
use crate::event_stream::BatchLine;
use crate::internals::{committer::CommitData, StreamState};

use processor::HandlerSlot;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

pub struct Worker;
impl Worker {
    pub fn new<H: BatchHandler>(
        handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        assignment: WorkerAssignment,
    ) -> SleepingWorker<H> {
        SleepingWorker {
            handler_slot: HandlerSlot::new(handler_factory, assignment, Duration::from_secs(60)),
        }
    }
}

pub struct SleepingWorker<H> {
    handler_slot: HandlerSlot<H>,
}

impl<H> SleepingWorker<H>
where
    H: BatchHandler,
{
    pub fn start(
        self,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
    ) -> ActiveWorker<H> {
        let SleepingWorker { handler_slot } = self;
        let queue = Arc::new(SegQueue::new());

        let join_handle = processor::Processor::start(
            Arc::clone(&queue),
            handler_slot,
            stream_state,
            committer,
            Duration::from_millis(100),
        );

        ActiveWorker { queue, join_handle }
    }

    pub fn assignment(&self) -> &WorkerAssignment {
        &self.handler_slot.assignment
    }

    pub fn tick(&mut self) {
        self.handler_slot.tick();
    }
}

pub struct ActiveWorker<H> {
    queue: Arc<SegQueue<BatchLine>>,
    join_handle: JoinHandle<Result<HandlerSlot<H>, ConsumerError>>,
}

impl<H: BatchHandler> ActiveWorker<H> {
    pub fn process(&self, batch: BatchLine) {
        self.queue.push(batch);
    }

    pub async fn join(self) -> Result<SleepingWorker<H>, ConsumerError> {
        let ActiveWorker { join_handle, .. } = self;

        let handler_slot = join_handle.await??;

        Ok(SleepingWorker { handler_slot })
    }
}

mod processor {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use crossbeam::queue::SegQueue;
    use tokio::{self, sync::mpsc::UnboundedSender, task::JoinHandle, time::delay_for};

    use crate::nakadi_types::{model::subscription::SubscriptionCursor, GenericError};

    use crate::consumer::{ConsumerError, ConsumerErrorKind};
    use crate::event_handler::{BatchHandler, BatchHandlerFactory, BatchMeta, BatchPostAction};
    use crate::event_stream::BatchLine;
    use crate::internals::{committer::CommitData, StreamState};

    use super::WorkerAssignment;

    pub struct Processor<H> {
        queue: Arc<SegQueue<BatchLine>>,
        handler_slot: HandlerSlot<H>,
        stream_state: StreamState,
        committer: UnboundedSender<CommitData>,
    }

    impl<H> Processor<H>
    where
        H: BatchHandler,
    {
        pub fn start(
            queue: Arc<SegQueue<BatchLine>>,
            handler_slot: HandlerSlot<H>,
            stream_state: StreamState,
            committer: UnboundedSender<CommitData>,
            tick_interval: Duration,
        ) -> JoinHandle<Result<HandlerSlot<H>, ConsumerError>> {
            let mut processor = Processor {
                handler_slot,
                queue,
                stream_state,
                committer,
            };

            let processor_loop = async move {
                loop {
                    if processor.stream_state.cancellation_requested() {
                        break;
                    }

                    match processor.process_queue().await {
                        Ok(true) => {}
                        Ok(false) => {
                            processor.handler_slot.tick();
                            delay_for(Duration::from_millis(100)).await
                        }
                        Err(err) => {
                            processor.stream_state.request_global_cancellation();
                            return Err(err);
                        }
                    }
                }
                Ok(processor.handler_slot)
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
        pub async fn process_queue(&mut self) -> Result<bool, ConsumerError> {
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

                match self.handler_slot.process_batch(events, meta).await? {
                    BatchPostAction::Commit { n_events } => {
                        let commit_data = CommitData {
                            cursor,
                            received_at,
                            batch_id,
                            events_hint: n_events,
                        };
                        if let Err(err) = self.try_commit(commit_data) {
                            self.stream_state.request_stream_cancellation();
                            break;
                        }
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
            }
            Ok(processed_at_least_one_event)
        }

        fn try_commit(&mut self, commit_data: CommitData) -> Result<(), GenericError> {
            if let Err(err) = self.committer.send(commit_data) {
                return Err(GenericError::new(format!(
                    "failed to enqueue commit data '{}'",
                    err
                )));
            }

            Ok(())
        }
    }

    pub struct HandlerSlot<H> {
        pub handler: Option<H>,
        pub handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
        pub last_event_processed: Instant,
        pub assignment: WorkerAssignment,
        pub inactivity_after: Duration,
    }

    impl<H: BatchHandler> HandlerSlot<H> {
        pub fn new(
            handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
            assignment: WorkerAssignment,
            inactivity_after: Duration,
        ) -> Self {
            Self {
                handler: None,
                handler_factory,
                last_event_processed: Instant::now(),
                assignment,
                inactivity_after,
            }
        }

        async fn process_batch<'a>(
            &mut self,
            events: Bytes,
            meta: BatchMeta<'a>,
        ) -> Result<BatchPostAction, ConsumerError> {
            self.last_event_processed = Instant::now();
            let handler = self.get_handler().await?;
            Ok(handler.handle(events, meta).await)
        }

        async fn get_handler(&mut self) -> Result<&mut H, ConsumerError> {
            if self.handler.is_none() {
                let new_handler = self
                    .handler_factory
                    .handler(self.assignment.handler_assignment())
                    .await
                    .map_err(|err| {
                        ConsumerError::from(err).with_kind(ConsumerErrorKind::HandlerFactory)
                    })?;

                self.handler = Some(new_handler);
            }

            Ok(self.handler.as_mut().unwrap())
        }

        pub fn tick(&mut self) {
            if let Some(handler) = self.handler.take() {
                if self.last_event_processed.elapsed() > self.inactivity_after
                    && handler
                        .on_inactivity_detected(self.last_event_processed)
                        .should_stay_alive()
                {
                    self.handler = Some(handler)
                }
            }
        }
    }
}
