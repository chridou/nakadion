//! The worker creates handlers and processes batches (events) on them.
//!
//! Each worker is responsible for a single handler and has
//! a predefined `HandlerAssignment` which is passed to the
//! handler factory to create the correct worker.
use std::sync::Arc;
use std::time::Instant;

use futures::Stream;
use tokio::{self, task::JoinHandle};

use crate::components::{committer::CommitHandle, streams::BatchLine};
use crate::consumer::HandlerInactivityTimeoutSecs;
use crate::handler::{BatchHandlerFactory, HandlerAssignment};
use crate::internals::{EnrichedErr, EnrichedResult, StreamState};

use processor::HandlerSlot;

#[derive(Debug)]
pub enum WorkerMessage {
    BatchWithEvents(BatchLine),
    Tick(Instant),
}

pub struct Worker;
impl Worker {
    pub(crate) fn sleeping(
        handler_factory: Arc<dyn BatchHandlerFactory>,
        assignment: HandlerAssignment,
        inactivity_after: HandlerInactivityTimeoutSecs,
    ) -> SleepingWorker {
        SleepingWorker {
            handler_slot: HandlerSlot::new(handler_factory, assignment, inactivity_after),
        }
    }
}

pub(crate) struct SleepingWorker {
    handler_slot: HandlerSlot,
}

impl SleepingWorker {
    pub fn start<S>(
        self,
        stream_state: StreamState,
        committer: CommitHandle,
        batches: S,
    ) -> ActiveWorker
    where
        S: Stream<Item = WorkerMessage> + Send + 'static,
    {
        let SleepingWorker { mut handler_slot } = self;

        handler_slot.set_logger(stream_state.logger());

        let join_handle = processor::start(batches, handler_slot, stream_state, committer);

        ActiveWorker { join_handle }
    }

    pub fn tick(&mut self) {
        self.handler_slot.tick();
    }
}

pub(crate) struct ActiveWorker {
    join_handle: JoinHandle<EnrichedResult<HandlerSlot>>,
}

impl ActiveWorker {
    pub async fn join(self) -> EnrichedResult<SleepingWorker> {
        let ActiveWorker { join_handle, .. } = self;

        let mut handler_slot_enriched = match join_handle.await {
            Ok(r) => r?,
            Err(join_err) => return Err(EnrichedErr::no_data(join_err)),
        };

        handler_slot_enriched.payload.reset();

        Ok(handler_slot_enriched.map(|handler_slot| SleepingWorker { handler_slot }))
    }
}

mod processor {
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::{pin_mut, Stream, StreamExt};
    use tokio::{self, task::JoinHandle};

    use crate::nakadi_types::subscription::SubscriptionCursor;

    use crate::components::{committer::CommitHandle, streams::BatchLine};
    use crate::consumer::{ConsumerError, ConsumerErrorKind, HandlerInactivityTimeoutSecs};
    use crate::handler::{
        BatchHandler, BatchHandlerFactory, BatchMeta, BatchPostAction, BatchStats,
        HandlerAssignment,
    };
    use crate::instrumentation::Instruments;
    use crate::internals::{EnrichedOk, EnrichedResult, StreamState};
    use crate::logging::{ContextualLogger, Logger};

    use super::WorkerMessage;

    /// Starts this worker.
    ///
    /// The worker will run as a task which has to be joined.
    pub(crate) fn start<S>(
        batches: S,
        mut handler_slot: HandlerSlot,
        stream_state: StreamState,
        committer: CommitHandle,
    ) -> JoinHandle<EnrichedResult<HandlerSlot>>
    where
        S: Stream<Item = WorkerMessage> + Send + 'static,
    {
        let mut batches_processed: usize = 0;
        handler_slot.set_logger(&stream_state.logger());
        let processor_loop = async move {
            let mut processing_compound = ProcessingCompound {
                handler_slot,
                stream_state: stream_state.clone(),
                committer,
            };

            pin_mut!(batches);
            while let Some(msg) = batches.next().await {
                if stream_state.cancellation_requested() {
                    break;
                }

                let batch = match msg {
                    WorkerMessage::BatchWithEvents(batch) => batch,
                    WorkerMessage::Tick(_) => {
                        processing_compound.handler_slot.tick();
                        continue;
                    }
                };

                match processing_compound.process_batch_line(batch).await {
                    Ok(true) => {
                        stream_state.instrumentation().batches_in_flight_dec();
                        batches_processed += 1;
                    }
                    Ok(false) => {
                        stream_state.instrumentation().batches_in_flight_dec();
                        batches_processed += 1;
                        break;
                    }
                    Err(err) => {
                        return Err(err.enriched(batches_processed));
                    }
                }
            }

            drop(batches);

            stream_state.debug(format_args!("Worker stopped"));

            Ok(EnrichedOk::new(
                processing_compound.handler_slot,
                batches_processed,
            ))
        };

        tokio::spawn(processor_loop)
    }

    pub struct ProcessingCompound {
        handler_slot: HandlerSlot,
        stream_state: StreamState,
        committer: CommitHandle,
    }

    impl ProcessingCompound {
        /// If this function returns with an error, treat is as a critical
        /// error that aborts the whole consumption
        ///
        /// Returns `false` if the stream was cancelled
        pub async fn process_batch_line(
            &mut self,
            batch: BatchLine,
        ) -> Result<bool, ConsumerError> {
            let events = if let Some(events) = batch.events_bytes() {
                events
            } else {
                // We do not expect info or keep alive lines here
                return Ok(true);
            };

            let frame_received_at = batch.received_at();
            let frame_id = batch.frame_id();
            let cursor = batch.cursor_deserialized::<SubscriptionCursor>()?;

            let meta = BatchMeta {
                stream_id: self.stream_state.stream_id(),
                cursor: &cursor,
                received_at: frame_received_at,
                frame_id,
            };

            let n_events_bytes = events.len();
            let batch_processing_started_at = Instant::now();
            self.stream_state
                .instrumentation()
                .batch_processing_started();
            match self.handler_slot.process_batch(events, meta).await? {
                BatchPostAction::Commit(BatchStats {
                    n_events,
                    t_deserialize,
                }) => {
                    self.report_processed_stats(
                        n_events_bytes,
                        n_events,
                        batch_processing_started_at,
                        frame_received_at,
                    );
                    if let Some(t_deserialize) = t_deserialize {
                        self.stream_state
                            .instrumentation
                            .batch_deserialized(n_events_bytes, t_deserialize);
                    }

                    if let Err(err) = self.committer.commit(cursor, frame_received_at, n_events) {
                        self.stream_state
                            .error(format_args!("Failed to enqueue commit data: {:?}", err));
                        self.stream_state.request_stream_cancellation();
                    }
                    Ok(true)
                }
                BatchPostAction::DoNotCommit(BatchStats {
                    n_events,
                    t_deserialize,
                }) => {
                    self.report_processed_stats(
                        n_events_bytes,
                        n_events,
                        batch_processing_started_at,
                        frame_received_at,
                    );
                    if let Some(t_deserialize) = t_deserialize {
                        self.stream_state
                            .instrumentation
                            .batch_deserialized(n_events_bytes, t_deserialize);
                    }

                    Ok(true)
                }
                BatchPostAction::AbortStream(reason) => {
                    self.report_processed_stats(
                        n_events_bytes,
                        None,
                        batch_processing_started_at,
                        frame_received_at,
                    );

                    self.stream_state.warn(format_args!(
                        "Stream cancellation requested by handler: {}",
                        reason
                    ));
                    self.stream_state.request_stream_cancellation();
                    Ok(false)
                }
                BatchPostAction::ShutDown(reason) => {
                    self.report_processed_stats(
                        n_events_bytes,
                        None,
                        batch_processing_started_at,
                        frame_received_at,
                    );

                    self.stream_state.warn(format_args!(
                        "Consumer shut down requested by handler: {}",
                        reason
                    ));
                    let err =
                        ConsumerError::new(ConsumerErrorKind::HandlerAbort).with_message(reason);
                    Err(err)
                }
            }
        }

        fn report_processed_stats(
            &self,
            n_events_bytes: usize,
            n_events: Option<usize>,
            batch_processing_started_at: Instant,
            frame_received_at: Instant,
        ) {
            self.stream_state.instrumentation.batch_processed(
                frame_received_at,
                n_events_bytes,
                batch_processing_started_at.elapsed(),
            );
            if let Some(n_events) = n_events {
                self.stream_state
                    .instrumentation
                    .batch_processed_n_events(n_events);
            }
        }
    }

    pub(crate) struct HandlerSlot {
        pub handler: Option<Box<dyn BatchHandler>>,
        pub handler_factory: Arc<dyn BatchHandlerFactory>,
        pub last_event_processed: Instant,
        pub assignment: HandlerAssignment,
        pub inactivity_after: HandlerInactivityTimeoutSecs,
        pub notified_on_inactivity: bool,
        pub logger: Option<ContextualLogger>,
    }

    impl HandlerSlot {
        pub fn new(
            handler_factory: Arc<dyn BatchHandlerFactory>,
            assignment: HandlerAssignment,
            inactivity_after: HandlerInactivityTimeoutSecs,
        ) -> Self {
            Self {
                handler: None,
                handler_factory,
                last_event_processed: Instant::now(),
                assignment,
                inactivity_after,
                notified_on_inactivity: false,
                logger: None,
            }
        }

        async fn process_batch(
            &mut self,
            events: Bytes,
            meta: BatchMeta<'_>,
        ) -> Result<BatchPostAction, ConsumerError> {
            self.last_event_processed = Instant::now();
            self.notified_on_inactivity = false;
            let handler = self.get_handler().await?;
            Ok(handler.handle(events, meta).await)
        }

        async fn get_handler(&mut self) -> Result<&mut dyn BatchHandler, ConsumerError> {
            if self.handler.is_none() {
                let new_handler = self
                    .handler_factory
                    .handler(&self.assignment)
                    .await
                    .map_err(|err| {
                        ConsumerError::from(err).with_kind(ConsumerErrorKind::HandlerFactory)
                    })?;
                self.with_logger(|l| {
                    l.info(format_args!(
                        "Created handler for assignment {}",
                        self.assignment
                    ))
                });
                self.handler = Some(new_handler);
            }

            Ok(self.unwrap_handler())
        }

        fn unwrap_handler(&mut self) -> &mut dyn BatchHandler {
            self.handler
                .as_mut()
                .map(|h| h.as_mut() as &mut (dyn BatchHandler + 'static))
                .unwrap()
        }

        pub fn tick(&mut self) {
            if let Some(mut handler) = self.handler.take() {
                let elapsed = self.last_event_processed.elapsed();
                if elapsed > self.inactivity_after.into_duration() && !self.notified_on_inactivity {
                    if handler
                        .on_inactive(elapsed, self.last_event_processed)
                        .should_stay_alive()
                    {
                        self.notified_on_inactivity = true;
                        self.handler = Some(handler);
                        self.with_logger(|l| {
                            l.info(format_args!(
                                "Keeping inactive handler \
                                for assignment {} alive.",
                                self.assignment
                            ))
                        });
                    } else {
                        self.with_logger(|l| {
                            l.info(format_args!(
                                "Killed inactive handler \
                            for assignment {}.",
                                self.assignment
                            ))
                        });
                    }
                } else {
                    self.handler = Some(handler);
                }
            }
        }

        pub fn with_logger<F>(&self, f: F)
        where
            F: Fn(&ContextualLogger),
        {
            if let Some(ref logger) = self.logger {
                f(logger)
            }
        }

        pub fn set_logger(&mut self, logger: &ContextualLogger) {
            let logger = match self.assignment.event_type_and_partition() {
                (Some(e), Some(p)) => logger.event_type(e.clone()).partition_id(p.clone()),
                (Some(e), None) => logger.event_type(e.clone()),
                (None, Some(p)) => logger.partition_id(p.clone()),
                (None, None) => logger.clone(),
            };
            self.logger = Some(logger)
        }

        pub fn reset(&mut self) {
            self.tick();
            self.logger = None
        }
    }
}
