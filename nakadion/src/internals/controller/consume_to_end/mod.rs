use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use tokio::{self, sync::mpsc::UnboundedSender};

use crate::api::SubscriptionCommitApi;
use crate::components::streams::{BatchLineError, BatchLineErrorKind};
use crate::consumer::{ConsumerError, ConsumerErrorKind, StreamDeadPolicy};
use crate::instrumentation::Instruments;
use crate::internals::{
    dispatcher::{ActiveDispatcher, DispatcherMessage, SleepingDispatcher},
    EnrichedErr, EnrichedOk, StreamState,
};
use crate::logging::Logger;
use crate::Error;

use super::*;
use partition_tracker::PartitionTracker;

mod partition_tracker;

/// Wakes up the infrastructure and then consumes the stream until it ends
/// or the consumption is aborted.
///
/// An error returned here means that we abort the `Consumer`
pub(crate) async fn consume_stream_to_end<C, S>(
    stream: S,
    active_dispatcher: ActiveDispatcher<'static, C>,
    mut dispatcher_sink: UnboundedSender<DispatcherMessage>,
    stream_state: StreamState,
) -> Result<SleepingDispatcher<C>, ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send + 'static,
{
    let instrumentation = stream_state.instrumentation();

    let mut controller_state = ControllerState::new(stream_state.clone());

    let mut stream = stream.boxed();

    let loop_result: Result<(), ConsumerError> = loop {
        if stream_state.cancellation_requested() {
            stream_state.debug(format_args!(
                "Stream was cancelled on request - shutting down stream"
            ));
            break Ok(());
        }

        match stream.next().await {
            None => {
                break Ok(());
            }
            Some(Ok(BatchLineMessage::NakadiStreamEnded)) => {
                break Ok(());
            }
            Some(Ok(BatchLineMessage::BatchLine(batch_line))) => {
                if let Err(err) = handle_batch_line(
                    batch_line,
                    &stream_state,
                    &mut controller_state,
                    &mut dispatcher_sink,
                )
                .await
                {
                    stream_state.warn(format_args!("Could not send batch line: {}", err));
                    break Ok(());
                } else {
                    continue;
                }
            }
            Some(Ok(BatchLineMessage::Tick(tick_timestamp))) => {
                if let Err(err) = handle_tick(
                    tick_timestamp,
                    &stream_state,
                    &mut controller_state,
                    &mut dispatcher_sink,
                )
                .await
                {
                    stream_state.warn(format_args!("Could not send tick: {}", err));
                    break Ok(());
                } else {
                    continue;
                }
            }
            Some(Err(batch_line_error)) => {
                instrumentation.stream_error(&batch_line_error);
                match batch_line_error.kind() {
                    BatchLineErrorKind::Parser => {
                        stream_state.error(format_args!(
                            "Aborting consumer - Invalid frame: {}",
                            batch_line_error
                        ));
                        break Err(ConsumerErrorKind::InvalidBatch.into());
                    }
                    BatchLineErrorKind::Io => {
                        stream_state.warn(format_args!(
                            "Aborting stream due to IO error: {}",
                            batch_line_error
                        ));
                        break Ok(());
                    }
                }
            }
        }
    };

    let shut_down_result = shutdown(
        stream,
        active_dispatcher,
        dispatcher_sink,
        stream_state.clone(),
        &controller_state,
    )
    .await;

    match (loop_result, shut_down_result) {
        (Ok(()), Ok(sleeping_dispatcher)) => {
            stream_state.info(format_args!(
                "Streaming infrastructure shut down after {:?}. Will reconnect \
                if consumer was not requested to stop.",
                controller_state.stream_started_at.elapsed(),
            ));
            Ok(sleeping_dispatcher)
        }
        (Ok(()), Err(shutdown_err)) => {
            stream_state.warn(format_args!(
                "Streaming infrastructure shut down after {:?} with an error \
                (causes consumer abort).",
                controller_state.stream_started_at.elapsed(),
            ));
            Err(shutdown_err)
        }
        (Err(loop_err), Ok(_sleeping_dispatcher)) => {
            stream_state.warn(format_args!(
                "Streaming infrastructure shut down after {:?} because there \
                was an unrecoverable error (causes consumer abort).",
                controller_state.stream_started_at.elapsed(),
            ));
            Err(loop_err)
        }
        (Err(loop_err), Err(shutdown_err)) => {
            stream_state.warn(format_args!(
                "Streaming infrastructure shut down after {:?} because there \
            was an unrecoverable error (causes consumer abort). Also the shutdown of \
            the infrastructure caused an error. Error which caused the streaming \
            infrastructure to shut down: {}",
                controller_state.stream_started_at.elapsed(),
                loop_err,
            ));
            // We return the shutdown error because this is most probably an
            // internal error which is more severe.
            Err(shutdown_err)
        }
    }
}

async fn handle_batch_line(
    batch_line: BatchLine,
    stream_state: &StreamState,
    controller_state: &mut ControllerState,
    dispatcher_sink: &mut UnboundedSender<DispatcherMessage>,
) -> Result<(), Error> {
    let frame_received_at = batch_line.received_at();
    let now = Instant::now();
    controller_state.last_frame_received_at = now;

    let event_type_partition = batch_line.to_event_type_partition();
    controller_state
        .partition_tracker
        .activity(&event_type_partition);

    if let Some(info_str) = batch_line.info_str() {
        stream_state
            .instrumentation
            .controller_info_received(frame_received_at);
        stream_state.info(format_args!(
            "Received info line for {}: {}",
            event_type_partition, info_str
        ));
    }

    if batch_line.is_keep_alive_line() {
        stream_state
            .instrumentation
            .controller_keep_alive_received(frame_received_at);
        /*stream_state.debug(format_args!(
            "received keep alive line for {}.",
            event_type_partition
        ));*/
        Ok(())
    } else {
        controller_state.last_events_received_at = now;
        let bytes = batch_line.bytes().len();
        stream_state
            .instrumentation
            .controller_batch_received(frame_received_at, bytes);
        if dispatcher_sink
            .send(DispatcherMessage::BatchWithEvents(
                event_type_partition,
                batch_line,
            ))
            .is_err()
        {
            Err(Error::new("Failed to send batch to dispatcher"))
        } else {
            // Only here we know for sure whether we sent events
            stream_state
                .instrumentation
                .consumer_batches_in_flight_inc();
            controller_state.batches_sent_to_dispatcher += 1;
            Ok(())
        }
    }
}

async fn handle_tick(
    tick_timestamp: Instant,
    stream_state: &StreamState,
    controller_state: &mut ControllerState,
    dispatcher_sink: &mut UnboundedSender<DispatcherMessage>,
) -> Result<(), Error> {
    if controller_state.stream_dead_policy.is_stream_dead(
        controller_state.last_frame_received_at,
        controller_state.last_events_received_at,
    ) {
        return Err(Error::new("The stream is dead boys..."));
    }

    let elapsed = controller_state.last_frame_received_at.elapsed();
    if elapsed >= controller_state.warn_no_frames {
        stream_state.warn(format_args!("No frames for {:?}.", elapsed));
        stream_state
            .instrumentation()
            .controller_no_frames_warning(elapsed);
    }

    let elapsed = controller_state.last_events_received_at.elapsed();
    if elapsed >= controller_state.warn_no_events {
        stream_state.warn(format_args!("No events received for {:?}.", elapsed));
        stream_state
            .instrumentation()
            .controller_no_events_warning(elapsed);
    }

    controller_state
        .partition_tracker
        .check_for_inactivity(Instant::now());

    match dispatcher_sink.send(DispatcherMessage::Tick(tick_timestamp)) {
        Ok(()) => Ok(()),
        Err(_err) => Err(Error::new("Could not send tick to backend.")),
    }
}

async fn shutdown<C, S>(
    stream: S,
    active_dispatcher: ActiveDispatcher<'static, C>,
    dispatcher_sink: UnboundedSender<DispatcherMessage>,
    stream_state: StreamState,
    controller_state: &ControllerState,
) -> Result<SleepingDispatcher<C>, ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send + 'static,
{
    // THIS MUST BEFORE WAITING FOR THE DISPATCHER TO JOIN!!!!
    drop(dispatcher_sink);

    drop(stream);

    stream_state.debug(format_args!(
        "Streaming ending after {:?}. Waiting for stream infrastructure to shut down.",
        controller_state.stream_started_at.elapsed()
    ));

    // Wait for the infrastructure to completely shut down before making further connect attempts for
    // new streams
    let (result, unprocessed_batches) = match active_dispatcher.join().await {
        Ok(EnrichedOk {
            processed_batches,
            payload: sleeping_dispatcher,
        }) => (
            Ok(sleeping_dispatcher),
            Some(controller_state.batches_sent_to_dispatcher - processed_batches),
        ),
        Err(EnrichedErr {
            processed_batches,
            err,
        }) => {
            stream_state.error(format_args!("Shutdown terminated with error: {}", err));
            if let Some(processed_batches) = processed_batches {
                (
                    Err(err),
                    Some(controller_state.batches_sent_to_dispatcher - processed_batches),
                )
            } else {
                (Err(err), None)
            }
        }
    };

    // Check whether there were unprocessed batches to fix the in flight metrics
    if let Some(unprocessed_batches) = unprocessed_batches {
        if unprocessed_batches > 0 {
            stream_state.info(format_args!(
                "There were still {} unprocessed batches in flight",
                unprocessed_batches,
            ));
            stream_state
                .instrumentation
                .consumer_batches_in_flight_dec_by(unprocessed_batches);
        }
    }

    result
}

struct ControllerState {
    /// We might want to abort if we do not receive data from Nakadi in time
    /// This is basically to prevents us from being locked in a dead stream.
    stream_dead_policy: StreamDeadPolicy,
    warn_no_frames: Duration,
    warn_no_events: Duration,

    stream_started_at: Instant,
    last_events_received_at: Instant,
    last_frame_received_at: Instant,
    /// If streaming ends, we use this to correct the in flight metrics
    batches_sent_to_dispatcher: usize,
    partition_tracker: PartitionTracker,
}

impl ControllerState {
    pub fn new(stream_state: StreamState) -> Self {
        let now = Instant::now();
        Self {
            stream_dead_policy: stream_state.config().stream_dead_policy,
            warn_no_frames: stream_state.config().warn_no_frames.into_duration(),
            warn_no_events: stream_state.config().warn_no_events.into_duration(),

            stream_started_at: now,
            last_events_received_at: now,
            last_frame_received_at: now,
            batches_sent_to_dispatcher: 0,
            partition_tracker: PartitionTracker::new(stream_state),
        }
    }
}
