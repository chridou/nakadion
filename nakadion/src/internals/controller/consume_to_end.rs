use std::time::Instant;

use futures::{Stream, StreamExt};
use tokio::{self, sync::mpsc::UnboundedSender};

use crate::api::SubscriptionCommitApi;
use crate::components::streams::{BatchLineError, BatchLineErrorKind};
use crate::consumer::{ConsumerError, ConsumerErrorKind};
use crate::instrumentation::Instruments;
use crate::internals::{
    dispatcher::{ActiveDispatcher, DispatcherMessage, SleepingDispatcher},
    EnrichedErr, EnrichedOk, StreamState,
};
use crate::logging::Logger;

use super::*;

/// Wakes up the infrastructure and then consumes the stream until it ends
/// or the consumption is aborted.
///
/// An error returned here means that we abort the `Consumer`
pub(crate) async fn consume_stream_to_end<C, S>(
    stream: S,
    active_dispatcher: ActiveDispatcher<'static, C>,
    batch_lines_sink: UnboundedSender<DispatcherMessage>,
    stream_state: StreamState,
) -> Result<SleepingDispatcher<C>, ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<BatchLineMessage, BatchLineError>> + Send + 'static,
{
    // We might want to abort if we do not receive data from Nakadi in time
    // This is basically to prevents us from being locked in a dead stream.
    let stream_dead_policy = stream_state.config().stream_dead_policy;
    // From time to time, if we do not receive data from Nakadi we want to emit warnings
    let warn_no_frames = stream_state.config().warn_no_frames.into_duration();
    let warn_no_events = stream_state.config().warn_no_events.into_duration();

    let now = Instant::now();
    let stream_started_at = now;
    let mut last_events_received_at = now;
    let mut last_frame_received_at = now;

    // If streaming ends, we use this to correct the in flight metrics
    let mut batches_sent_to_dispatcher = 0usize;
    let instrumentation = stream_state.instrumentation();

    let mut stream = stream.boxed();

    // Track the activity of partitions on event types to collect metrics
    // on active (used) partitions
    let mut partition_tracker = partition_tracker::PartitionTracker::new(stream_state.clone());

    while let Some(batch_line_message_or_err) = stream.next().await {
        if stream_state.cancellation_requested() {
            stream_state.debug(format_args!(
                "Stream was cancelled by request - shutting down stream"
            ));
            break;
        }

        let batch_line_message = match batch_line_message_or_err {
            Ok(msg) => msg,
            Err(batch_line_error) => {
                instrumentation.stream_error(&batch_line_error);
                match batch_line_error.kind() {
                    BatchLineErrorKind::Parser => {
                        stream_state.error(format_args!(
                            "Aborting consumer - Invalid frame: {}",
                            batch_line_error
                        ));
                        return Err(ConsumerErrorKind::InvalidBatch.into());
                    }
                    BatchLineErrorKind::Io => {
                        stream_state.warn(format_args!(
                            "Aborting stream due to IO error: {}",
                            batch_line_error
                        ));
                        break;
                    }
                }
            }
        };

        let msg_for_dispatcher = match batch_line_message {
            BatchLineMessage::NakadiStreamEnded => {
                break;
            }
            BatchLineMessage::Tick(timestamp) => {
                if stream_dead_policy
                    .is_stream_dead(last_frame_received_at, last_events_received_at)
                {
                    stream_state.warn(format_args!("The stream is dead boys..."));
                    break;
                }

                let elapsed = last_frame_received_at.elapsed();
                if elapsed >= warn_no_frames {
                    stream_state.warn(format_args!("No frames for {:?}.", elapsed));
                    stream_state
                        .instrumentation()
                        .controller_no_frames_warning(elapsed);
                }

                let elapsed = last_events_received_at.elapsed();
                if elapsed >= warn_no_events {
                    stream_state.warn(format_args!("No events received for {:?}.", elapsed));
                    stream_state
                        .instrumentation()
                        .controller_no_events_warning(elapsed);
                }

                partition_tracker.check_for_inactivity(Instant::now());
                DispatcherMessage::Tick(timestamp)
            }
            BatchLineMessage::BatchLine(batch) => {
                let frame_received_at = batch.received_at();
                let now = Instant::now();
                last_frame_received_at = now;

                let event_type_partition = batch.to_event_type_partition();
                partition_tracker.activity(&event_type_partition);

                if let Some(info_str) = batch.info_str() {
                    instrumentation.controller_info_received(frame_received_at);
                    stream_state.info(format_args!(
                        "Received info line for {}: {}",
                        event_type_partition, info_str
                    ));
                }

                if batch.is_keep_alive_line() {
                    instrumentation.controller_keep_alive_received(frame_received_at);
                    stream_state.debug(format_args!(
                        "received keep alive line for {}.",
                        event_type_partition
                    ));
                    continue;
                } else {
                    if batch.has_events() {
                        last_events_received_at = now;
                    }
                    let bytes = batch.bytes().len();
                    instrumentation.controller_batch_received(frame_received_at, bytes);
                    DispatcherMessage::BatchWithEvents(event_type_partition, batch)
                }
            }
        };

        let was_batch_with_events = msg_for_dispatcher.is_batch_with_events();
        if batch_lines_sink.send(msg_for_dispatcher).is_err() {
            stream_state.warn(format_args!("Failed to send batch to dispatcher"));
            stream_state.request_stream_cancellation();
            break;
        } else if was_batch_with_events {
            // Only here we know for sure whether we sent events
            instrumentation.consumer_batches_in_flight_inc();
            batches_sent_to_dispatcher += 1;
        }
    }

    // THIS MUST BEFORE WAITING FOR THE DISPATCHER TO JOIN!!!!
    drop(batch_lines_sink);

    stream_state.debug(format_args!(
        "Streaming ending after {:?}. Waiting for stream infrastructure to shut down.",
        stream_started_at.elapsed()
    ));

    drop(stream);
    drop(partition_tracker);

    // Wait for the infrastructure to completely shut down before making further connect attempts for
    // new streams
    let (result, unprocessed_batches) = match active_dispatcher.join().await {
        Ok(EnrichedOk {
            processed_batches,
            payload: sleeping_dispatcher,
        }) => (
            Ok(sleeping_dispatcher),
            Some(batches_sent_to_dispatcher - processed_batches),
        ),
        Err(EnrichedErr {
            processed_batches,
            err,
        }) => {
            if let Some(processed_batches) = processed_batches {
                (
                    Err(err),
                    Some(batches_sent_to_dispatcher - processed_batches),
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
            instrumentation.consumer_batches_in_flight_dec_by(unprocessed_batches);
        }
    }

    stream_state.info(format_args!(
        "Streaming stopped after {:?}",
        stream_started_at.elapsed(),
    ));

    result
}
