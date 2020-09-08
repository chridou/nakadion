use std::time::Instant;

use futures::{Stream, StreamExt};
use tokio::{self, sync::mpsc::UnboundedSender};

use crate::api::SubscriptionCommitApi;
use crate::components::streams::{EventStreamError, EventStreamErrorKind};
use crate::consumer::{ConsumerError, ConsumerErrorKind};
use crate::instrumentation::Instruments;
use crate::internals::{
    dispatcher::{ActiveDispatcher, DispatcherMessage, SleepingDispatcher},
    ConsumptionResult, StreamState,
};
use crate::logging::Logger;
use crate::Error;

use super::*;

use controller_state::ControllerState;

mod controller_state;

/// Wakes up the infrastructure and then consumes the stream until it ends
/// or the consumption is aborted.
///
/// An error returned here means that we abort the `Consumer`
pub(crate) async fn consume_stream_to_end<C, S>(
    event_stream: S,
    active_dispatcher: ActiveDispatcher<'static, C>,
    mut dispatcher_sink: UnboundedSender<DispatcherMessage>,
    stream_state: StreamState,
) -> Result<SleepingDispatcher<C>, ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<EventStreamMessage, EventStreamError>> + Send + 'static,
{
    let instrumentation = stream_state.instrumentation();

    let mut controller_state = ControllerState::new(stream_state.clone());

    let mut event_stream = event_stream.boxed();

    let loop_result: Result<(), ConsumerError> = loop {
        if stream_state.cancellation_requested() {
            stream_state.debug(format_args!("[CONTROLLER] Cancellation requested."));
            break Ok(());
        }

        match event_stream.next().await {
            None => {
                break Ok(());
            }
            Some(Ok(EventStreamMessage::EventStreamEnded)) => {
                break Ok(());
            }
            Some(Ok(EventStreamMessage::Nakadi(nakadi_message))) => {
                if let Err(err) = handle_nakadi_message(
                    nakadi_message,
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
            Some(Ok(EventStreamMessage::Tick(tick_timestamp))) => {
                if let Err(err) =
                    handle_tick(tick_timestamp, &mut controller_state, &mut dispatcher_sink).await
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
                    EventStreamErrorKind::Parser => {
                        stream_state.error(format_args!(
                            "Aborting consumer - Invalid frame: {}",
                            batch_line_error
                        ));
                        break Err(ConsumerErrorKind::InvalidBatch.into());
                    }
                    EventStreamErrorKind::Io => {
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

    stream_state
        .instrumentation()
        .streaming_ended(controller_state.stream_started_at.elapsed());

    let shut_down_result = shutdown(
        event_stream,
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

async fn handle_nakadi_message(
    nakadi_message: NakadiMessage,
    controller_state: &mut ControllerState,
    dispatcher_sink: &mut UnboundedSender<DispatcherMessage>,
) -> Result<(), Error> {
    match nakadi_message {
        NakadiMessage::Events(batch) => {
            let event_type_partition = batch.to_event_type_partition();

            controller_state.received_frame(&event_type_partition);

            if dispatcher_sink
                .send(DispatcherMessage::BatchWithEvents(
                    event_type_partition,
                    batch,
                ))
                .is_err()
            {
                Err(Error::new("Failed to send batch to dispatcher"))
            } else {
                Ok(())
            }
        }
        NakadiMessage::KeepAlive => {
            controller_state.received_keep_alive();
            Ok(())
        }
    }
}

async fn handle_tick(
    tick_timestamp: Instant,
    controller_state: &mut ControllerState,
    dispatcher_sink: &mut UnboundedSender<DispatcherMessage>,
) -> Result<(), Error> {
    controller_state.received_tick(tick_timestamp)?;

    match dispatcher_sink.send(DispatcherMessage::Tick(tick_timestamp)) {
        Ok(()) => Ok(()),
        Err(_err) => Err(Error::new("Could not send tick to backend.")),
    }
}

async fn shutdown<C, S>(
    event_stream: S,
    active_dispatcher: ActiveDispatcher<'static, C>,
    dispatcher_sink: UnboundedSender<DispatcherMessage>,
    stream_state: StreamState,
    controller_state: &ControllerState,
) -> ConsumptionResult<SleepingDispatcher<C>>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    S: Stream<Item = Result<EventStreamMessage, EventStreamError>> + Send + 'static,
{
    // THIS MUST BE DONE BEFORE WAITING FOR THE DISPATCHER TO JOIN!!!!
    drop(dispatcher_sink);

    drop(event_stream);

    stream_state.debug(format_args!(
        "Streaming ending after {:?}. Waiting for stream infrastructure to shut down. {} uncommitted batches.",
        controller_state.stream_started_at.elapsed(), stream_state.uncommitted_batches()
    ));

    // Wait for the infrastructure to completely shut down before making further connect attempts for
    // new streams
    let result = match active_dispatcher.join().await {
        Ok(sleeping_dispatcher) => Ok(sleeping_dispatcher),
        Err(err) => {
            stream_state.error(format_args!("Shutdown terminated with error: {}", err));
            Err(err)
        }
    };

    if stream_state.stats.is_a_warning() {
        stream_state.warn(format_args!("Unprocessed data: {:?}", stream_state.stats));
    }

    stream_state.reset_in_flight_stats();

    result
}
