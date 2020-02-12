use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use tokio::{sync::mpsc::unbounded_channel, time::interval};

use crate::api::{BytesStream, NakadionEssentials, SubscriptionCommitApi};
use crate::consumer::{Config, ConsumerError, ConsumerErrorKind};
use crate::event_handler::{BatchHandler, BatchHandlerFactory};
use crate::event_stream::{BatchLineErrorKind, BatchLineStream, FramedStream};
use crate::internals::dispatcher::{Dispatcher, DispatcherMessage, SleepingDispatcher};

use super::{ConsumerState, StreamState};

#[derive(Clone)]
pub(crate) struct Controller<H, C> {
    params: ControllerParams<H, C>,
}

impl<H, C> Controller<H, C>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
{
    pub(crate) fn new(params: ControllerParams<H, C>) -> Self {
        Self { params }
    }

    pub(crate) async fn start(self) -> Result<(), ConsumerError> {
        let params = self.params;

        params.consumer_state.logger.debug(format_args!(
            "Commit strategy: {:?}",
            params.consumer_state.config()
        ));

        create_background_task(params).await
    }
}

async fn create_background_task<H, C>(
    mut params: ControllerParams<H, C>,
) -> Result<(), ConsumerError>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
{
    let mut sleeping_dispatcher = Dispatcher::sleeping(
        params.config().dispatch_strategy.clone(),
        Arc::clone(&params.handler_factory),
        params.api_client.clone(),
    );

    loop {
        let shared_sleeping_dispatcher = Arc::new(Mutex::new(sleeping_dispatcher));
        let (stream_id, bytes_stream) = match connect_stream::connect_with_retries(
            params.api_client.clone(),
            params.consumer_state.clone(),
            Arc::clone(&shared_sleeping_dispatcher),
        )
        .await
        {
            Ok(stream) => stream.parts(),
            Err(err) => {
                return Err(err);
            }
        };

        sleeping_dispatcher =
            if let Ok(sleeping_dispatcher) = Arc::try_unwrap(shared_sleeping_dispatcher) {
                sleeping_dispatcher.into_inner().unwrap()
            } else {
                params.consumer_state.request_global_cancellation();
                return Err(ConsumerError::new_with_message(
                    ConsumerErrorKind::Internal,
                    "THIS IS A BUG! Could not get a hand on the sleeping dispatcher. \
                    There are multiple references!",
                ));
            };

        let stream_state = params.consumer_state.stream_state(stream_id);
        let (returned_params, returned_dispatcher) =
            consume_stream(params, stream_state, bytes_stream, sleeping_dispatcher).await?;

        sleeping_dispatcher = returned_dispatcher;
        params = returned_params;

        if params.consumer_state.global_cancellation_requested() {
            return Err(ConsumerError::new(ConsumerErrorKind::UserAbort));
        }
    }
}

async fn consume_stream<H, C>(
    params: ControllerParams<H, C>,
    stream_state: StreamState,
    bytes_stream: BytesStream,
    dispatcher: SleepingDispatcher<H, C>,
) -> Result<(ControllerParams<H, C>, SleepingDispatcher<H, C>), ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    H: BatchHandler,
{
    let frame_stream = FramedStream::new(bytes_stream);
    let batch_stream = BatchLineStream::new(frame_stream).map_ok(DispatcherMessage::Batch);

    let ticker = interval(stream_state.config().tick_interval.into_duration())
        .map(|_| Ok(DispatcherMessage::Tick));

    let merged = stream::select(batch_stream, ticker);

    let (batch_lines_sink, batch_lines_receiver) = unbounded_channel::<DispatcherMessage>();

    let active_dispatcher = dispatcher.start(stream_state.clone(), batch_lines_receiver);

    let stream_dead_timeout = stream_state
        .config()
        .stream_dead_timeout
        .map(|t| t.into_duration());
    let mut last_batch_received = Instant::now();
    pin_mut!(merged);
    while let Some(msg) = merged.next().await {
        let dispatcher_message = match msg {
            Ok(msg) => msg,
            Err(batch_line_error) => match batch_line_error.kind() {
                BatchLineErrorKind::Parser => return Err(ConsumerErrorKind::InvalidBatch.into()),
                BatchLineErrorKind::Io => {
                    stream_state.request_stream_cancellation();
                    break;
                }
            },
        };

        let msg_for_dispatcher = match dispatcher_message {
            DispatcherMessage::Tick => {
                if let Some(stream_dead_timeout) = stream_dead_timeout {
                    let elapsed = last_batch_received.elapsed();
                    if elapsed > stream_dead_timeout {
                        stream_state
                            .logger
                            .warn(format_args!("Stream dead after {:?}", elapsed));
                        break;
                    }
                }

                DispatcherMessage::Tick
            }
            DispatcherMessage::Batch(batch) => {
                last_batch_received = Instant::now();
                if let Some(info_str) = batch.info_str() {
                    stream_state
                        .logger
                        .info(format_args!("Received info line: {}", info_str));
                }

                if batch.is_keep_alive_line() {
                    continue;
                } else {
                    DispatcherMessage::Batch(batch)
                }
            }
        };

        if batch_lines_sink.send(msg_for_dispatcher).is_err() {
            stream_state.request_stream_cancellation();
            break;
        }
    }

    let mut sleeping_dispatcher = active_dispatcher.join().await?;

    sleeping_dispatcher.tick();

    stream_state
        .logger()
        .info(format_args!("Streaming stopped"));

    Ok((params, sleeping_dispatcher))
}

pub(crate) struct ControllerParams<H, C> {
    pub api_client: C,
    pub consumer_state: ConsumerState,
    pub handler_factory: Arc<dyn BatchHandlerFactory<Handler = H>>,
}

impl<H, C> ControllerParams<H, C> {
    pub fn config(&self) -> &Config {
        &self.consumer_state.config()
    }
}

impl<H, C> Clone for ControllerParams<H, C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            api_client: self.api_client.clone(),
            consumer_state: self.consumer_state.clone(),
            handler_factory: Arc::clone(&self.handler_factory),
        }
    }
}

mod connect_stream {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use http::status::StatusCode;
    use tokio::time::delay_for;

    use crate::nakadi_types::{
        model::subscription::{StreamParameters, SubscriptionId},
        FlowId,
    };

    use crate::api::{NakadiApiError, SubscriptionStream, SubscriptionStreamApi};
    use crate::consumer::{ConsumerError, ConsumerErrorKind};
    use crate::event_handler::BatchHandler;
    use crate::internals::dispatcher::SleepingDispatcher;

    use crate::internals::ConsumerState;

    pub(crate) async fn connect_with_retries<C: SubscriptionStreamApi, H: BatchHandler>(
        api_client: C,
        consumer_state: ConsumerState,
        sleeping_dispatcher: Arc<Mutex<SleepingDispatcher<H, C>>>,
    ) -> Result<SubscriptionStream, ConsumerError> {
        let config = consumer_state.config();
        let max_retry_delay = config.connect_retry_max_delay.into_inner();
        let mut current_retry_delay = 0;
        loop {
            sleeping_dispatcher.lock().unwrap().tick();

            if current_retry_delay < max_retry_delay {
                current_retry_delay += 1;
            }
            if consumer_state.global_cancellation_requested() {
                return Err(ConsumerError::new(ConsumerErrorKind::UserAbort));
            }

            match connect(
                &api_client,
                consumer_state.subscription_id(),
                consumer_state.stream_parameters(),
            )
            .await
            {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    if let Some(status) = err.status() {
                        match status {
                            StatusCode::NOT_FOUND => {
                                if config.abort_connect_on_subscription_not_found.into() {
                                    return Err(ConsumerError::new(
                                        ConsumerErrorKind::SubscriptionNotFound,
                                    )
                                    .with_source(err));
                                }
                            }
                            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                                if config.abort_connect_on_auth_error.into() {
                                    return Err(ConsumerError::new(
                                        ConsumerErrorKind::AccessDenied,
                                    )
                                    .with_source(err));
                                }
                            }
                            StatusCode::BAD_REQUEST => {
                                return Err(ConsumerError::new(ConsumerErrorKind::Internal)
                                    .with_source(err));
                            }
                            _ => {}
                        }
                        consumer_state
                            .logger()
                            .warn(format_args!("Failed to connect to Nakadi: {}", err));
                    } else {
                        consumer_state
                            .logger()
                            .warn(format_args!("Failed to connect to Nakadi: {}", err));
                    }
                    if current_retry_delay != 0 {
                        delay_for(Duration::from_secs(current_retry_delay)).await;
                    }
                    continue;
                }
            }
        }
    }

    async fn connect<C: SubscriptionStreamApi>(
        client: &C,
        subscription_id: SubscriptionId,
        stream_params: &StreamParameters,
    ) -> Result<SubscriptionStream, NakadiApiError> {
        client
            .request_stream(subscription_id, stream_params, FlowId::default())
            .await
    }
}
