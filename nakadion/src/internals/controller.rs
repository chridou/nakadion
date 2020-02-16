use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use tokio::{
    self,
    sync::mpsc::unbounded_channel,
    time::{delay_for, interval_at},
};

use crate::api::{BytesStream, NakadionEssentials, SubscriptionCommitApi};
use crate::consumer::{Config, ConsumerError, ConsumerErrorKind};
use crate::event_stream::{BatchLine, BatchLineErrorKind, BatchLineStream, FramedStream};
use crate::handler::{BatchHandler, BatchHandlerFactory};
use crate::internals::dispatcher::{
    ActiveDispatcher, Dispatcher, DispatcherMessage, SleepingDispatcher,
};
use crate::logging::Logs;

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
        create_background_task(self.params).await
    }
}

async fn create_background_task<H, C>(
    mut params: ControllerParams<H, C>,
) -> Result<(), ConsumerError>
where
    C: NakadionEssentials + Clone,
    H: BatchHandler,
{
    let consumer_state = params.consumer_state.clone();
    let mut sleeping_dispatcher = Dispatcher::sleeping(
        params.config().dispatch_strategy.clone(),
        Arc::clone(&params.handler_factory),
        params.api_client.clone(),
        params.consumer_state.config().clone(),
    );

    loop {
        let sleep_ticker = SleepTicker::start(sleeping_dispatcher, consumer_state.clone());
        let (stream_id, bytes_stream) = match connect_stream::connect_with_retries(
            params.api_client.clone(),
            params.consumer_state.clone(),
        )
        .await
        {
            Ok(stream) => stream.parts(),
            Err(err) => {
                return Err(err);
            }
        };

        consumer_state
            .logger()
            .info(format_args!("Connected to stream {}.", stream_id));

        let stream_state = consumer_state.stream_state(stream_id);
        let (returned_params, returned_dispatcher) =
            consume_stream(params, stream_state, bytes_stream, sleep_ticker).await?;

        sleeping_dispatcher = returned_dispatcher;
        params = returned_params;

        if params.consumer_state.global_cancellation_requested() {
            return Err(ConsumerError::new(ConsumerErrorKind::UserAbort));
        }
    }
}

enum BatchLineMessage {
    BatchLine(BatchLine),
    Tick,
}

async fn consume_stream<H, C>(
    params: ControllerParams<H, C>,
    stream_state: StreamState,
    bytes_stream: BytesStream,
    sleep_ticker: SleepTicker<H, C>,
) -> Result<(ControllerParams<H, C>, SleepingDispatcher<H, C>), ConsumerError>
where
    C: SubscriptionCommitApi + Clone + Send + Sync + 'static,
    H: BatchHandler,
{
    let frame_stream = FramedStream::new(bytes_stream);
    let batch_stream = BatchLineStream::new(frame_stream).map_ok(BatchLineMessage::BatchLine);

    let tick_interval = stream_state.config().tick_interval.duration();
    let ticker = interval_at((Instant::now() + tick_interval).into(), tick_interval)
        .map(|_| Ok(BatchLineMessage::Tick));

    let merged = stream::select(batch_stream, ticker);

    let (batch_lines_sink, batch_lines_receiver) = unbounded_channel::<DispatcherMessage>();

    let stream_dead_timeout = stream_state
        .config()
        .stream_dead_timeout
        .map(|t| t.duration());

    let mut last_batch_received = Instant::now();
    pin_mut!(merged);

    let (active_dispatcher, first_line) = loop {
        if let Some(next) = merged.next().await {
            match next {
                Ok(BatchLineMessage::BatchLine(line)) => {
                    stream_state.info(format_args!("Received first batch line from Nakadi."));
                    last_batch_received = Instant::now();
                    let sleeping_dispatcher = sleep_ticker.join().await?;
                    let active_dispatcher =
                        sleeping_dispatcher.start(stream_state.clone(), batch_lines_receiver);
                    break (active_dispatcher, line);
                }
                Ok(BatchLineMessage::Tick) => {
                    if let Some(stream_dead_timeout) = stream_dead_timeout {
                        let elapsed = last_batch_received.elapsed();
                        if elapsed > stream_dead_timeout {
                            stream_state.warn(format_args!(
                                "The stream is dead boys... after {:?}",
                                elapsed
                            ));
                            let sleeping_dispatcher = sleep_ticker.join().await?;
                            stream_state.request_stream_cancellation();
                            return Ok((params, sleeping_dispatcher));
                        }
                    }
                }
                Err(batch_line_error) => match batch_line_error.kind() {
                    BatchLineErrorKind::Parser => {
                        return Err(ConsumerErrorKind::InvalidBatch.into())
                    }
                    BatchLineErrorKind::Io => {
                        let sleeping_dispatcher = sleep_ticker.join().await?;
                        stream_state.request_stream_cancellation();
                        return Ok((params, sleeping_dispatcher));
                    }
                },
            }
        } else {
            let sleeping_dispatcher = sleep_ticker.join().await?;
            stream_state.request_stream_cancellation();
            return Ok((params, sleeping_dispatcher));
        }
    };

    let merged = stream::once(async { Ok(BatchLineMessage::BatchLine(first_line)) }).chain(merged);

    pin_mut!(merged);
    while let Some(batch_line_message_or_err) = merged.next().await {
        let batch_line_message = match batch_line_message_or_err {
            Ok(msg) => msg,
            Err(batch_line_error) => match batch_line_error.kind() {
                BatchLineErrorKind::Parser => return Err(ConsumerErrorKind::InvalidBatch.into()),
                BatchLineErrorKind::Io => {
                    stream_state.request_stream_cancellation();
                    break;
                }
            },
        };

        let msg_for_dispatcher = match batch_line_message {
            BatchLineMessage::Tick => {
                if let Some(stream_dead_timeout) = stream_dead_timeout {
                    let elapsed = last_batch_received.elapsed();
                    if elapsed > stream_dead_timeout {
                        stream_state.warn(format_args!(
                            "The stream is dead boys... after {:?}",
                            elapsed
                        ));
                        break;
                    }
                }

                DispatcherMessage::Tick
            }
            BatchLineMessage::BatchLine(batch) => {
                last_batch_received = Instant::now();

                if let Some(info_str) = batch.info_str() {
                    stream_state.info(format_args!("Received info line: {}", info_str));
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

    let sleeping_dispatcher = active_dispatcher.join().await?;

    stream_state.info(format_args!("Streaming stopped"));

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

struct SleepTicker<H, C> {
    join_handle: Option<tokio::task::JoinHandle<SleepingDispatcher<H, C>>>,
    wake_up: Arc<AtomicBool>,
}

impl<H, C> SleepTicker<H, C>
where
    H: BatchHandler,
    C: Send + 'static,
{
    pub fn start(
        sleeping_dispatcher: SleepingDispatcher<H, C>,
        consumer_state: ConsumerState,
    ) -> Self {
        let wake_up = Arc::new(AtomicBool::new(false));
        let join_handle =
            Self::tick_sleeping(Arc::clone(&wake_up), sleeping_dispatcher, consumer_state);

        Self {
            join_handle: Some(join_handle),
            wake_up,
        }
    }

    pub fn join(mut self) -> tokio::task::JoinHandle<SleepingDispatcher<H, C>> {
        self.join_handle.take().unwrap()
    }

    fn tick_sleeping(
        wake_up: Arc<AtomicBool>,
        mut sleeping_dispatcher: SleepingDispatcher<H, C>,
        consumer_state: ConsumerState,
    ) -> tokio::task::JoinHandle<SleepingDispatcher<H, C>>
    where
        H: BatchHandler,
        C: Send + 'static,
    {
        let delay = consumer_state.config().tick_interval.duration();

        let mut last_wait_notification = Instant::now();
        consumer_state.info(format_args!("Waiting for connection"));
        let sleep = async move {
            loop {
                if wake_up.load(Ordering::SeqCst) || consumer_state.global_cancellation_requested()
                {
                    consumer_state.debug(format_args!("Woke up!"));
                    break;
                }
                sleeping_dispatcher.tick();
                delay_for(delay).await;
                if last_wait_notification.elapsed() > Duration::from_secs(10) {
                    consumer_state.info(format_args!("Waiting for incoming batches..."));
                    last_wait_notification = Instant::now();
                }
            }

            sleeping_dispatcher
        };

        tokio::spawn(sleep)
    }
}

impl<H, C> Drop for SleepTicker<H, C> {
    fn drop(&mut self) {
        self.wake_up.store(true, Ordering::SeqCst)
    }
}

mod connect_stream {
    use std::time::{Duration, Instant};

    use http::status::StatusCode;
    use tokio::time::{delay_for, timeout};

    use crate::nakadi_types::{
        model::subscription::{StreamParameters, SubscriptionId},
        FlowId,
    };

    use crate::api::{NakadiApiError, SubscriptionStream, SubscriptionStreamApi};
    use crate::consumer::{ConsumerError, ConsumerErrorKind};
    use crate::internals::ConsumerState;
    use crate::logging::Logs;

    /// Sequence of backoffs after failed commit attempts
    const CONNECT_RETRY_BACKOFF_SECS: &[u64] = &[
        1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 3, 5, 5, 5, 10, 10, 10, 20, 20, 20, 30, 30, 30, 45,
        45, 45, 60, 60, 60, 90, 90, 90,
    ];

    pub(crate) async fn connect_with_retries<C: SubscriptionStreamApi>(
        api_client: C,
        consumer_state: ConsumerState,
    ) -> Result<SubscriptionStream, ConsumerError> {
        let config = consumer_state.config();
        let connect_stream_timeout = config.connect_stream_timeout.duration();
        let max_retry_delay = config.connect_stream_retry_max_delay.into_inner();
        let mut current_retry_delay = 0;
        let flow_id = FlowId::random();
        loop {
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
                connect_stream_timeout,
                flow_id.clone(),
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
                        consumer_state.warn(format_args!("Failed to connect to Nakadi: {}", err));
                    } else {
                        consumer_state.warn(format_args!("Failed to connect to Nakadi: {}", err));
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
        connect_timeout: Duration,
        flow_id: FlowId,
    ) -> Result<SubscriptionStream, NakadiApiError> {
        let f = client.request_stream(subscription_id, stream_params, flow_id.clone());
        let started = Instant::now();
        match timeout(connect_timeout, f).await {
            Ok(r) => r,
            Err(err) => Err(NakadiApiError::io()
                .with_context(format!(
                    "Connecting to Nakadi for a stream timed ot after {:?}.",
                    started.elapsed()
                ))
                .caused_by(err)),
        }
    }
}
