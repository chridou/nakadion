//! The consumer iterates over batches of events.
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use nakadi::api::ApiClient;
use nakadi::batch::{Batch, BatchLine};
use nakadi::committer::Committer;
use nakadi::dispatcher::Dispatcher;
use nakadi::handler::HandlerFactory;
use nakadi::metrics::MetricsCollector;
use nakadi::model::*;
use nakadi::streaming_client::{ConnectError, LineResult, RawLine, StreamingClient};
use nakadi::{CommitStrategy, Lifecycle};

/// Sequence of backoffs after failed commit attempts
const CONNECT_RETRY_BACKOFF_MS: &'static [u64] = &[
    10, 50, 100, 500, 1000, 1000, 1000, 3000, 3000, 3000, 5000, 5000, 5000, 10_000, 10_000, 10_000,
    15_000, 15_000, 15_000,
];

/// The consumer connects to the stream using a `StreamingClient` and then
/// iterates over the batches.
///
/// The consumer also manages connection attempts to the stream and reconnect.
///
/// This is the main component consuming batches and instatiation
/// helper components for each newly connected stream.
///
/// The consumer creates a background thread.
#[derive(Clone)]
pub struct Consumer {
    lifecycle: Lifecycle,
    subscription_id: SubscriptionId,
}

impl Consumer {
    /// Start a new `Consumer`
    pub fn start<C, A, HF, M>(
        streaming_client: C,
        api_client: A,
        subscription_id: SubscriptionId,
        handler_factory: HF,
        commit_strategy: CommitStrategy,
        metrics_collector: M,
        min_idle_worker_lifetime: Option<Duration>,
    ) -> Consumer
    where
        C: StreamingClient + Clone + Send + 'static,
        A: ApiClient + Clone + Send + 'static,
        HF: HandlerFactory + Send + Sync + 'static,
        M: MetricsCollector + Clone + Send + 'static,
    {
        let lifecycle = Lifecycle::default();

        let consumer = Consumer {
            lifecycle: lifecycle.clone(),
            subscription_id: subscription_id.clone(),
        };

        start_consumer_loop(
            streaming_client,
            api_client,
            handler_factory,
            commit_strategy,
            subscription_id,
            lifecycle,
            metrics_collector,
            min_idle_worker_lifetime,
        );

        consumer
    }

    pub fn running(&self) -> bool {
        self.lifecycle.running()
    }

    pub fn stop(&self) {
        self.lifecycle.request_abort()
    }
}

fn start_consumer_loop<C, A, HF, M>(
    streaming_client: C,
    api_client: A,
    handler_factory: HF,
    commit_strategy: CommitStrategy,
    subscription_id: SubscriptionId,
    lifecycle: Lifecycle,
    metrics_collector: M,
    min_idle_worker_lifetime: Option<Duration>,
) where
    C: StreamingClient + Clone + Send + 'static,
    A: ApiClient + Clone + Send + 'static,
    HF: HandlerFactory + Send + Sync + 'static,
    M: MetricsCollector + Clone + Send + 'static,
{
    thread::spawn(move || {
        consumer_loop(
            streaming_client,
            api_client,
            handler_factory,
            commit_strategy,
            subscription_id,
            lifecycle,
            metrics_collector,
            min_idle_worker_lifetime,
        )
    });
}

fn consumer_loop<C, A, HF, M>(
    streaming_client: C,
    api_client: A,
    handler_factory: HF,
    commit_strategy: CommitStrategy,
    subscription_id: SubscriptionId,
    lifecycle: Lifecycle,
    metrics_collector: M,
    min_idle_worker_lifetime: Option<Duration>,
) where
    C: StreamingClient + Clone + Send + 'static,
    A: ApiClient + Clone + Send + 'static,
    HF: HandlerFactory + Send + Sync + 'static,
    M: MetricsCollector + Clone + Send + 'static,
{
    let handler_factory = Arc::new(handler_factory);

    loop {
        if lifecycle.abort_requested() {
            info!(
                "[Consumer, subscription={}] Abort requested",
                subscription_id
            );
            break;
        }

        info!(
            "[Consumer, subscription={}] Connecting to stream",
            subscription_id
        );
        let start = Instant::now();
        let (stream_id, line_iterator) = match connect(
            &streaming_client,
            &subscription_id,
            Duration::from_secs(300),
            &lifecycle,
        ) {
            Ok(v) => {
                metrics_collector.consumer_connected(start);
                v
            }
            Err(err) => {
                if err.is_permanent() {
                    error!(
                        "[Consumer, subscription={}] Permanent connection error: {}",
                        subscription_id, err
                    );
                    break;
                } else {
                    warn!(
                        "[Consumer, subscription={}] Temporary connection error: {}",
                        subscription_id, err
                    );
                    continue;
                }
            }
        };

        info!(
            "[Consumer, subscription={}, stream={}] Connected to a new stream.",
            subscription_id, stream_id
        );
        let connected_since = Instant::now();

        let committer = Committer::start(
            api_client.clone(),
            commit_strategy,
            subscription_id.clone(),
            stream_id.clone(),
            metrics_collector.clone(),
        );

        let dispatcher = Dispatcher::start(
            handler_factory.clone(),
            committer.clone(),
            metrics_collector.clone(),
            min_idle_worker_lifetime,
        );

        consume(
            line_iterator,
            subscription_id.clone(),
            stream_id.clone(),
            dispatcher,
            committer,
            lifecycle.clone(),
            &metrics_collector,
        );
        info!(
            "[Consumer, subscription={}, stream={}] Stream ended.",
            subscription_id, stream_id
        );

        metrics_collector.consumer_connection_lifetime(connected_since);
    }

    lifecycle.stopped();

    info!(
        "[Consumer, subscription={}] Nakadi consumer stopped",
        subscription_id
    );
}

fn consume<I, M>(
    line_iterator: I,
    subscription_id: SubscriptionId,
    stream_id: StreamId,
    dispatcher: Dispatcher,
    committer: Committer,
    lifecycle: Lifecycle,
    metrics_collector: &M,
) where
    I: Iterator<Item = LineResult>,
    M: MetricsCollector,
{
    for line_result in line_iterator {
        if lifecycle.abort_requested() {
            info!(
                "[Consumer, subscription={}, stream={}] Abort requested",
                subscription_id, stream_id
            );
            break;
        }
        match line_result {
            Ok(raw_line) => {
                if let Err(err) = send_line(
                    &dispatcher,
                    &subscription_id,
                    &stream_id,
                    raw_line,
                    metrics_collector,
                ) {
                    error!(
                        "[Consumer, subscription={}, stream={}] Could not process batch: {}",
                        subscription_id, stream_id, err
                    );
                    break;
                }
            }
            Err(err) => {
                error!(
                    "[Consumer, subscription={}, stream={}]The connection broke: {}",
                    subscription_id, stream_id, err
                );
                break;
            }
        }
    }

    info!(
        "[Consumer, subscription={}, stream={}] Stream ended. Stopping components",
        subscription_id, stream_id
    );

    info!(
        "[Consumer, subscription={}, stream={}] Stopping dispatcher.",
        subscription_id, stream_id
    );

    dispatcher.stop();

    while dispatcher.is_running() {
        thread::sleep(Duration::from_millis(10));
    }

    info!(
        "[Consumer, subscription={}, stream={}] Dispatcher stopped.",
        subscription_id, stream_id
    );

    info!(
        "[Consumer, subscription={}, stream={}] Stopping committer",
        subscription_id, stream_id
    );

    committer.stop();

    while committer.running() {
        thread::sleep(Duration::from_millis(10));
    }

    info!(
        "[Consumer, subscription={}, stream={}] Committer stopped.",
        subscription_id, stream_id
    );
}

fn send_line<M>(
    dispatcher: &Dispatcher,
    subscription_id: &SubscriptionId,
    stream_id: &StreamId,
    raw_line: RawLine,
    metrics_collector: &M,
) -> Result<(), String>
where
    M: MetricsCollector,
{
    let num_bytes = raw_line.bytes.len();
    metrics_collector.consumer_line_received(num_bytes);

    let batch_line = BatchLine::new(raw_line.bytes)?;

    if let Some(info) = batch_line.info() {
        match ::std::str::from_utf8(info) {
            Ok(info) => {
                metrics_collector.consumer_info_line_received(info.len());
                info!(
                    "[Consumer, subscription={}, stream={}] Received info: {}",
                    subscription_id, stream_id, info
                );
            }
            Err(err) => info!(
                "[Consumer, subscription={}, stream={}] Received info line \
                 which is not readable: {}",
                subscription_id, stream_id, err
            ),
        };
    }

    if batch_line.is_keep_alive_line() {
        debug!("Keep alive!");
        metrics_collector.consumer_keep_alive_line_received(num_bytes);
        Ok(())
    } else {
        metrics_collector.consumer_batch_line_received(num_bytes);
        dispatcher.process(Batch {
            batch_line: batch_line,
            received_at: raw_line.received_at,
        })
    }
}

fn connect<C: StreamingClient>(
    client: &C,
    subscription_id: &SubscriptionId,
    max_dur: Duration,
    lifecycle: &Lifecycle,
) -> Result<(StreamId, C::LineIterator), ConnectError> {
    let deadline = Instant::now() + max_dur;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let flow_id = FlowId::default();
        match client.connect(subscription_id, flow_id.clone()) {
            Ok(it) => {
                return Ok(it);
            }
            Err(err) => {
                let sleep_dur_ms = *CONNECT_RETRY_BACKOFF_MS.get(attempt).unwrap_or(&30_000);
                if Instant::now() >= deadline {
                    return Err(ConnectError::Other(
                        format!("Failed to connect to Nakadi after {} attempts.", attempt),
                        flow_id,
                    ));
                } else if lifecycle.abort_requested() {
                    return Err(ConnectError::Other(
                        format!(
                            "Failed to connect to Nakadi after {} attempts. Abort requested",
                            attempt
                        ),
                        flow_id,
                    ));
                } else {
                    warn!(
                        "Failed to connect(attempt {}) to Nakadi(retry in {}ms): {}",
                        attempt, sleep_dur_ms, err
                    );
                    thread::sleep(Duration::from_millis(sleep_dur_ms));
                }
            }
        }
    }
}
