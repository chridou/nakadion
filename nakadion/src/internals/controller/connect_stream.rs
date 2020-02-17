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

pub(crate) async fn connect_with_retries<C: SubscriptionStreamApi>(
    api_client: C,
    consumer_state: ConsumerState,
) -> Result<SubscriptionStream, ConsumerError> {
    let config = consumer_state.config();
    let connect_stream_timeout = config.connect_stream_timeout.into_duration();
    let mut backoff = Backoff::new(config.connect_stream_retry_max_delay.into_inner());
    let flow_id = FlowId::random();
    loop {
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
                                return Err(ConsumerError::new(ConsumerErrorKind::AccessDenied)
                                    .with_source(err));
                            }
                        }
                        StatusCode::BAD_REQUEST => {
                            return Err(
                                ConsumerError::new(ConsumerErrorKind::Internal).with_source(err)
                            );
                        }
                        _ => {}
                    }
                    consumer_state.warn(format_args!("Failed to connect to Nakadi: {}", err));
                } else {
                    consumer_state.warn(format_args!("Failed to connect to Nakadi: {}", err));
                }
                let delay = backoff.next();
                delay_for(delay).await;
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

/// Sequence of backoffs after failed commit attempts
const CONNECT_RETRY_BACKOFF_SECS: &[u64] = &[
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 3, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 20, 20, 20, 30,
    30, 30, 45, 45, 45, 60, 60, 60, 90, 90, 90, 120, 180, 240, 300, 480, 960, 1920, 2400, 3000,
    3600,
];

struct Backoff {
    max: u64,
    iter: Box<dyn Iterator<Item = u64> + Send + 'static>,
}

impl Backoff {
    pub fn new(max: u64) -> Self {
        let iter = Box::new(CONNECT_RETRY_BACKOFF_SECS.iter().copied());
        Backoff { iter, max }
    }

    pub fn next(&mut self) -> Duration {
        let d = if let Some(next) = self.iter.next() {
            next
        } else {
            self.max
        };

        let d = std::cmp::min(d, self.max);

        Duration::from_secs(d)
    }
}
