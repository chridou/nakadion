use std::time::{Duration, Instant};

use tokio::time::delay_for;

use crate::nakadi_types::FlowId;

use crate::api::SubscriptionStreamChunks;
use crate::components::connector::{ConnectErrorKind, ProvidesConnector};
use crate::consumer::{ConsumerError, ConsumerErrorKind};
use crate::instrumentation::Instruments;
use crate::internals::ConsumerState;
use crate::logging::Logs;

pub(crate) async fn connect_with_retries<C: ProvidesConnector>(
    connector_provider: C,
    consumer_state: ConsumerState,
) -> Result<SubscriptionStreamChunks, ConsumerError> {
    let config = consumer_state.config();
    let instrumentation = consumer_state.instrumentation();
    let subscription_id = consumer_state.subscription_id();

    let mut connector = connector_provider.connector();
    connector.set_flow_id(FlowId::random());
    connector.set_connect_stream_timeout(config.connect_stream_timeout);
    connector.set_instrumentation(instrumentation.clone());
    *connector.stream_parameters_mut() = config.stream_parameters.clone();

    let mut backoff = Backoff::new(config.connect_stream_retry_max_delay.into_inner());

    let mut attempts_left = config.max_connect_attempts.into_inner();

    let connect_started_at = Instant::now();
    loop {
        if consumer_state.global_cancellation_requested() {
            return Err(ConsumerError::new(ConsumerErrorKind::UserAbort));
        }

        if attempts_left == 0 {
            return Err(ConsumerError::other().with_message("No connect attempts left. Aborting"));
        }
        attempts_left -= 1;

        match connector.connect(subscription_id).await {
            Ok(stream) => {
                instrumentation.stream_connected(connect_started_at.elapsed());

                return Ok(stream);
            }
            Err(err) => {
                match err.kind() {
                    ConnectErrorKind::SubscriptionNotFound => {
                        if config.abort_connect_on_subscription_not_found.into() {
                            instrumentation.stream_not_connected(connect_started_at.elapsed());
                            return Err(ConsumerError::new(
                                ConsumerErrorKind::SubscriptionNotFound,
                            )
                            .with_source(err));
                        }
                    }
                    ConnectErrorKind::AccessDenied => {
                        if config.abort_connect_on_auth_error.into() {
                            instrumentation.stream_not_connected(connect_started_at.elapsed());
                            return Err(ConsumerError::new(ConsumerErrorKind::AccessDenied)
                                .with_source(err));
                        }
                    }
                    ConnectErrorKind::BadRequest => {
                        instrumentation.stream_not_connected(connect_started_at.elapsed());
                        return Err(
                            ConsumerError::new(ConsumerErrorKind::Internal).with_source(err)
                        );
                    }
                    ConnectErrorKind::Unprocessable => {
                        instrumentation.stream_not_connected(connect_started_at.elapsed());
                        return Err(
                            ConsumerError::new(ConsumerErrorKind::Internal).with_source(err)
                        );
                    }
                    ConnectErrorKind::Io => {}
                    ConnectErrorKind::Other => {}
                }
                consumer_state.warn(format_args!("Failed to connect to Nakadi: {}", err));
                let delay = backoff.next();
                delay_for(delay).await;
                continue;
            }
        }
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
