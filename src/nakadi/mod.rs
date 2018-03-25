use std::env;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use failure::*;
use serde_json;

pub mod api;
pub mod batch;
pub mod committer;
pub mod consumer;
pub mod dispatcher;
pub mod events;
pub mod handler;
pub mod metrics;
pub mod model;
pub mod publisher;
pub mod streaming_client;
pub mod worker;

use auth::ProvidesAccessToken;
use metrics::{DevNullMetricsCollector, MetricsCollector};
use nakadi::api::{ApiClient, NakadiApiClient};
use nakadi::handler::HandlerFactory;
use nakadi::model::SubscriptionId;
use nakadi::streaming_client::StreamingClient;

#[cfg(feature = "metrix")]
use metrix::processor::AggregatesProcessors;

/// Strategy for committing cursors
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CommitStrategy {
    /// Commit all cursors immediately
    AllBatches,
    /// Commit as late as possile
    Latest,
    /// Commit latest after `seconds` seconds
    AfterSeconds { seconds: u16 },
    /// Commit latest after `after_batches` batches have been
    /// received or after `after_seconds` seconds have
    /// elapsed
    Batches {
        after_batches: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        after_seconds: Option<u16>,
    },
    /// Commit latest after `after_events` events have been
    /// received or after `after_seconds` seconds have
    /// elapsed
    ///
    /// This requires the `BatchHandler` to return the number
    /// of processed events to work properly.
    Events {
        after_events: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        after_seconds: Option<u16>,
    },
}

#[derive(Clone)]
pub struct Lifecycle {
    state: Arc<(AtomicBool, AtomicBool)>,
}

impl Lifecycle {
    pub fn abort_requested(&self) -> bool {
        self.state.0.load(Ordering::Relaxed)
    }

    pub fn request_abort(&self) {
        self.state.0.store(true, Ordering::Relaxed)
    }

    pub fn stopped(&self) {
        self.state.1.store(false, Ordering::Relaxed)
    }

    pub fn running(&self) -> bool {
        self.state.1.load(Ordering::Relaxed)
    }
}

impl Default for Lifecycle {
    fn default() -> Lifecycle {
        Lifecycle {
            state: Arc::new((AtomicBool::new(false), AtomicBool::new(true))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionDiscovery {
    /// Connect with an existing `SubscriptionId`
    ExistingId(SubscriptionId),
    /// Create a new subscription for the application
    /// and if it already exists use
    /// the existing subscription
    Application(api::SubscriptionRequest),
}

#[test]
fn discovery_serialize_existing_id() {
    let discovery = SubscriptionDiscovery::ExistingId(SubscriptionId::new("abd"));

    let json_str = serde_json::to_string(&discovery).unwrap();

    assert_eq!(&json_str, "{\"ExistingId\":\"abd\"}");
}

#[test]
fn discovery_serialize_application() {
    let discovery = SubscriptionDiscovery::Application(api::SubscriptionRequest {
        owning_application: "test_app".into(),
        event_types: vec!["event_type_1".into()],
        read_from: Some(api::ReadFrom::Begin),
    });

    let json_str = serde_json::to_string(&discovery).unwrap();

    assert_eq!(
        &json_str,
        "{\"Application\":{\"owning_application\":\"test_app\",\
         \"event_types\":[\"event_type_1\"],\"read_from\":\"begin\"}}"
    );
}

impl fmt::Display for SubscriptionDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Settings for establishing a connection to `Nakadi`.
#[derive(Debug, Clone)]
pub struct NakadionConfig {
    /// Maximum number of empty keep alive batches to get in a row before closing the
    /// connection. If 0 or undefined will send keep alive messages indefinitely.
    pub stream_keep_alive_limit: usize,
    /// Maximum number of `Event`s in this stream (over all partitions being streamed
    /// in this
    /// connection).
    ///
    /// * If 0 or undefined, will stream batches indefinitely.
    /// * Stream initialization will fail if `stream_limit` is lower than `batch_limit`.
    pub stream_limit: usize,
    /// Maximum time in seconds a stream will live before connection is closed by the
    /// server.
    ///
    /// If 0 or unspecified will stream indefinitely.
    /// If this timeout is reached, any pending messages (in the sense of
    /// `stream_limit`)
    /// will be flushed to the client.
    /// Stream initialization will fail if `stream_timeout` is lower than
    /// `batch_flush_timeout`.
    pub stream_timeout: Duration,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    ///
    ///  * If the amount of buffered Events reaches `batch_limit`
    /// before this `batch_flush_timeout` is reached, the messages are immediately
    /// flushed to the client and batch flush timer is reset.
    ///  * If 0 or undefined, will assume 30 seconds.
    pub batch_flush_timeout: Duration,
    /// Maximum number of `Event`s in each chunk (and therefore per partition) of the
    /// stream.
    ///
    ///  * If 0 or unspecified will buffer Events indefinitely and flush on reaching of
    ///  `batch_flush_timeout`.
    pub batch_limit: usize,
    /// The amount of uncommitted events Nakadi will stream before pausing the stream.
    /// When in paused state and commit comes - the stream will resume. Minimal value
    /// is 1.
    pub max_uncommitted_events: usize,
    /// The URI prefix for the Nakadi Host, e.g. "https://my.nakadi.com"
    pub nakadi_host: String,
    /// The request timeout used when committing events
    pub request_timeout: Duration,
    /// The `CommitStrategy` to use
    pub commit_strategy: CommitStrategy,
    /// Used for discovering/creating the subscription to consume
    pub subscription_discovery: SubscriptionDiscovery,
    /// The time after which a worker that received no events
    /// will be shut down.
    pub min_idle_worker_lifetime: Option<Duration>,
}

pub struct NakadionBuilder {
    /// The configuration of the streaming client used to connect to the stream.
    ///
    /// The defaults are defaults of `streaming_client::ConfigBuilder`
    pub streaming_client_builder: streaming_client::ConfigBuilder,
    /// The request timeout used when committing events
    pub request_timeout: Option<Duration>,
    /// The `CommitStrategy` to use for committing cursors
    pub commit_strategy: Option<CommitStrategy>,
    /// Used for discovering/creating the subscription to consume
    pub subscription_discovery: Option<SubscriptionDiscovery>,
    /// The time after which a worker that received no events
    /// will be shut down. The default is to never shut down workers.
    pub min_idle_worker_lifetime: Option<Duration>,
}

impl Default for NakadionBuilder {
    fn default() -> NakadionBuilder {
        NakadionBuilder {
            streaming_client_builder: Default::default(),
            request_timeout: None,
            commit_strategy: None,
            subscription_discovery: None,
            min_idle_worker_lifetime: None,
        }
    }
}

impl NakadionBuilder {
    /// Maximum number of empty keep alive batches to get in a row before closing the
    /// connection. If 0 or undefined will send keep alive messages indefinitely.
    pub fn stream_keep_alive_limit(mut self, stream_keep_alive_limit: usize) -> NakadionBuilder {
        self.streaming_client_builder.stream_keep_alive_limit = Some(stream_keep_alive_limit);
        self
    }
    /// Maximum number of `Event`s in this stream (over all partitions being streamed
    /// in this
    /// connection).
    ///
    /// * If 0 or undefined, will stream batches indefinitely.
    /// * Stream initialization will fail if `stream_limit` is lower than `batch_limit`.
    pub fn stream_limit(mut self, stream_limit: usize) -> NakadionBuilder {
        self.streaming_client_builder.stream_limit = Some(stream_limit);
        self
    }
    /// Maximum time in seconds a stream will live before connection is closed by the
    /// server.
    ///
    /// If 0 or unspecified will stream indefinitely.
    /// If this timeout is reached, any pending messages (in the sense of
    /// `stream_limit`)
    /// will be flushed to the client.
    /// Stream initialization will fail if `stream_timeout` is lower than
    /// `batch_flush_timeout`.
    pub fn stream_timeout(mut self, stream_timeout: Duration) -> NakadionBuilder {
        self.streaming_client_builder.stream_timeout = Some(stream_timeout);
        self
    }
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    ///
    ///  * If the amount of buffered Events reaches `batch_limit`
    /// before this `batch_flush_timeout` is reached, the messages are immediately
    /// flushed to the client and batch flush timer is reset.
    ///  * If 0 or undefined, will assume 30 seconds.
    pub fn batch_flush_timeout(mut self, batch_flush_timeout: Duration) -> NakadionBuilder {
        self.streaming_client_builder.batch_flush_timeout = Some(batch_flush_timeout);
        self
    }
    /// Maximum number of `Event`s in each chunk (and therefore per partition) of the
    /// stream.
    ///
    ///  * If 0 or unspecified will buffer Events indefinitely and flush on reaching of
    ///  `batch_flush_timeout`.
    pub fn batch_limit(mut self, batch_limit: usize) -> NakadionBuilder {
        self.streaming_client_builder.batch_limit = Some(batch_limit);
        self
    }
    /// The amount of uncommitted events Nakadi will stream before pausing the stream.
    /// When in paused state and commit comes - the stream will resume. Minimal value
    /// is 1.
    ///
    /// When using the concurrent worker you should adjust this value to safe your
    /// workers from running dry.
    pub fn max_uncommitted_events(mut self, max_uncommitted_events: usize) -> NakadionBuilder {
        self.streaming_client_builder.max_uncommitted_events = Some(max_uncommitted_events);
        self
    }
    /// The URI prefix for the Nakadi Host, e.g. "https://my.nakadi.com"
    pub fn nakadi_host<T: Into<String>>(mut self, nakadi_host: T) -> NakadionBuilder {
        self.streaming_client_builder.nakadi_host = Some(nakadi_host.into());
        self
    }

    /// The request timeout used when committing events
    pub fn request_timeout(mut self, request_timeout: Duration) -> NakadionBuilder {
        self.request_timeout = Some(request_timeout);
        self
    }

    /// The time after which a worker that received no events
    /// will be shut down. The default is to never shut down workers.
    pub fn commit_strategy(mut self, commit_strategy: CommitStrategy) -> NakadionBuilder {
        self.commit_strategy = Some(commit_strategy);
        self
    }

    pub fn subscription_discovery(
        mut self,
        subscription_discovery: SubscriptionDiscovery,
    ) -> NakadionBuilder {
        self.subscription_discovery = Some(subscription_discovery);
        self
    }

    pub fn min_idle_worker_lifetime(
        mut self,
        min_idle_worker_lifetime: Option<Duration>,
    ) -> NakadionBuilder {
        self.min_idle_worker_lifetime = min_idle_worker_lifetime;
        self
    }

    pub fn set_min_idle_worker_lifetime(
        mut self,
        min_idle_worker_lifetime: Duration,
    ) -> NakadionBuilder {
        self.min_idle_worker_lifetime = Some(min_idle_worker_lifetime);
        self
    }

    /// Create a new builder from environment variables.
    ///
    /// # Environment Variables:
    ///
    /// * `NAKADION_NAKADI_HOST`: See `NakadionBuilder::nakadi_host`
    /// * `NAKADION_MAX_UNCOMMITED_EVENTS`: See
    /// `NakadionBuilder::max_uncommitted_events`
    /// * `NAKADION_BATCH_LIMIT`: See `NakadionBuilder::batch_limit`
    /// * `NAKADION_BATCH_FLUSH_TIMEOUT_SECS`: See
    /// `NakadionBuilder::batch_flush_timeout`
    /// * `NAKADION_STREAM_TIMEOUT_SECS`: See `NakadionBuilder::stream_timeout`
    /// * `NAKADION_STREAM_LIMIT`: See `NakadionBuilder::stream_limit`
    /// * `NAKADION_STREAM_KEEP_ALIVE_LIMIT´: See
    /// `NakadionBuilder::stream_keep_alive_limit`
    /// * `NAKADION_REQUEST_TIMEOUT_MS`: See `NakadionBuilder::request_timeout`
    /// * `NAKADION_COMMIT_STRATEGY`: See `NakadionBuilder::commit_strategy`
    /// * `NAKADION_SUBSCRIPTION_DISCOVERY`: See `NakadionBuilder::subscription_discovery`
    /// * `NAKADION_MIN_IDLE_WORKER_LIFETIME_SECS`:
    /// See `NakadionBuilder::min_idle_worker_lifetime`
    ///
    /// # Errors
    ///
    /// A value from an existing environment variable was not parsable
    pub fn from_env() -> Result<NakadionBuilder, Error> {
        let streaming_client_builder = streaming_client::ConfigBuilder::from_env()?;

        let mut builder = NakadionBuilder::default();
        builder.streaming_client_builder = streaming_client_builder;

        let builder = if let Some(env_val) = env::var("NAKADION_REQUEST_TIMEOUT_MS").ok() {
            builder.request_timeout(Duration::from_millis(env_val
                .parse::<u64>()
                .context("Could not parse 'NAKADION_REQUEST_TIMEOUT_MS'")?))
        } else {
            warn!(
                "Environment variable 'NAKADION_REQUEST_TIMEOUT_MS' not found. It will be set \
                 to the default."
            );
            builder
        };

        let builder = if let Some(env_val) = env::var("NAKADION_COMMIT_STRATEGY").ok() {
            let commit_strategy = serde_json::from_str(&env_val)
                .context("Could not parse 'NAKADION_COMMIT_STRATEGY'")?;
            builder.commit_strategy(commit_strategy)
        } else {
            warn!(
                "Environment variable 'NAKADION_COMMIT_STRATEGY' not found. It will be set \
                 to the default."
            );
            builder
        };

        let builder = if let Some(env_val) = env::var("NAKADION_SUBSCRIPTION_DISCOVERY").ok() {
            let discovery = serde_json::from_str(&env_val)
                .context("Could not parse 'NAKADION_SUBSCRIPTION_DISCOVERY'")?;
            builder.subscription_discovery(discovery)
        } else {
            warn!(
                "Environment variable 'NAKADION_SUBSCRIPTION_DISCOVERY' not found. It must be set \
                 set manually."
            );
            builder
        };

        let builder = if let Some(env_val) = env::var("NAKADION_MIN_IDLE_WORKER_LIFETIME_SECS").ok()
        {
            builder.min_idle_worker_lifetime(Some(Duration::from_secs(env_val
                .parse::<u64>()
                .context("Could not parse 'NAKADION_MIN_IDLE_WORKER_LIFETIME_SECS'")?)))
        } else {
            warn!(
                "Environment variable 'NAKADION_MIN_IDLE_WORKER_LIFETIME_SECS' not found. Using \
                 default."
            );
            builder
        };

        Ok(builder)
    }

    /// Build a `NakadionConfig` from this builder
    ///
    /// # Errors
    ///
    /// Not all mandatory values have been set
    pub fn build_config(self) -> Result<NakadionConfig, Error> {
        let streaming_client_config = self.streaming_client_builder.build()?;

        let request_timeout = if let Some(request_timeout) = self.request_timeout {
            request_timeout
        } else {
            Duration::from_millis(300)
        };

        let commit_strategy = if let Some(commit_strategy) = self.commit_strategy {
            commit_strategy
        } else {
            CommitStrategy::AllBatches
        };

        let subscription_discovery =
            if let Some(subscription_discovery) = self.subscription_discovery {
                subscription_discovery
            } else {
                return Err(format_err!("Subscription discovery is missing"));
            };

        Ok(NakadionConfig {
            stream_keep_alive_limit: streaming_client_config.stream_keep_alive_limit,
            stream_limit: streaming_client_config.stream_limit,
            stream_timeout: streaming_client_config.stream_timeout,
            batch_flush_timeout: streaming_client_config.batch_flush_timeout,
            batch_limit: streaming_client_config.batch_limit,
            max_uncommitted_events: streaming_client_config.max_uncommitted_events,
            request_timeout,
            commit_strategy,
            subscription_discovery,
            nakadi_host: streaming_client_config.nakadi_host,
            min_idle_worker_lifetime: self.min_idle_worker_lifetime,
        })
    }

    /// Build and start `Nakadion` from this builder
    ///
    /// No metrics will be collected.
    ///
    /// # Errors
    ///
    /// Not all mandatory values have been set or
    /// creating `Nakadion` failed.
    pub fn build_and_start<HF, P>(
        self,
        handler_factory: HF,
        access_token_provider: P,
    ) -> Result<Nakadion, Error>
    where
        HF: HandlerFactory + Sync + Send + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
    {
        self.build_and_start_with_metrics(
            handler_factory,
            access_token_provider,
            DevNullMetricsCollector,
        )
    }

    /// Build and start `Nakadion` from this builder
    ///
    /// The given `MetricsCollector` will be used to collect metrics
    ///
    /// # Errors
    ///
    /// Not all mandatory values have been set or
    /// creating `Nakadion` failed.
    pub fn build_and_start_with_metrics<HF, P, M>(
        self,
        handler_factory: HF,
        access_token_provider: P,
        metrics_collector: M,
    ) -> Result<Nakadion, Error>
    where
        HF: HandlerFactory + Sync + Send + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
        M: MetricsCollector + Clone + Send + Sync + 'static,
    {
        let config = self.build_config()?;

        Nakadion::start(
            config,
            handler_factory,
            access_token_provider,
            metrics_collector,
        )
    }

    /// Build and start `Nakadion` from this builder.
    ///
    /// Metrics will be collected using
    /// [metrix](https://crates.io/crates/metrix)
    ///
    /// # Errors
    ///
    /// Not all mandatory values have been set or
    /// creating `Nakadion` failed.
    #[cfg(feature = "metrix")]
    pub fn build_and_start_with_metrix<HF, P, T>(
        self,
        handler_factory: HF,
        access_token_provider: P,
        put_metrics_here: &mut T,
    ) -> Result<Nakadion, Error>
    where
        HF: HandlerFactory + Sync + Send + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
        T: AggregatesProcessors,
    {
        let metrix_collector = ::nakadi::metrics::MetrixCollector::new(put_metrics_here);
        let config = self.build_config()?;

        Nakadion::start(
            config,
            handler_factory,
            access_token_provider,
            metrix_collector,
        )
    }
}

/// This struct represents Nakadion.
///
/// Once instatntiated it can only be used
/// query its running state or to stop
/// consuming events.
pub struct Nakadion {
    guard: Arc<DropGuard>,
}

impl Nakadion {
    /// Start with manually created components and parameter
    ///
    /// The `SubscriptionId` must be already known
    ///
    /// # Errors
    ///
    /// Nakadion could not be started.
    pub fn start_with<HF, C, A, M>(
        subscription_id: SubscriptionId,
        streaming_client: C,
        api_client: A,
        handler_factory: HF,
        commit_strategy: CommitStrategy,
        metrics_collector: M,
        min_idle_worker_lifetime: Option<Duration>,
    ) -> Result<Nakadion, Error>
    where
        C: StreamingClient + Clone + Sync + Send + 'static,
        A: ApiClient + Clone + Sync + Send + 'static,
        HF: HandlerFactory + Sync + Send + 'static,
        M: MetricsCollector + Clone + Send + Sync + 'static,
    {
        let consumer = consumer::Consumer::start(
            streaming_client,
            api_client,
            subscription_id,
            handler_factory,
            commit_strategy,
            metrics_collector,
            min_idle_worker_lifetime,
        );

        let guard = Arc::new(DropGuard { consumer });
        Ok(Nakadion { guard })
    }

    /// Start with the given configuration
    /// and create all interally required components from the configuration
    ///
    /// # Errors
    ///
    /// Nakadion could not be started. This might be due to an
    /// invalid configuration
    pub fn start<HF, P, M>(
        config: NakadionConfig,
        handler_factory: HF,
        access_token_provider: P,
        metrics_collector: M,
    ) -> Result<Nakadion, Error>
    where
        HF: HandlerFactory + Sync + Send + 'static,
        P: ProvidesAccessToken + Send + Sync + 'static,
        M: MetricsCollector + Clone + Send + Sync + 'static,
    {
        let access_token_provider = Arc::new(access_token_provider);

        let api_client = NakadiApiClient::with_shared_access_token_provider(
            api::Config {
                nakadi_host: config.nakadi_host.clone(),
                request_timeout: config.request_timeout,
            },
            access_token_provider.clone(),
        )?;

        info!(
            "Discovering subscription with {}",
            config.subscription_discovery
        );

        let subscription_id = match config.subscription_discovery {
            SubscriptionDiscovery::ExistingId(id) => id,
            SubscriptionDiscovery::Application(request) => {
                match api_client.create_subscription(&request)? {
                    api::CreateSubscriptionStatus::Created(subscription) => {
                        info!("Created new subscription {}", subscription.id);
                        subscription.id
                    }
                    api::CreateSubscriptionStatus::AlreadyExists(subscription) => {
                        info!("Using already existing subscription {}", subscription.id);
                        subscription.id
                    }
                }
            }
        };

        let streaming_client_config = streaming_client::Config {
            stream_keep_alive_limit: config.stream_keep_alive_limit,
            stream_limit: config.stream_limit,
            stream_timeout: config.stream_timeout,
            batch_flush_timeout: config.batch_flush_timeout,
            batch_limit: config.batch_limit,
            max_uncommitted_events: config.max_uncommitted_events,
            nakadi_host: config.nakadi_host,
        };

        let streaming_client =
            streaming_client::NakadiStreamingClient::with_shared_access_token_provider(
                streaming_client_config,
                access_token_provider,
                metrics_collector.clone(),
            )?;

        Nakadion::start_with(
            subscription_id,
            streaming_client,
            api_client,
            handler_factory,
            config.commit_strategy,
            metrics_collector,
            config.min_idle_worker_lifetime,
        )
    }

    /// Returns true if Nakadion is running
    pub fn running(&self) -> bool {
        self.guard.running()
    }

    /// Stops Nakadion
    pub fn stop(&self) {
        self.guard.consumer.stop()
    }

    /// Block the current thread until Nakadion has stopped
    pub fn block_until_stopped(&self) {
        self.block_until_stopped_with_interval(Duration::from_secs(1))
    }

    /// Block the current thread until Nakadion has stopped.
    ///
    /// The state is polled at the given `poll_interval`.
    pub fn block_until_stopped_with_interval(&self, poll_interval: Duration) {
        while self.running() {
            thread::sleep(poll_interval);
        }
    }
}

struct DropGuard {
    consumer: consumer::Consumer,
}

impl DropGuard {
    fn running(&self) -> bool {
        self.consumer.running()
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.consumer.stop()
    }
}
