use std::sync::Arc;
use std::env;
use std::time::{Duration, Instant};
use std::io::{BufRead, BufReader, Error as IoError, Read, Split};

use auth::{AccessToken, ProvidesAccessToken, TokenError};
use nakadi::model::{StreamId, SubscriptionId};
use reqwest::{Client as HttpClient, Response};
use reqwest::StatusCode;
use reqwest::header::{Authorization, Bearer, ContentType, Headers};
use serde_json;
use backoff::{Error as BackoffError, ExponentialBackoff, Operation};
use self::stats::*;

header! { (XNakadiStreamId, "X-Nakadi-StreamId") => [String] }

const LINE_SPLIT_BYTE: u8 = b'\n';

pub type LineResult = ::std::result::Result<(Vec<u8>, Instant), IoError>;

pub struct NakadiLineIterator {
    lines: Split<BufReader<Response>>,
}

impl NakadiLineIterator {
    pub fn new(response: Response) -> Self {
        let reader = BufReader::new(response);
        NakadiLineIterator {
            lines: reader.split(LINE_SPLIT_BYTE),
        }
    }
}

impl Iterator for NakadiLineIterator {
    type Item = LineResult;

    fn next(&mut self) -> Option<LineResult> {
        self.lines.next().map(|r| r.map(|l| (l, Instant::now())))
    }
}

/// A client to the Nakadi Event Broker
pub trait StreamingClient {
    type LineIterator: Iterator<Item = LineResult>;
    fn connect(&self) -> ::std::result::Result<(StreamId, Self::LineIterator), ConnectError>;
    fn commit<T: AsRef<[u8]>>(
        &self,
        stream_id: StreamId,
        cursors: &[T],
    ) -> ::std::result::Result<CommitStatus, CommitError>;

    fn subscription_id(&self) -> &SubscriptionId;
}

/// Settings for establishing a connection to `Nakadi`.
#[derive(Builder, Debug)]
#[builder(pattern = "owned")]
pub struct ClientConfig {
    /// Maximum number of empty keep alive batches to get in a row before closing the
    /// connection. If 0 or undefined will send keep alive messages indefinitely.
    #[builder(default = "0")]
    pub stream_keep_alive_limit: usize,
    /// Maximum number of `Event`s in this stream (over all partitions being streamed
    /// in this
    /// connection).
    ///
    /// * If 0 or undefined, will stream batches indefinitely.
    /// * Stream initialization will fail if `stream_limit` is lower than `batch_limit`.
    #[builder(default = "0")]
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
    #[builder(default = "Duration::from_secs(0)")]
    pub stream_timeout: Duration,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    ///
    ///  * If the amount of buffered Events reaches `batch_limit`
    /// before this `batch_flush_timeout` is reached, the messages are immediately
    /// flushed to the client and batch flush timer is reset.
    ///  * If 0 or undefined, will assume 30 seconds.
    #[builder(default = "Duration::from_secs(0)")]
    pub batch_flush_timeout: Duration,
    /// Maximum number of `Event`s in each chunk (and therefore per partition) of the
    /// stream.
    ///
    ///  * If 0 or unspecified will buffer Events indefinitely and flush on reaching of
    ///  `batch_flush_timeout`.
    #[builder(default = "0")]
    pub batch_limit: usize,
    /// The amount of uncommitted events Nakadi will stream before pausing the stream.
    /// When in paused state and commit comes - the stream will resume. Minimal value
    /// is 1.
    ///
    /// When using the concurrent worker you should adjust this value to safe your
    /// workers from running dry.
    #[builder(default = "0")]
    pub max_uncommitted_events: usize,
    /// The URI prefix for the Nakadi Host, e.g. "https://my.nakadi.com"
    pub nakadi_host: String,
    /// The subscription id to use, e.g. "https://my.nakadi.com"
    pub subscription_id: SubscriptionId,
}

impl ClientConfigBuilder {
    /// Create a builder from environment variables.
    ///
    /// For variables not found except 'NAKADION_NAKADI_HOST' a default will be set.
    ///
    /// Variables:
    ///
    /// * NAKADION_NAKADI_HOST: See `ConnectorSettings::nakadi_host`
    /// * NAKADION_SUBSCRIPTION_ID: See `ConnectorSettings::subscription_id`
    /// * NAKADION_MAX_UNCOMMITED_EVENTS: See
    /// `ConnectorSettings::max_uncommitted_events`
    /// * NAKADION_BATCH_LIMIT: See `ConnectorSettings::batch_limit`
    /// * NAKADION_BATCH_FLUSH_TIMEOUT_SECS: See
    /// `ConnectorSettings::batch_flush_timeout`
    /// * NAKADION_STREAM_TIMEOUT_SECS: See `ConnectorSettings::stream_timeout`
    /// * NAKADION_STREAM_LIMIT: See `ConnectorSettings::stream_limit`
    /// * NAKADION_STREAM_KEEP_ALIVE_LIMIT: See
    /// `ConnectorSettings::stream_keep_alive_limit`
    pub fn from_env() -> Result<ClientConfigBuilder, String> {
        let builder = ClientConfigBuilder::default();
        let builder = if let Some(env_val) = env::var("NAKADION_STREAM_KEEP_ALIVE_LIMIT").ok() {
            builder.stream_keep_alive_limit(env_val.parse().map_err(|err| {
                format!(
                    "Could not parse 'NAKADION_STREAM_KEEP_ALIVE_LIMIT': {}",
                    err
                )
            })?)
        } else {
            warn!(
                "Environment variable 'NAKADION_STREAM_KEEP_ALIVE_LIMIT' not found. Using \
                 default."
            );
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_STREAM_LIMIT").ok() {
            builder.stream_limit(env_val
                .parse()
                .map_err(|err| format!("Could not parse 'NAKADION_STREAM_LIMIT': {}", err))?)
        } else {
            warn!("Environment variable 'NAKADION_STREAM_LIMIT' not found. Using default.");
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_STREAM_TIMEOUT_SECS").ok() {
            builder.stream_timeout(Duration::from_secs(env_val.parse().map_err(|err| {
                format!("Could not parse 'NAKADION_STREAM_TIMEOUT_SECS': {}", err)
            })?))
        } else {
            warn!("Environment variable 'NAKADION_STREAM_TIMEOUT_SECS' not found. Using default.");
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_BATCH_FLUSH_TIMEOUT_SECS").ok() {
            builder.batch_flush_timeout(Duration::from_secs(env_val.parse().map_err(|err| {
                format!(
                    "Could not parse 'NAKADION_BATCH_FLUSH_TIMEOUT_SECS': {}",
                    err
                )
            })?))
        } else {
            warn!(
                "Environment variable 'NAKADION_BATCH_FLUSH_TIMEOUT_SECS' not found. Using \
                 default."
            );
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_BATCH_LIMIT").ok() {
            builder.batch_limit(env_val
                .parse()
                .map_err(|err| format!("Could not parse 'NAKADION_BATCH_LIMIT': {}", err))?)
        } else {
            warn!("Environment variable 'NAKADION_BATCH_LIMIT' not found. Using default.");
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_MAX_UNCOMMITED_EVENTS").ok() {
            builder.max_uncommitted_events(env_val.parse().map_err(|err| {
                format!("Could not parse 'NAKADION_MAX_UNCOMMITED_EVENTS': {}", err)
            })?)
        } else {
            warn!(
                "Environment variable 'NAKADION_MAX_UNCOMMITED_EVENTS' not found. Using \
                 default."
            );
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_NAKADI_HOST").ok() {
            builder.nakadi_host(env_val)
        } else {
            warn!(
                "Environment variable 'NAKADION_NAKADI_HOST' not found. It will have to be set \
                 manually."
            );
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_SUBSCRIPTION_ID").ok() {
            builder.subscription_id(SubscriptionId(env_val))
        } else {
            warn!(
                "Environment variable 'NAKADION_SUBSCRIPTION_ID' not found. It will have \
                 to be set manually."
            );
            builder
        };
        Ok(builder)
    }
}

#[derive(Clone)]
pub struct Client {
    connect_url: String,
    stats_url: String,
    commit_url: String,
    subscription_id: SubscriptionId,
    http_client: HttpClient,
    token_provider: Arc<ProvidesAccessToken + Send + Sync + 'static>,
}

impl Client {
    pub fn new<T: ProvidesAccessToken + Send + Sync + 'static>(
        settings: ClientConfig,
        token_provider: T,
    ) -> Client {
        let connect_url = create_connect_url(&settings);
        let stats_url = create_stats_url(&settings);
        let commit_url = create_commit_url(&settings);

        let http_client = HttpClient::new();

        Client {
            http_client,
            connect_url,
            stats_url,
            commit_url,
            subscription_id: settings.subscription_id,
            token_provider: Arc::new(token_provider),
        }
    }

    pub fn attempt_commit<T: AsRef<[u8]>>(
        &self,
        stream_id: StreamId,
        cursors: &[T],
    ) -> ::std::result::Result<CommitStatus, CommitError> {
        let mut headers = Headers::new();
        if let Some(AccessToken(token)) = self.token_provider.get_token()? {
            headers.set(Authorization(Bearer { token }));
        }

        headers.set(XNakadiStreamId(stream_id.0));
        headers.set(ContentType::json());

        let body = make_cursors_body(cursors);

        let mut response = self.http_client
            .post(&self.commit_url)
            .headers(headers)
            .body(body)
            .send()?;

        match response.status() {
            // All cursors committed but at least one did not increase an offset.
            StatusCode::Ok => Ok(CommitStatus::NotAllOffsetsIncreased),
            // All cursors committed and all increased the offset.
            StatusCode::NoContent => Ok(CommitStatus::AllOffsetsIncreased),
            StatusCode::NotFound => Err(CommitError::SubscriptionNotFound(format!(
                "{}: {}",
                StatusCode::NotFound,
                read_response_body(&mut response)
            ))),
            StatusCode::UnprocessableEntity => Err(CommitError::UnprocessableEntity(format!(
                "{}: {}",
                StatusCode::UnprocessableEntity,
                read_response_body(&mut response)
            ))),
            StatusCode::Forbidden => Err(CommitError::Client(format!(
                "{}: {}",
                StatusCode::Forbidden,
                "<Nakadion: Nakadi said forbidden.>"
            ))),
            other_status if other_status.is_client_error() => Err(CommitError::Client(format!(
                "{}: {}",
                other_status,
                read_response_body(&mut response)
            ))),
            other_status if other_status.is_server_error() => Err(CommitError::Server(format!(
                "{}: {}",
                other_status,
                read_response_body(&mut response)
            ))),
            other_status => Err(CommitError::Other(format!(
                "{}: {}",
                other_status,
                read_response_body(&mut response)
            ))),
        }
    }
}

fn create_connect_url(settings: &ClientConfig) -> String {
    let mut connect_url = String::new();
    connect_url.push_str(&settings.nakadi_host);
    if !connect_url.ends_with("/") {
        connect_url.push('/');
    }
    connect_url.push_str("subscriptions/");
    connect_url.push_str(&settings.subscription_id.0);
    connect_url.push_str("/events");

    let mut connect_params = Vec::new();
    if settings.stream_keep_alive_limit != 0 {
        connect_params.push(format!(
            "stream_keep_alive_limit={}",
            settings.stream_keep_alive_limit
        ));
    }
    if settings.stream_limit != 0 {
        connect_params.push(format!("stream_limit={}", settings.stream_limit));
    }
    if settings.stream_timeout != Duration::from_secs(0) {
        connect_params.push(format!(
            "stream_timeout={}",
            settings.stream_timeout.as_secs()
        ));
    }
    if settings.batch_flush_timeout != Duration::from_secs(0) {
        connect_params.push(format!(
            "batch_flush_timeout={}",
            settings.batch_flush_timeout.as_secs()
        ));
    }
    if settings.batch_limit != 0 {
        connect_params.push(format!("batch_limit={}", settings.batch_limit));
    }
    if settings.max_uncommitted_events != 0 {
        connect_params.push(format!(
            "max_uncommitted_events={}",
            settings.max_uncommitted_events
        ));
    }

    if !connect_params.is_empty() {
        connect_url.push('?');
        connect_url.push_str(&connect_params.join("&"));
    };

    connect_url
}

fn create_commit_url(settings: &ClientConfig) -> String {
    let mut commit_url = String::new();
    commit_url.push_str(&settings.nakadi_host);
    if !commit_url.ends_with("/") {
        commit_url.push('/');
    }
    commit_url.push_str("subscriptions/");
    commit_url.push_str(&settings.subscription_id.0);
    commit_url.push_str("/stats");
    commit_url
}

fn create_stats_url(settings: &ClientConfig) -> String {
    let mut commit_url = String::new();
    commit_url.push_str(&settings.nakadi_host);
    if !commit_url.ends_with("/") {
        commit_url.push('/');
    }
    commit_url.push_str("subscriptions/");
    commit_url.push_str(&settings.subscription_id.0);
    commit_url.push_str("/cursors");
    commit_url
}

impl StreamingClient for Client {
    type LineIterator = NakadiLineIterator;
    fn connect(&self) -> ::std::result::Result<(StreamId, NakadiLineIterator), ConnectError> {
        let mut headers = Headers::new();
        if let Some(AccessToken(token)) = self.token_provider.get_token()? {
            headers.set(Authorization(Bearer { token }));
        }

        let mut response = self.http_client
            .get(&self.connect_url)
            .headers(headers)
            .send()?;

        match response.status() {
            StatusCode::Ok => {
                let stream_id = if let Some(stream_id) = response
                    .headers()
                    .get::<XNakadiStreamId>()
                    .map(|v| StreamId(v.to_string()))
                {
                    stream_id
                } else {
                    return Err(ConnectError::Server {
                        message: "The response lacked the \
                                  'X-Nakadi-StreamId' header."
                            .to_string(),
                    });
                };
                Ok((stream_id, NakadiLineIterator::new(response)))
            }
            StatusCode::Forbidden => Err(ConnectError::Client {
                message: format!(
                    "{}: {}",
                    StatusCode::Forbidden,
                    "Nakadion: Nakadi said forbidden."
                ),
            }),
            StatusCode::Conflict => Err(ConnectError::Client {
                message: format!(
                    "{}: {}",
                    StatusCode::Conflict,
                    read_response_body(&mut response)
                ),
            }),
            other_status if other_status.is_client_error() => Err(ConnectError::Client {
                message: format!("{}: {}", other_status, read_response_body(&mut response)),
            }),
            other_status if other_status.is_server_error() => Err(ConnectError::Server {
                message: format!("{}: {}", other_status, read_response_body(&mut response)),
            }),
            other_status => Err(ConnectError::Other {
                message: format!("{}: {}", other_status, read_response_body(&mut response)),
            }),
        }
    }

    fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
    }

    /*    fn stats(&self) -> ::std::result::Result<SubscriptionStats, StatsError> {
        let mut headers = Headers::new();
        if let Some(token) = self.token_provider.get_token()? {
            headers.set(Authorization(Bearer { token: token.0 }));
        };

        let mut response = self.http_client
            .get(&self.stats_url)
            .headers(headers)
            .send()?;
        match response.status() {
            StatusCode::Ok => {
                let parsed = serde_json::from_reader(response)?;
                Ok(parsed)
            }
            StatusCode::Forbidden => Err(StatsError::Client {
                message: format!(
                    "{}: {}",
                    StatusCode::Forbidden,
                    "<Nakadion: Nakadi said forbidden.>"
                ),
            }),
            other_status if other_status.is_client_error() => Err(StatsError::Client {
                message: format!("{}: {}", other_status, read_response_body(&mut response)),
            }),
            other_status if other_status.is_server_error() => Err(StatsError::Server {
                message: format!("{}: {}", other_status, read_response_body(&mut response)),
            }),
            other_status => Err(StatsError::Other {
                message: format!("{}: {}", other_status, read_response_body(&mut response)),
            }),
        }
    }*/

    fn commit<T: AsRef<[u8]>>(
        &self,
        stream_id: StreamId,
        cursors: &[T],
    ) -> ::std::result::Result<CommitStatus, CommitError> {
        let mut op = || {
            self.attempt_commit(stream_id.clone(), cursors)
                .map_err(|err| match err {
                    err @ CommitError::Client { .. } => BackoffError::Permanent(err),
                    err => BackoffError::Transient(err),
                })
        };

        let notify = |err, dur| {
            warn!(
                "Stream {} - Commit Error happened at {:?}: {}",
                stream_id.clone(),
                dur,
                err
            );
        };

        let mut backoff = ExponentialBackoff::default();

        match op.retry_notify(&mut backoff, notify) {
            Ok(x) => Ok(x),
            Err(BackoffError::Transient(err)) => Err(err),
            Err(BackoffError::Permanent(err)) => Err(err),
        }
    }
}

fn read_response_body(response: &mut Response) -> String {
    let mut buf = String::new();
    response
        .read_to_string(&mut buf)
        .map(|_| buf)
        .unwrap_or("<Nakadion: Could not read body.>".to_string())
}

fn make_cursors_body<T: AsRef<[u8]>>(cursors: &[T]) -> Vec<u8> {
    let bytes_required: usize = cursors.iter().map(|c| c.as_ref().len()).sum();
    let mut body = Vec::with_capacity(bytes_required + 20);
    body.extend(b"{items=[");
    for i in 0..cursors.len() {
        body.extend(cursors[i].as_ref().iter().cloned());
        if i != cursors.len() - 1 {
            body.push(b',');
        }
    }
    body.extend(b"}]");
    body
}

#[derive(Fail, Debug)]
pub enum ConnectError {
    #[fail(display = "Token Error on connect: {}", cause)]
    TokenError {
        #[cause] cause: TokenError,
    },
    #[fail(display = "Connection Error: {}", message)] Connection {
        message: String,
    },
    #[fail(display = "Server Error: {}", message)] Server {
        message: String,
    },
    #[fail(display = "Client Error: {}", message)] Client {
        message: String,
    },
    #[fail(display = "Other Error: {}", message)] Other {
        message: String,
    },
}

#[derive(Debug)]
pub enum CommitStatus {
    AllOffsetsIncreased,
    NotAllOffsetsIncreased,
}

#[derive(Fail, Debug)]
pub enum CommitError {
    #[fail(display = "Token Error on commit: {}", _0)] TokenError(String),
    #[fail(display = "Connection Error: {}", _0)] Connection(String),
    #[fail(display = "Subscription not found: {}", _0)] SubscriptionNotFound(String),
    #[fail(display = "Unprocessable Entity: {}", _0)] UnprocessableEntity(String),
    #[fail(display = "Server Error: {}", _0)] Server(String),
    #[fail(display = "Client Error: {}", _0)] Client(String),
    #[fail(display = "Other Error: {}", _0)] Other(String),
}

#[derive(Fail, Debug)]
pub enum StatsError {
    #[fail(display = "Token Error on stats: {}", _0)] TokenError(String),
    #[fail(display = "Connection Error: {}", _0)] Connection(String),
    #[fail(display = "Server Error: {}", _0)] Server(String),
    #[fail(display = "Client Error: {}", _0)] Client(String),
    #[fail(display = "Parse Error: {}", _0)] Parse(String),
    #[fail(display = "Other Error: {}", _0)] Other(String),
}

impl From<TokenError> for ConnectError {
    fn from(cause: TokenError) -> ConnectError {
        ConnectError::TokenError { cause }
    }
}

impl From<::reqwest::Error> for ConnectError {
    fn from(e: ::reqwest::Error) -> ConnectError {
        ConnectError::Connection {
            message: format!("Connection Error: {}", e),
        }
    }
}

impl From<TokenError> for CommitError {
    fn from(e: TokenError) -> CommitError {
        CommitError::TokenError(format!("{}", e))
    }
}

impl From<::reqwest::Error> for CommitError {
    fn from(e: ::reqwest::Error) -> CommitError {
        CommitError::Connection(format!("{}", e))
    }
}

impl From<TokenError> for StatsError {
    fn from(e: TokenError) -> StatsError {
        StatsError::TokenError(format!("{}", e))
    }
}

impl From<serde_json::error::Error> for StatsError {
    fn from(e: serde_json::error::Error) -> StatsError {
        StatsError::Parse(format!("{}", e))
    }
}

impl From<::reqwest::Error> for StatsError {
    fn from(e: ::reqwest::Error) -> StatsError {
        StatsError::Connection(format!("{}", e))
    }
}

pub mod stats {
    /// Information on a partition
    #[derive(Debug, Deserialize)]
    pub struct PartitionInfo {
        pub partition: String,
        pub stream_id: String,
        pub unconsumed_events: usize,
    }

    /// An `EventType` can be published on multiple partitions.
    #[derive(Debug, Deserialize)]
    pub struct EventTypeInfo {
        pub event_type: String,
        pub partitions: Vec<PartitionInfo>,
    }

    impl EventTypeInfo {
        /// Returns the number of partitions this `EventType` is
        /// published over.
        pub fn num_partitions(&self) -> usize {
            self.partitions.len()
        }
    }

    /// A stream can provide multiple `EventTypes` where each of them can have
    /// its own partitioning setup.
    #[derive(Debug, Deserialize, Default)]
    pub struct SubscriptionStats {
        #[serde(rename = "items")] pub event_types: Vec<EventTypeInfo>,
    }

    impl SubscriptionStats {
        /// Returns the number of partitions of the `EventType`
        /// that has the most partitions.
        pub fn max_partitions(&self) -> usize {
            self.event_types
                .iter()
                .map(|et| et.num_partitions())
                .max()
                .unwrap_or(0)
        }
    }
}
