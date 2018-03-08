use std::sync::Arc;
use std::env;
use std::time::{Duration, Instant};
use std::io::{BufRead, BufReader, Error as IoError, Read, Split};

use auth::{AccessToken, ProvidesAccessToken, TokenError};
use nakadi::model::{FlowId, StreamId, SubscriptionId};

use reqwest::{Client as HttpClient, ClientBuilder as HttpClientBuilder, Response};
use reqwest::StatusCode;
use reqwest::header::{Authorization, Bearer, ContentType, Headers};
use serde_json;
use backoff::{Error as BackoffError, ExponentialBackoff, Operation};
use failure::*;

header! { (XNakadiStreamId, "X-Nakadi-StreamId") => [String] }
header! { (XFlowId, "X-Flow-Id") => [String] }

const LINE_SPLIT_BYTE: u8 = b'\n';

pub struct RawLine {
    pub bytes: Vec<u8>,
    pub received_at: Instant,
}

pub type LineResult = ::std::result::Result<RawLine, IoError>;

pub struct NakadiLineIterator {
    lines: Split<BufReader<Response>>,
}

impl NakadiLineIterator {
    pub fn new(response: Response) -> Self {
        let reader = BufReader::with_capacity(1024 * 1024, response);
        NakadiLineIterator {
            lines: reader.split(LINE_SPLIT_BYTE),
        }
    }
}

impl Iterator for NakadiLineIterator {
    type Item = LineResult;

    fn next(&mut self) -> Option<LineResult> {
        self.lines.next().map(|r| {
            r.map(|l| RawLine {
                bytes: l,
                received_at: Instant::now(),
            })
        })
    }
}

/// A client to the Nakadi Event Broker
pub trait StreamingClient {
    type LineIterator: Iterator<Item = LineResult>;
    fn connect(
        &self,
        subscription_id: &SubscriptionId,
        flow_id: FlowId,
    ) -> ::std::result::Result<(StreamId, Self::LineIterator), ConnectError>;
}

/// Settings for establishing a connection to `Nakadi`.
#[derive(Debug, Clone)]
pub struct Config {
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
}

pub struct ConfigBuilder {
    pub stream_keep_alive_limit: Option<usize>,
    pub stream_limit: Option<usize>,
    pub stream_timeout: Option<Duration>,
    pub batch_flush_timeout: Option<Duration>,
    pub batch_limit: Option<usize>,
    pub max_uncommitted_events: Option<usize>,
    pub nakadi_host: Option<String>,
}

impl Default for ConfigBuilder {
    fn default() -> ConfigBuilder {
        ConfigBuilder {
            stream_keep_alive_limit: None,
            stream_limit: None,
            stream_timeout: None,
            batch_flush_timeout: None,
            batch_limit: None,
            max_uncommitted_events: None,
            nakadi_host: None,
        }
    }
}

impl ConfigBuilder {
    /// Maximum number of empty keep alive batches to get in a row before closing the
    /// connection. If 0 or undefined will send keep alive messages indefinitely.
    pub fn stream_keep_alive_limit(mut self, stream_keep_alive_limit: usize) -> ConfigBuilder {
        self.stream_keep_alive_limit = Some(stream_keep_alive_limit);
        self
    }
    /// Maximum number of `Event`s in this stream (over all partitions being streamed
    /// in this
    /// connection).
    ///
    /// * If 0 or undefined, will stream batches indefinitely.
    /// * Stream initialization will fail if `stream_limit` is lower than `batch_limit`.
    pub fn stream_limit(mut self, stream_limit: usize) -> ConfigBuilder {
        self.stream_limit = Some(stream_limit);
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
    pub fn stream_timeout(mut self, stream_timeout: Duration) -> ConfigBuilder {
        self.stream_timeout = Some(stream_timeout);
        self
    }
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    ///
    ///  * If the amount of buffered Events reaches `batch_limit`
    /// before this `batch_flush_timeout` is reached, the messages are immediately
    /// flushed to the client and batch flush timer is reset.
    ///  * If 0 or undefined, will assume 30 seconds.
    pub fn batch_flush_timeout(mut self, batch_flush_timeout: Duration) -> ConfigBuilder {
        self.batch_flush_timeout = Some(batch_flush_timeout);
        self
    }
    /// Maximum number of `Event`s in each chunk (and therefore per partition) of the
    /// stream.
    ///
    ///  * If 0 or unspecified will buffer Events indefinitely and flush on reaching of
    ///  `batch_flush_timeout`.
    pub fn batch_limit(mut self, batch_limit: usize) -> ConfigBuilder {
        self.batch_limit = Some(batch_limit);
        self
    }
    /// The amount of uncommitted events Nakadi will stream before pausing the stream.
    /// When in paused state and commit comes - the stream will resume. Minimal value
    /// is 1.
    ///
    /// When using the concurrent worker you should adjust this value to safe your
    /// workers from running dry.
    pub fn max_uncommitted_events(mut self, max_uncommitted_events: usize) -> ConfigBuilder {
        self.max_uncommitted_events = Some(max_uncommitted_events);
        self
    }
    /// The URI prefix for the Nakadi Host, e.g. "https://my.nakadi.com"
    pub fn nakadi_host<T: Into<String>>(mut self, nakadi_host: T) -> ConfigBuilder {
        self.nakadi_host = Some(nakadi_host.into());
        self
    }

    /// Create a builder from environment variables.
    ///
    /// For variables not found except 'NAKADION_NAKADI_HOST' a default will be set.
    ///
    /// Variables:
    ///
    /// * NAKADION_NAKADI_HOST: See `ConnectorSettings::nakadi_host`
    /// * NAKADION_MAX_UNCOMMITED_EVENTS: See
    /// `ConnectorSettings::max_uncommitted_events`
    /// * NAKADION_BATCH_LIMIT: See `ConnectorSettings::batch_limit`
    /// * NAKADION_BATCH_FLUSH_TIMEOUT_SECS: See
    /// `ConnectorSettings::batch_flush_timeout`
    /// * NAKADION_STREAM_TIMEOUT_SECS: See `ConnectorSettings::stream_timeout`
    /// * NAKADION_STREAM_LIMIT: See `ConnectorSettings::stream_limit`
    /// * NAKADION_STREAM_KEEP_ALIVE_LIMIT: See
    /// `ConnectorSettings::stream_keep_alive_limit`
    pub fn from_env() -> Result<ConfigBuilder, Error> {
        let builder = ConfigBuilder::default();
        let builder = if let Some(env_val) = env::var("NAKADION_STREAM_KEEP_ALIVE_LIMIT").ok() {
            builder.stream_keep_alive_limit(env_val
                .parse::<usize>()
                .context("Could not parse 'NAKADION_STREAM_KEEP_ALIVE_LIMIT'")?)
        } else {
            warn!(
                "Environment variable 'NAKADION_STREAM_KEEP_ALIVE_LIMIT' not found. Using \
                 default."
            );
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_STREAM_LIMIT").ok() {
            builder.stream_limit(env_val
                .parse::<usize>()
                .context("Could not parse 'NAKADION_STREAM_LIMIT'")?)
        } else {
            warn!("Environment variable 'NAKADION_STREAM_LIMIT' not found. Using default.");
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_STREAM_TIMEOUT_SECS").ok() {
            builder.stream_timeout(Duration::from_secs(env_val
                .parse::<u64>()
                .context("Could not parse 'NAKADION_STREAM_TIMEOUT_SECS'")?))
        } else {
            warn!("Environment variable 'NAKADION_STREAM_TIMEOUT_SECS' not found. Using default.");
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_BATCH_FLUSH_TIMEOUT_SECS").ok() {
            builder.batch_flush_timeout(Duration::from_secs(env_val
                .parse::<u64>()
                .context("Could not parse 'NAKADION_BATCH_FLUSH_TIMEOUT_SECS'")?))
        } else {
            warn!(
                "Environment variable 'NAKADION_BATCH_FLUSH_TIMEOUT_SECS' not found. Using \
                 default."
            );
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_BATCH_LIMIT").ok() {
            builder.batch_limit(env_val
                .parse::<usize>()
                .context("Could not parse 'NAKADION_BATCH_LIMIT'")?)
        } else {
            warn!("Environment variable 'NAKADION_BATCH_LIMIT' not found. Using default.");
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_MAX_UNCOMMITED_EVENTS").ok() {
            builder.max_uncommitted_events(env_val
                .parse::<usize>()
                .context("Could not parse 'NAKADION_MAX_UNCOMMITED_EVENTS'")?)
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
        Ok(builder)
    }

    pub fn build(self) -> Result<Config, Error> {
        let nakadi_host = if let Some(nakadi_host) = self.nakadi_host {
            nakadi_host
        } else {
            bail!("Nakadi host required");
        };
        Ok(Config {
            stream_keep_alive_limit: self.stream_keep_alive_limit.unwrap_or(0),
            stream_limit: self.stream_keep_alive_limit.unwrap_or(0),
            stream_timeout: self.stream_timeout.unwrap_or(Duration::from_secs(0)),
            batch_flush_timeout: self.batch_flush_timeout.unwrap_or(Duration::from_secs(0)),
            batch_limit: self.batch_limit.unwrap_or(0),
            max_uncommitted_events: self.max_uncommitted_events.unwrap_or(0),
            nakadi_host: nakadi_host,
        })
    }

    pub fn build_client<T>(self, token_provider: T) -> Result<NakadiStreamingClient, Error>
    where
        T: ProvidesAccessToken + Send + Sync + 'static,
    {
        self.build_client_with_shared_access_token_provider(Arc::new(token_provider))
    }

    pub fn build_client_with_shared_access_token_provider(
        self,
        token_provider: Arc<ProvidesAccessToken + Send + Sync + 'static>,
    ) -> Result<NakadiStreamingClient, Error> {
        let config = self.build().context("Could not build client config")?;

        NakadiStreamingClient::with_shared_access_token_provider(config, token_provider)
    }
}

#[derive(Clone)]
pub struct NakadiStreamingClient {
    http_client: HttpClient,
    token_provider: Arc<ProvidesAccessToken + Send + Sync + 'static>,
    config: Config,
}

impl NakadiStreamingClient {
    pub fn new<T: ProvidesAccessToken + Send + Sync + 'static>(
        config: Config,
        token_provider: T,
    ) -> Result<NakadiStreamingClient, Error> {
        NakadiStreamingClient::with_shared_access_token_provider(config, Arc::new(token_provider))
    }

    pub fn with_shared_access_token_provider(
        config: Config,
        token_provider: Arc<ProvidesAccessToken + Send + Sync + 'static>,
    ) -> Result<NakadiStreamingClient, Error> {
        let http_client = HttpClientBuilder::new()
            .timeout(None)
            .build()
            .context("Could not create HTTP client")?;

        Ok(NakadiStreamingClient {
            http_client,
            token_provider,
            config,
        })
    }
}

fn create_connect_url(config: &Config, subscription_id: &SubscriptionId) -> String {
    let mut connect_url = String::new();
    connect_url.push_str(&config.nakadi_host);
    if !connect_url.ends_with("/") {
        connect_url.push('/');
    }
    connect_url.push_str("subscriptions/");
    connect_url.push_str(&subscription_id.0);
    connect_url.push_str("/events");

    let mut connect_params = Vec::new();
    if config.stream_keep_alive_limit != 0 {
        connect_params.push(format!(
            "stream_keep_alive_limit={}",
            config.stream_keep_alive_limit
        ));
    }
    if config.stream_limit != 0 {
        connect_params.push(format!("stream_limit={}", config.stream_limit));
    }
    if config.stream_timeout != Duration::from_secs(0) {
        connect_params.push(format!(
            "stream_timeout={}",
            config.stream_timeout.as_secs()
        ));
    }
    if config.batch_flush_timeout != Duration::from_secs(0) {
        connect_params.push(format!(
            "batch_flush_timeout={}",
            config.batch_flush_timeout.as_secs()
        ));
    }
    if config.batch_limit != 0 {
        connect_params.push(format!("batch_limit={}", config.batch_limit));
    }
    if config.max_uncommitted_events != 0 {
        connect_params.push(format!(
            "max_uncommitted_events={}",
            config.max_uncommitted_events
        ));
    }

    if !connect_params.is_empty() {
        connect_url.push('?');
        connect_url.push_str(&connect_params.join("&"));
    };

    connect_url
}

impl StreamingClient for NakadiStreamingClient {
    type LineIterator = NakadiLineIterator;
    fn connect(
        &self,
        subscription_id: &SubscriptionId,
        flow_id: FlowId,
    ) -> ::std::result::Result<(StreamId, NakadiLineIterator), ConnectError> {
        let connect_url = create_connect_url(&self.config, &subscription_id);

        let mut headers = Headers::new();
        if let Some(AccessToken(token)) = self.token_provider.get_token()? {
            headers.set(Authorization(Bearer { token }));
        }

        headers.set(XFlowId(flow_id.0.clone()));

        let mut response = self.http_client.get(&connect_url).headers(headers).send()?;

        match response.status() {
            StatusCode::Ok => {
                let stream_id = if let Some(stream_id) = response
                    .headers()
                    .get::<XNakadiStreamId>()
                    .map(|v| StreamId(v.to_string()))
                {
                    stream_id
                } else {
                    return Err(ConnectError::Other(
                        "The response lacked the \
                         'X-Nakadi-StreamId' header."
                            .into(),
                        flow_id.clone(),
                    ));
                };
                Ok((stream_id, NakadiLineIterator::new(response)))
            }
            StatusCode::Forbidden => Err(ConnectError::Forbidden(
                format!(
                    "{}: {}",
                    StatusCode::Forbidden,
                    "Nakadion: Nakadi said forbidden."
                ),
                flow_id,
            )),
            StatusCode::Unauthorized => Err(ConnectError::Unauthorized(
                format!(
                    "{}: {}",
                    StatusCode::Unauthorized,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            StatusCode::NotFound => Err(ConnectError::SubscriptionNotFound(
                format!(
                    "{}: {}",
                    StatusCode::NotFound,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            StatusCode::BadRequest => Err(ConnectError::BadRequest(
                format!(
                    "{}: {}",
                    StatusCode::BadRequest,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            StatusCode::Conflict => Err(ConnectError::Conflict(
                format!(
                    "{}: {}",
                    StatusCode::Conflict,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            other_status => Err(ConnectError::Other(
                format!("{}: {}", other_status, read_response_body(&mut response)),
                flow_id,
            )),
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

#[derive(Fail, Debug)]
pub enum ConnectError {
    #[fail(display = "Token Error on connect: {}", _0)] Token(String),
    #[fail(display = "Connection Error: {}", _0)] Connection(String),
    #[fail(display = "Forbidden: {}", _0)] Forbidden(String, FlowId),
    #[fail(display = "Unauthorized: {}", _0)] Unauthorized(String, FlowId),
    #[fail(display = "Bad request: {}", _0)] BadRequest(String, FlowId),
    #[fail(display = "Conflict: {}", _0)] Conflict(String, FlowId),
    #[fail(display = "Subscription not found: {}", _0)] SubscriptionNotFound(String, FlowId),
    #[fail(display = "Other error: {}", _0)] Other(String, FlowId),
}

impl ConnectError {
    pub fn is_permanent(&self) -> bool {
        match *self {
            ConnectError::Forbidden(_, _) => true,
            ConnectError::BadRequest(_, _) => true,
            ConnectError::SubscriptionNotFound(_, _) => true,
            _ => false,
        }
    }
}

impl From<TokenError> for ConnectError {
    fn from(err: TokenError) -> ConnectError {
        ConnectError::Token(format!("Could not get token: {}", err))
    }
}

impl From<::reqwest::Error> for ConnectError {
    fn from(e: ::reqwest::Error) -> ConnectError {
        ConnectError::Connection(format!("Connection Error: {}", e))
    }
}
