//! Access to the REST API of Nakadi
//!
//! # Functionality
//!
//! * Commit cursors
//! * Create a new event type
//! * Delete an existing event type
//! * Create a new Subscription or get an exiting subscription
//! * Delete an existing subscription
use std::env;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use serde::{self, Deserialize, Deserializer, Serialize, Serializer};
use serde_json;

use backoff::{Error as BackoffError, ExponentialBackoff, Operation};
use failure::*;
use reqwest::header::{HeaderMap, CONTENT_TYPE};
use reqwest::StatusCode;
use reqwest::blocking::{Client as HttpClient, ClientBuilder as HttpClientBuilder, Response};

use auth::{AccessToken, ProvidesAccessToken, TokenError};
use nakadi::model::{FlowId, StreamId, SubscriptionId};

/// A REST client for the Nakadi API.
///
/// This accesses the REST API only and does not provide
/// functionality for streaming.
pub trait ApiClient {
    /// Commit the cursors encoded in the given
    /// bytes.
    ///
    /// A commit can only be done on a valid stream identified by
    /// its id.
    ///
    /// This method will retry for 500ms in case of a failure.
    ///
    /// # Errors
    ///
    /// The cursors could not be committed.
    fn commit_cursors<T: AsRef<[u8]>>(
        &self,
        subscription_id: &SubscriptionId,
        stream_id: &StreamId,
        cursors: &[T],
        flow_id: FlowId,
    ) -> ::std::result::Result<CommitStatus, CommitError> {
        self.commit_cursors_budgeted(
            subscription_id,
            stream_id,
            cursors,
            flow_id,
            Duration::from_millis(500),
        )
    }

    /// Commit the cursors encoded in the given
    /// bytes.
    ///
    /// A commit can only be done on a valid stream identified by
    /// its id.
    ///
    /// This method will retry for the `Duration`
    /// defined by `budget` in case of a failure.
    ///
    /// # Errors
    ///
    /// The cursors could not be committed.
    fn commit_cursors_budgeted<T: AsRef<[u8]>>(
        &self,
        subscription_id: &SubscriptionId,
        stream_id: &StreamId,
        cursors: &[T],
        flow_id: FlowId,
        budget: Duration,
    ) -> ::std::result::Result<CommitStatus, CommitError>;

    /// Deletes an event type.
    ///
    /// # Errors
    ///
    /// The event type could not be deleted.
    fn delete_event_type(&self, event_type_name: &str) -> Result<(), DeleteEventTypeError>;

    /// Creates an event type defined by `EventTypeDefinition`.
    ///
    /// # Errors
    ///
    /// The event type could not be created.
    fn create_event_type(
        &self,
        event_type: &EventTypeDefinition,
    ) -> Result<(), CreateEventTypeError>;

    /// Creates an new subscription defined by a `SubscriptionRequest`.
    ///
    /// Trying to create a `Subscription` that already existed is not
    /// considered a failure. See `CreateSubscriptionStatus` for mor details.
    ///
    /// # Errors
    ///
    /// The subscription could not be created.
    fn create_subscription(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<CreateSubscriptionStatus, CreateSubscriptionError>;

    /// Deletes a `Subscription` identified by a `SubscriptionId`.
    ///
    /// # Errors
    ///
    /// The subscription could not be deleted.
    fn delete_subscription(&self, id: &SubscriptionId) -> Result<(), DeleteSubscriptionError>;
}

/// Settings for establishing a connection to `Nakadi`.
///
/// You can use the `ConfigBuilder` in this module to easily
/// create a configuration.
#[derive(Debug)]
pub struct Config {
    /// The Nakadi host
    pub nakadi_host: String,
    /// Timeout after which a connection to the REST API
    /// is aborted. If `None` wait indefinitely.
    pub request_timeout: Duration,
}

pub struct ConfigBuilder {
    /// The Nakadi host
    pub nakadi_host: Option<String>,
    /// The request timeout when connecting to Nakadi
    pub request_timeout: Option<Duration>,
}

impl Default for ConfigBuilder {
    fn default() -> ConfigBuilder {
        ConfigBuilder {
            nakadi_host: None,
            request_timeout: Some(Duration::from_secs(1)),
        }
    }
}

impl ConfigBuilder {
    /// The URI prefix for the Nakadi Host, e.g. "https://my.nakadi.com"
    pub fn nakadi_host<T: Into<String>>(mut self, nakadi_host: T) -> ConfigBuilder {
        self.nakadi_host = Some(nakadi_host.into());
        self
    }

    /// Timeout after which a connection to the REST API
    /// is aborted. If `None` wait indefinitely
    pub fn request_timeout(mut self, request_timeout: Duration) -> ConfigBuilder {
        self.request_timeout = Some(request_timeout);
        self
    }

    /// Create a builder from environment variables.
    ///
    /// # Environment Variables:
    ///
    /// For variables not found except 'NAKADION_NAKADI_HOST' a default will be set.
    ///
    /// Variables:
    ///
    /// * NAKADION_NAKADI_HOST: Host address of Nakadi. The host is mandatory.
    /// * NAKADION_REQUEST_TIMEOUT_MS: Timeout in ms after which a connection to the REST API
    /// is aborted. This is optional and defaults to 1 second.
    ///
    /// # Errors
    ///
    /// Fails if a value can not be parsed from an existing environment variable.
    pub fn from_env() -> Result<ConfigBuilder, Error> {
        let builder = ConfigBuilder::default();
        let builder = if let Ok(env_val) = env::var("NAKADION_NAKADI_HOST") {
            builder.nakadi_host(env_val)
        } else {
            warn!(
                "Environment variable 'NAKADION_NAKADI_HOST' not found. It will have to be set \
                 manually."
            );
            builder
        };
        let builder = if let Ok(env_val) = env::var("NAKADION_REQUEST_TIMEOUT_MS") {
            builder.request_timeout(Duration::from_millis(
                env_val
                    .parse::<u64>()
                    .context("Could not parse 'NAKADION_REQUEST_TIMEOUT_MS'")?,
            ))
        } else {
            warn!(
                "Environment variable 'NAKADION_REQUEST_TIMEOUT_MS' not found. It will have be set \
                 to the default."
            );
            builder
        };
        Ok(builder)
    }

    /// Build a config from this builder.
    ///
    /// # Errors
    ///
    /// Fails if `nakadi_host` is not set.
    pub fn build(self) -> Result<Config, Error> {
        let nakadi_host = if let Some(nakadi_host) = self.nakadi_host {
            nakadi_host
        } else {
            bail!("Nakadi host required");
        };
        Ok(Config {
            nakadi_host,
            request_timeout: self.request_timeout.unwrap_or(Duration::from_millis(500)),
        })
    }

    /// Directly build an API client from this builder.
    ///
    /// Takes ownership of the `token_provider`
    ///
    /// # Errors
    ///
    /// Fails if this builder is in an invalid state.
    pub fn build_client<T>(self, token_provider: T) -> Result<NakadiApiClient, Error>
    where
        T: ProvidesAccessToken + Send + Sync + 'static,
    {
        self.build_client_with_shared_access_token_provider(Arc::new(token_provider))
    }

    /// Directly build an API client builder.
    ///
    /// The `token_provider` can be a shared token provider.
    ///
    /// # Errors
    ///
    /// Fails if this builder is in an invalid state.
    pub fn build_client_with_shared_access_token_provider(
        self,
        token_provider: Arc<dyn ProvidesAccessToken + Send + Sync + 'static>,
    ) -> Result<NakadiApiClient, Error> {
        let config = self.build().context("Could not build client config")?;

        NakadiApiClient::with_shared_access_token_provider(config, token_provider)
    }
}

/// A REST client for the Nakadi API.
///
/// This accesses the REST API only and does not provide
/// functionality for streaming.
#[derive(Clone)]
pub struct NakadiApiClient {
    nakadi_host: String,
    http_client: HttpClient,
    token_provider: Arc<dyn ProvidesAccessToken + Send + Sync + 'static>,
}

impl NakadiApiClient {
    /// Build a new client with an owned access token provider.
    ///
    /// # Errors
    ///
    /// Fails if no HTTP client could be created.
    pub fn new<T: ProvidesAccessToken + Send + Sync + 'static>(
        config: Config,
        token_provider: T,
    ) -> Result<NakadiApiClient, Error> {
        NakadiApiClient::with_shared_access_token_provider(config, Arc::new(token_provider))
    }

    /// Build a new client with a shared access token provider.
    ///
    /// # Errors
    ///
    /// Fails if no HTTP client could be created.
    pub fn with_shared_access_token_provider(
        config: Config,
        token_provider: Arc<dyn ProvidesAccessToken + Send + Sync + 'static>,
    ) -> Result<NakadiApiClient, Error> {
        let http_client = HttpClientBuilder::new()
            .timeout(config.request_timeout)
            .build()
            .context("Could not create HTTP client")?;

        Ok(NakadiApiClient {
            nakadi_host: config.nakadi_host,
            http_client,
            token_provider,
        })
    }

    /// Try to commit the cursors encoded in the given
    /// bytes.
    ///
    /// A commit can only be done on a valid stream identified by
    /// its id.
    fn attempt_commit<T: AsRef<[u8]>>(
        &self,
        url: &str,
        stream_id: StreamId,
        cursors: &[T],
        flow_id: FlowId,
    ) -> ::std::result::Result<CommitStatus, CommitError> {
        let mut headers = HeaderMap::new();

        headers.insert("X-Flow-Id", flow_id.0.parse().unwrap());
        headers.insert("X-Nakadi-StreamId", stream_id.0.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let body = make_cursors_body(cursors);

        let request_builder = self.http_client.post(url).headers(headers).body(body);

        let request_builder = if let Some(AccessToken(token)) = self.token_provider.get_token()? {
            request_builder.bearer_auth(token)
        } else {
            request_builder
        };

        let mut response = request_builder.send()?;

        match response.status() {
            // All cursors committed but at least one did not increase an offset.
            StatusCode::OK => Ok(CommitStatus::NotAllOffsetsIncreased),
            // All cursors committed and all increased the offset.
            StatusCode::NO_CONTENT => Ok(CommitStatus::AllOffsetsIncreased),
            StatusCode::NOT_FOUND => Err(CommitError::SubscriptionNotFound(
                format!(
                    "{}: {}",
                    StatusCode::NOT_FOUND,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            StatusCode::UNPROCESSABLE_ENTITY => Err(CommitError::UnprocessableEntity(
                format!(
                    "{}: {}",
                    StatusCode::UNPROCESSABLE_ENTITY,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            StatusCode::FORBIDDEN => Err(CommitError::Client(
                format!(
                    "{}: {}",
                    StatusCode::FORBIDDEN,
                    "<Nakadion: Nakadi said forbidden.>"
                ),
                flow_id,
            )),
            other_status if other_status.is_client_error() => Err(CommitError::Client(
                format!("{}: {}", other_status, read_response_body(&mut response)),
                flow_id,
            )),
            other_status if other_status.is_server_error() => Err(CommitError::Server(
                format!("{}: {}", other_status, read_response_body(&mut response)),
                flow_id,
            )),
            other_status => Err(CommitError::Other(
                format!("{}: {}", other_status, read_response_body(&mut response)),
                flow_id,
            )),
        }
    }
}

impl ApiClient for NakadiApiClient {
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

    fn commit_cursors_budgeted<T: AsRef<[u8]>>(
        &self,
        subscription_id: &SubscriptionId,
        stream_id: &StreamId,
        cursors: &[T],
        flow_id: FlowId,
        budget: Duration,
    ) -> ::std::result::Result<CommitStatus, CommitError> {
        if cursors.is_empty() {
            return Ok(CommitStatus::NothingToCommit);
        }

        let url = format!(
            "{}/subscriptions/{}/cursors",
            self.nakadi_host, subscription_id.0
        );

        let mut op = || {
            self.attempt_commit(&url, stream_id.clone(), cursors, flow_id.clone())
                .map_err(|err| match err {
                    err @ CommitError::UnprocessableEntity { .. } => BackoffError::Permanent(err),
                    err @ CommitError::Client { .. } => BackoffError::Permanent(err),
                    err => BackoffError::Transient(err),
                })
        };

        let notify = |err, dur| {
            warn!(
                "[API-Client, stream={}, flow_id={}] - Retry notification. Commit error happened at {:?}: {}",
                stream_id,
                flow_id,
                dur,
                err
            );
        };

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(budget);
        backoff.initial_interval = Duration::from_millis(50);
        backoff.multiplier = 1.5;

        match op.retry_notify(&mut backoff, notify) {
            Ok(x) => Ok(x),
            Err(BackoffError::Transient(err)) => Err(err),
            Err(BackoffError::Permanent(err)) => Err(err),
        }
    }

    fn delete_event_type(&self, event_type_name: &str) -> Result<(), DeleteEventTypeError> {
        let url = format!("{}/event-types/{}", self.nakadi_host, event_type_name);

        let mut op = || match delete_event_type(&self.http_client, &url, &*self.token_provider) {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.is_retry_suggested() {
                    Err(BackoffError::Transient(err))
                } else {
                    Err(BackoffError::Permanent(err))
                }
            }
        };

        let notify = |err, dur| {
            warn!("Delete event type error happened {:?}: {}", dur, err);
        };

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(5));
        backoff.initial_interval = Duration::from_millis(100);
        backoff.multiplier = 1.5;

        match op.retry_notify(&mut backoff, notify) {
            Ok(_) => Ok(()),
            Err(BackoffError::Transient(err)) => Err(err),
            Err(BackoffError::Permanent(err)) => Err(err),
        }
    }

    fn create_event_type(
        &self,
        event_type: &EventTypeDefinition,
    ) -> Result<(), CreateEventTypeError> {
        let url = format!("{}/event-types", self.nakadi_host);

        let mut op = || match create_event_type(
            &self.http_client,
            &url,
            &*self.token_provider,
            event_type,
        ) {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.is_retry_suggested() {
                    Err(BackoffError::Transient(err))
                } else {
                    Err(BackoffError::Permanent(err))
                }
            }
        };

        let notify = |err, dur| {
            warn!("Create event type error happened {:?}: {}", dur, err);
        };

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(5));
        backoff.initial_interval = Duration::from_millis(100);
        backoff.multiplier = 1.5;

        match op.retry_notify(&mut backoff, notify) {
            Ok(_) => Ok(()),
            Err(BackoffError::Transient(err)) => Err(err),
            Err(BackoffError::Permanent(err)) => Err(err),
        }
    }

    fn create_subscription(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<CreateSubscriptionStatus, CreateSubscriptionError> {
        let url = format!("{}/subscriptions", self.nakadi_host);
        create_subscription(&self.http_client, &url, &*self.token_provider, request)
    }

    fn delete_subscription(&self, id: &SubscriptionId) -> Result<(), DeleteSubscriptionError> {
        let url = format!("{}/subscriptions/{}", self.nakadi_host, id.0);
        delete_subscription(&self.http_client, &url, &*self.token_provider)
    }
}

fn make_cursors_body<T: AsRef<[u8]>>(cursors: &[T]) -> Vec<u8> {
    let bytes_required: usize = cursors.iter().map(|c| c.as_ref().len()).sum();
    let mut body = Vec::with_capacity(bytes_required + 20);
    body.extend(b"{\"items\":[");
    for i in 0..cursors.len() {
        body.extend(cursors[i].as_ref().iter().cloned());
        if i != cursors.len() - 1 {
            body.push(b',');
        }
    }
    body.extend(b"]}");
    body
}

/// A commit attempt can result in multiple statuses
#[derive(Debug)]
pub enum CommitStatus {
    /// All the cursors have been successfully committed
    /// and all of them increased the offset of a cursor
    AllOffsetsIncreased,
    /// All the cursors have been successfully committed
    /// and at least one of them did not increase an offset.
    ///
    /// This usually happens when committing a keep alive line
    NotAllOffsetsIncreased,
    /// There was nothing to commit.
    NothingToCommit,
}

#[derive(Fail, Debug)]
pub enum CommitError {
    #[fail(display = "Token Error on commit: {}", _0)]
    TokenError(String),
    #[fail(display = "Connection Error: {}", _0)]
    Connection(String),
    #[fail(display = "Subscription not found(FlowId: {}): {}", _1, _0)]
    SubscriptionNotFound(String, FlowId),
    #[fail(display = "Unprocessable Entity(FlowId: {}): {}", _1, _0)]
    UnprocessableEntity(String, FlowId),
    #[fail(display = "Server Error(FlowId: {}): {}", _1, _0)]
    Server(String, FlowId),
    #[fail(display = "Client Error(FlowId: {}): {}", _1, _0)]
    Client(String, FlowId),
    #[fail(display = "Other Error(FlowId: {}): {}", _1, _0)]
    Other(String, FlowId),
}

#[derive(Fail, Debug)]
pub enum StatsError {
    #[fail(display = "Token Error on stats: {}", _0)]
    TokenError(String),
    #[fail(display = "Connection Error: {}", _0)]
    Connection(String),
    #[fail(display = "Server Error: {}", _0)]
    Server(String),
    #[fail(display = "Client Error: {}", _0)]
    Client(String),
    #[fail(display = "Parse Error: {}", _0)]
    Parse(String),
    #[fail(display = "Other Error: {}", _0)]
    Other(String),
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

fn create_event_type(
    client: &HttpClient,
    url: &str,
    token_provider: &dyn ProvidesAccessToken,
    event_type: &EventTypeDefinition,
) -> Result<(), CreateEventTypeError> {
    let mut headers = HeaderMap::new();

    headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

    let request_builder = client.post(url).headers(headers);

    let request_builder = match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => request_builder.bearer_auth(token),
        Ok(None) => request_builder,
        Err(err) => return Err(CreateEventTypeError::Other(err.to_string())),
    };

    match request_builder.json(event_type).send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::CREATED => Ok(()),
            StatusCode::UNAUTHORIZED => {
                let msg = read_response_body(response);
                Err(CreateEventTypeError::Unauthorized(msg))
            }
            StatusCode::CONFLICT => {
                let msg = read_response_body(response);
                Err(CreateEventTypeError::Conflict(msg))
            }
            StatusCode::UNPROCESSABLE_ENTITY => {
                let msg = read_response_body(response);
                Err(CreateEventTypeError::UnprocessableEntity(msg))
            }
            _ => {
                let msg = read_response_body(response);
                Err(CreateEventTypeError::Other(msg))
            }
        },
        Err(err) => Err(CreateEventTypeError::Other(format!("{}", err))),
    }
}

fn delete_event_type(
    client: &HttpClient,
    url: &str,
    token_provider: &dyn ProvidesAccessToken,
) -> Result<(), DeleteEventTypeError> {
    let request_builder = client.delete(url);

    let request_builder = match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => request_builder.bearer_auth(token),
        Ok(None) => request_builder,
        Err(err) => return Err(DeleteEventTypeError::Other(err.to_string())),
    };

    match request_builder.send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => {
                let msg = read_response_body(response);
                Err(DeleteEventTypeError::Unauthorized(msg))
            }
            StatusCode::FORBIDDEN => {
                let msg = read_response_body(response);
                Err(DeleteEventTypeError::Forbidden(msg))
            }
            _ => {
                let msg = read_response_body(response);
                Err(DeleteEventTypeError::Other(msg))
            }
        },
        Err(err) => Err(DeleteEventTypeError::Other(format!("{}", err))),
    }
}

fn delete_subscription(
    client: &HttpClient,
    url: &str,
    token_provider: &dyn ProvidesAccessToken,
) -> Result<(), DeleteSubscriptionError> {
    let request_builder = client.delete(url);

    let request_builder = match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => request_builder.bearer_auth(token),
        Ok(None) => request_builder,
        Err(err) => return Err(DeleteSubscriptionError::Other(err.to_string())),
    };

    match request_builder.send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => {
                let msg = read_response_body(response);
                Err(DeleteSubscriptionError::NotFound(msg))
            }
            StatusCode::UNAUTHORIZED => {
                let msg = read_response_body(response);
                Err(DeleteSubscriptionError::Unauthorized(msg))
            }
            StatusCode::FORBIDDEN => {
                let msg = read_response_body(response);
                Err(DeleteSubscriptionError::Forbidden(msg))
            }
            _ => {
                let msg = read_response_body(response);
                Err(DeleteSubscriptionError::Other(msg))
            }
        },
        Err(err) => Err(DeleteSubscriptionError::Other(format!("{}", err))),
    }
}

fn read_response_body(response: &mut Response) -> String {
    let mut buf = String::new();
    response
        .read_to_string(&mut buf)
        .map(|_| buf)
        .unwrap_or_else(|_| "<Could not read body.>".to_string())
}

fn create_subscription(
    client: &HttpClient,
    url: &str,
    token_provider: &dyn ProvidesAccessToken,
    request: &SubscriptionRequest,
) -> Result<CreateSubscriptionStatus, CreateSubscriptionError> {
    let mut headers = HeaderMap::new();

    headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

    let request_builder = client.post(url).headers(headers);

    let request_builder = match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => request_builder.bearer_auth(token),
        Ok(None) => request_builder,
        Err(err) => return Err(CreateSubscriptionError::Other(err.to_string())),
    };

    match request_builder.json(request).send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::OK => match serde_json::from_reader(response) {
                Ok(sub) => Ok(CreateSubscriptionStatus::AlreadyExists(sub)),
                Err(err) => Err(CreateSubscriptionError::Other(err.to_string())),
            },
            StatusCode::CREATED => match serde_json::from_reader(response) {
                Ok(sub) => Ok(CreateSubscriptionStatus::Created(sub)),
                Err(err) => Err(CreateSubscriptionError::Other(err.to_string())),
            },
            StatusCode::UNAUTHORIZED => {
                let msg = read_response_body(response);
                Err(CreateSubscriptionError::Unauthorized(msg))
            }
            StatusCode::UNPROCESSABLE_ENTITY => {
                let msg = read_response_body(response);
                Err(CreateSubscriptionError::UnprocessableEntity(msg))
            }
            StatusCode::BAD_REQUEST => {
                let msg = read_response_body(response);
                Err(CreateSubscriptionError::BadRequest(msg))
            }
            _ => {
                let msg = read_response_body(response);
                Err(CreateSubscriptionError::Other(msg))
            }
        },
        Err(err) => Err(CreateSubscriptionError::Other(format!("{}", err))),
    }
}

/// A request describing the subscription to be created.
///
/// The fields are described in more detail in
/// the [Nakadi Documentation](http://nakadi.io/manual.html#definition_Subscription)
///
/// # Serialization(JSON)
///
/// ```javascript
/// {
///     "owning_application": "my_app",
///     "event_types": ["my_event_type"],
///     "read_from": "begin"
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    /// This is the application which owns the subscription.
    pub owning_application: String,
    /// One or more event types that should
    /// be steamed on the subscription.
    pub event_types: Vec<String>,
    /// Defines the offset on the stream
    /// when creating a subscription.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_from: Option<ReadFrom>,
}

/// The definition of an existing subscription
///
/// The fields are described in more detail in
/// the [Nakadi Documentation](http://nakadi.io/manual.html#definition_Subscription)
#[derive(Debug, Clone, Deserialize)]
pub struct Subscription {
    /// The `SubscriptionId` of the subscription
    /// generated by Nakadi.
    pub id: SubscriptionId,
    /// The owner of the subscription as defined
    /// when the subscription was created.
    pub owning_application: String,
    /// The event types that are streamed over this subscription
    pub event_types: Vec<String>,
}

/// An offset on the stream when creating a subscription.
///
/// The enum is described in more detail in
/// the [Nakadi Documentation](http://nakadi.io/manual.html#definition_Subscription)
/// as the `read_from` member.
#[derive(Debug, Clone)]
pub enum ReadFrom {
    /// Read from the beginning of the stream.
    ///
    /// # Serialization(JSON)
    ///
    /// ```javascript
    /// "begin"
    /// ```
    Begin,
    /// Read from the end of the stream.
    ///
    /// # Serialization(JSON)
    ///
    /// ```javascript
    /// "end"
    /// ```
    End,
}
impl Serialize for ReadFrom {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            ReadFrom::Begin => serializer.serialize_str("begin"),
            ReadFrom::End => serializer.serialize_str("end"),
        }
    }
}

impl<'de> Deserialize<'de> for ReadFrom {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag: &str = Deserialize::deserialize(deserializer)?;
        match tag {
            "begin" => Ok(ReadFrom::Begin),
            "end" => Ok(ReadFrom::End),
            other => Err(serde::de::Error::custom(format!(
                "not a read from: {}",
                other
            ))),
        }
    }
}

#[derive(Fail, Debug)]
pub enum CreateSubscriptionError {
    #[fail(display = "Unauthorized: {}", _0)]
    Unauthorized(String),
    /// Already exists
    #[fail(display = "Unprocessable Entity: {}", _0)]
    UnprocessableEntity(String),
    #[fail(display = "Bad request: {}", _0)]
    BadRequest(String),
    #[fail(display = "An error occurred: {}", _0)]
    Other(String),
}

#[derive(Fail, Debug)]
pub enum DeleteSubscriptionError {
    #[fail(display = "Unauthorized: {}", _0)]
    Unauthorized(String),
    #[fail(display = "Forbidden: {}", _0)]
    Forbidden(String),
    #[fail(display = "NotFound: {}", _0)]
    NotFound(String),
    #[fail(display = "An error occurred: {}", _0)]
    Other(String),
}

/// The result of a successful request to create a subscription.
///
/// Creating a subscription is also considered successful if
/// the subscription already existed at the time of the request.
#[derive(Debug, Clone)]
pub enum CreateSubscriptionStatus {
    /// A subscription already existed and the `Subscription`
    /// is contained
    AlreadyExists(Subscription),
    /// The `Subscription` did not exist and was newly created.
    Created(Subscription),
}

impl CreateSubscriptionStatus {
    pub fn subscription(&self) -> &Subscription {
        match *self {
            CreateSubscriptionStatus::AlreadyExists(ref subscription) => subscription,
            CreateSubscriptionStatus::Created(ref subscription) => subscription,
        }
    }
}

#[derive(Fail, Debug)]
pub enum CreateEventTypeError {
    #[fail(display = "Unauthorized: {}", _0)]
    Unauthorized(String),
    /// Already exists
    #[fail(display = "Event type already exists: {}", _0)]
    Conflict(String),
    #[fail(display = "Unprocessable Entity: {}", _0)]
    UnprocessableEntity(String),
    #[fail(display = "An error occurred: {}", _0)]
    Other(String),
}

impl CreateEventTypeError {
    pub fn is_retry_suggested(&self) -> bool {
        match *self {
            CreateEventTypeError::Unauthorized(_) => true,
            CreateEventTypeError::Conflict(_) => false,
            CreateEventTypeError::UnprocessableEntity(_) => false,
            CreateEventTypeError::Other(_) => true,
        }
    }
}

#[derive(Fail, Debug)]
pub enum DeleteEventTypeError {
    #[fail(display = "Unauthorized: {}", _0)]
    Unauthorized(String),
    #[fail(display = "Forbidden: {}", _0)]
    Forbidden(String),
    #[fail(display = "An error occurred: {}", _0)]
    Other(String),
}

impl DeleteEventTypeError {
    pub fn is_retry_suggested(&self) -> bool {
        match *self {
            DeleteEventTypeError::Unauthorized(_) => true,
            DeleteEventTypeError::Forbidden(_) => false,
            DeleteEventTypeError::Other(_) => true,
        }
    }
}

/// The category of an event type.
///
/// For more information see [Event Type](http://nakadi.io/manual.html#definition_EventType)
#[derive(Debug, Clone, Copy)]
pub enum EventCategory {
    Undefined,
    Data,
    Business,
}

impl Serialize for EventCategory {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            EventCategory::Undefined => serializer.serialize_str("undefined"),
            EventCategory::Data => serializer.serialize_str("data"),
            EventCategory::Business => serializer.serialize_str("business"),
        }
    }
}

impl<'de> Deserialize<'de> for EventCategory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag: &str = Deserialize::deserialize(deserializer)?;
        match tag {
            "undefined" => Ok(EventCategory::Undefined),
            "data" => Ok(EventCategory::Data),
            "business" => Ok(EventCategory::Business),
            other => Err(serde::de::Error::custom(format!(
                "not an event category: {}",
                other
            ))),
        }
    }
}

/// The enrichment strategy of an event type.
///
/// For more information see [Event Type](http://nakadi.io/manual.html#definition_EventType)
#[derive(Debug, Clone, Copy)]
pub enum EnrichmentStrategy {
    MetadataEnrichment,
}

impl Serialize for EnrichmentStrategy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            EnrichmentStrategy::MetadataEnrichment => {
                serializer.serialize_str("metadata_enrichment")
            }
        }
    }
}

impl<'de> Deserialize<'de> for EnrichmentStrategy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag: &str = Deserialize::deserialize(deserializer)?;
        match tag {
            "metadata_enrichment" => Ok(EnrichmentStrategy::MetadataEnrichment),
            other => Err(serde::de::Error::custom(format!(
                "not an enrichment strategy: {}",
                other
            ))),
        }
    }
}

/// The partition strategy of an event type.
///
/// For more information see [Event Type](http://nakadi.io/manual.html#definition_EventType)
#[derive(Debug, Clone, Copy)]
pub enum PartitionStrategy {
    Random,
    Hash,
    UserDefined,
}

impl Serialize for PartitionStrategy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            PartitionStrategy::Random => serializer.serialize_str("random"),
            PartitionStrategy::Hash => serializer.serialize_str("hash"),
            PartitionStrategy::UserDefined => serializer.serialize_str("user_defined"),
        }
    }
}

impl<'de> Deserialize<'de> for PartitionStrategy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag: &str = Deserialize::deserialize(deserializer)?;
        match tag {
            "random" => Ok(PartitionStrategy::Random),
            "hash" => Ok(PartitionStrategy::Hash),
            "user_defined" => Ok(PartitionStrategy::UserDefined),
            other => Err(serde::de::Error::custom(format!(
                "not a partition strategy: {}",
                other
            ))),
        }
    }
}

/// The compatibility mode of an event type.
///
/// For more information see [Event Type](http://nakadi.io/manual.html#definition_EventType)
#[derive(Debug, Clone, Copy)]
pub enum CompatibilityMode {
    Compatible,
    Forward,
    None,
}

impl Serialize for CompatibilityMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            CompatibilityMode::Compatible => serializer.serialize_str("compatible"),
            CompatibilityMode::Forward => serializer.serialize_str("forward"),
            CompatibilityMode::None => serializer.serialize_str("none"),
        }
    }
}

impl<'de> Deserialize<'de> for CompatibilityMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag: &str = Deserialize::deserialize(deserializer)?;
        match tag {
            "compatible" => Ok(CompatibilityMode::Compatible),
            "forward" => Ok(CompatibilityMode::Forward),
            "none" => Ok(CompatibilityMode::None),
            other => Err(serde::de::Error::custom(format!(
                "not a compatibility mode: {}",
                other
            ))),
        }
    }
}

/// The definition of an event type.
///
/// These are the parameters used to create a new event type.
///
/// For more information see [Event Type](http://nakadi.io/manual.html#definition_EventType)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeDefinition {
    pub name: String,
    pub owning_application: String,
    pub category: EventCategory,
    pub enrichment_strategies: Vec<EnrichmentStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_strategy: Option<PartitionStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_mode: Option<CompatibilityMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key_fields: Option<Vec<String>>,
    pub schema: EventTypeSchema,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_statistic: Option<EventTypeStatistics>,
}

/// The schema definition of an event type.
///
/// These are part of the parameters used to create a new event type.
///
/// For more information see
/// [Event Type Schema](http://nakadi.io/manual.html#definition_EventTypeSchema)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeSchema {
    pub version: Option<String>,
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub schema: String,
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaType {
    JsonSchema,
}

impl Serialize for SchemaType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            SchemaType::JsonSchema => serializer.serialize_str("json_schema"),
        }
    }
}

impl<'de> Deserialize<'de> for SchemaType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag: &str = Deserialize::deserialize(deserializer)?;
        match tag {
            "json_schema" => Ok(SchemaType::JsonSchema),
            other => Err(serde::de::Error::custom(format!(
                "not a schema type: {}",
                other
            ))),
        }
    }
}

/// Known statistics on an event type passed to Nakadi when creating
/// an event.
///
/// These are part of the parameters used to create a new event type.
///
/// For more information see
/// [Event Type Statistics](http://nakadi.io/manual.html#definition_EventTypeStatistics)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeStatistics {
    pub messages_per_minute: usize,
    pub message_size: usize,
    pub read_parallelism: u16,
    pub write_parallelism: u16,
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
        #[serde(rename = "items")]
        pub event_types: Vec<EventTypeInfo>,
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
