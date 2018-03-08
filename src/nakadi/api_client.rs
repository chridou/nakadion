use std::sync::Arc;
use std::env;
use std::time::Duration;
use std::io::Read;

use auth::{AccessToken, ProvidesAccessToken, TokenError};
use nakadi::model::{FlowId, StreamId, SubscriptionId};

use serde::{self, Deserialize, Deserializer, Serialize, Serializer};
use serde_json;

use reqwest::{Client as HttpClient, ClientBuilder as HttpClientBuilder, Response};
use reqwest::StatusCode;
use reqwest::header::{Authorization, Bearer, ContentType, Headers};
use backoff::{Error as BackoffError, ExponentialBackoff, Operation};
use failure::*;

header! { (XNakadiStreamId, "X-Nakadi-StreamId") => [String] }
header! { (XFlowId, "X-Flow-Id") => [String] }

/// A client to the Nakadi Event Broker
pub trait ApiClient {
    fn commit_cursors<T: AsRef<[u8]>>(
        &self,
        subscription_id: &SubscriptionId,
        stream_id: &StreamId,
        cursors: &[T],
        flow_id: FlowId,
    ) -> ::std::result::Result<CommitStatus, CommitError>;

    fn delete_event_type(&self, event_type_name: &str) -> Result<(), DeleteEventTypeError>;

    fn create_event_type(
        &self,
        event_type: &EventTypeDefinition,
    ) -> Result<(), CreateEventTypeError>;

    fn create_subscription(
        &self,
        request: &CreateSubscriptionRequest,
    ) -> Result<CreateSubscriptionStatus, CreateSubscriptionError>;

    fn delete_subscription(&self, id: &SubscriptionId) -> Result<(), DeleteSubscriptionError>;
}

/// Settings for establishing a connection to `Nakadi`.
#[derive(Debug)]
pub struct Config {
    pub nakadi_host: String,
    pub request_timeout: Duration,
}

pub struct ConfigBuilder {
    pub nakadi_host: Option<String>,
    pub request_timeout: Option<Duration>,
}

impl Default for ConfigBuilder {
    fn default() -> ConfigBuilder {
        ConfigBuilder {
            nakadi_host: None,
            request_timeout: None,
        }
    }
}

impl ConfigBuilder {
    /// The URI prefix for the Nakadi Host, e.g. "https://my.nakadi.com"
    pub fn nakadi_host<T: Into<String>>(mut self, nakadi_host: T) -> ConfigBuilder {
        self.nakadi_host = Some(nakadi_host.into());
        self
    }
    pub fn request_timeout(mut self, request_timeout: Duration) -> ConfigBuilder {
        self.request_timeout = Some(request_timeout);
        self
    }

    /// Create a builder from environment variables.
    ///
    /// For variables not found except 'NAKADION_NAKADI_HOST' a default will be set.
    ///
    /// Variables:
    ///
    /// * NAKADION_NAKADI_HOST: See `ConnectorSettings::nakadi_host`
    /// * NAKADION_REQUEST_TIMEOUT_MS:
    pub fn from_env() -> Result<ConfigBuilder, Error> {
        let builder = ConfigBuilder::default();
        let builder = if let Some(env_val) = env::var("NAKADION_NAKADI_HOST").ok() {
            builder.nakadi_host(env_val)
        } else {
            warn!(
                "Environment variable 'NAKADION_NAKADI_HOST' not found. It will have to be set \
                 manually."
            );
            builder
        };
        let builder = if let Some(env_val) = env::var("NAKADION_REQUEST_TIMEOUT_MS").ok() {
            builder.request_timeout(Duration::from_millis(env_val
                .parse::<u64>()
                .context("Could not parse 'NAKADION_REQUEST_TIMEOUT_MS'")?))
        } else {
            warn!(
                "Environment variable 'NAKADION_REQUEST_TIMEOUT_MS' not found. It will have be set \
                 to the default."
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
            nakadi_host: nakadi_host,
            request_timeout: self.request_timeout.unwrap_or(Duration::from_millis(300)),
        })
    }

    pub fn build_client<T>(self, token_provider: T) -> Result<NakadiApiClient, Error>
    where
        T: ProvidesAccessToken + Send + Sync + 'static,
    {
        self.build_client_with_shared_access_token_provider(Arc::new(token_provider))
    }

    pub fn build_client_with_shared_access_token_provider(
        self,
        token_provider: Arc<ProvidesAccessToken + Send + Sync + 'static>,
    ) -> Result<NakadiApiClient, Error> {
        let config = self.build().context("Could not build client config")?;

        NakadiApiClient::with_shared_access_token_provider(config, token_provider)
    }
}

#[derive(Clone)]
pub struct NakadiApiClient {
    nakadi_host: String,
    http_client: HttpClient,
    token_provider: Arc<ProvidesAccessToken + Send + Sync + 'static>,
}

impl NakadiApiClient {
    pub fn new<T: ProvidesAccessToken + Send + Sync + 'static>(
        config: Config,
        token_provider: T,
    ) -> Result<NakadiApiClient, Error> {
        NakadiApiClient::with_shared_access_token_provider(config, Arc::new(token_provider))
    }

    pub fn with_shared_access_token_provider(
        config: Config,
        token_provider: Arc<ProvidesAccessToken + Send + Sync + 'static>,
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

    pub fn attempt_commit<T: AsRef<[u8]>>(
        &self,
        url: &str,
        stream_id: StreamId,
        cursors: &[T],
        flow_id: FlowId,
    ) -> ::std::result::Result<CommitStatus, CommitError> {
        let mut headers = Headers::new();
        if let Some(AccessToken(token)) = self.token_provider.get_token()? {
            headers.set(Authorization(Bearer { token }));
        }

        headers.set(XFlowId(flow_id.0.clone()));
        headers.set(XNakadiStreamId(stream_id.0));
        headers.set(ContentType::json());

        let body = make_cursors_body(cursors);

        let mut response = self.http_client
            .post(url)
            .headers(headers)
            .body(body)
            .send()?;

        match response.status() {
            // All cursors committed but at least one did not increase an offset.
            StatusCode::Ok => Ok(CommitStatus::NotAllOffsetsIncreased),
            // All cursors committed and all increased the offset.
            StatusCode::NoContent => Ok(CommitStatus::AllOffsetsIncreased),
            StatusCode::NotFound => Err(CommitError::SubscriptionNotFound(
                format!(
                    "{}: {}",
                    StatusCode::NotFound,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            StatusCode::UnprocessableEntity => Err(CommitError::UnprocessableEntity(
                format!(
                    "{}: {}",
                    StatusCode::UnprocessableEntity,
                    read_response_body(&mut response)
                ),
                flow_id,
            )),
            StatusCode::Forbidden => Err(CommitError::Client(
                format!(
                    "{}: {}",
                    StatusCode::Forbidden,
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

    fn commit_cursors<T: AsRef<[u8]>>(
        &self,
        subscription_id: &SubscriptionId,
        stream_id: &StreamId,
        cursors: &[T],
        flow_id: FlowId,
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
            Ok(x) => Ok(x),
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
            Ok(x) => Ok(x),
            Err(BackoffError::Transient(err)) => Err(err),
            Err(BackoffError::Permanent(err)) => Err(err),
        }
    }

    fn create_subscription(
        &self,
        request: &CreateSubscriptionRequest,
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

#[derive(Debug)]
pub enum CommitStatus {
    AllOffsetsIncreased,
    NotAllOffsetsIncreased,
    NothingToCommit,
}

#[derive(Fail, Debug)]
pub enum CommitError {
    #[fail(display = "Token Error on commit: {}", _0)] TokenError(String),
    #[fail(display = "Connection Error: {}", _0)] Connection(String),
    #[fail(display = "Subscription not found(FlowId: {}): {}", _1, _0)]
    SubscriptionNotFound(String, FlowId),
    #[fail(display = "Unprocessable Entity(FlowId: {}): {}", _1, _0)]
    UnprocessableEntity(String, FlowId),
    #[fail(display = "Server Error(FlowId: {}): {}", _1, _0)] Server(String, FlowId),
    #[fail(display = "Client Error(FlowId: {}): {}", _1, _0)] Client(String, FlowId),
    #[fail(display = "Other Error(FlowId: {}): {}", _1, _0)] Other(String, FlowId),
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
    token_provider: &ProvidesAccessToken,
    event_type: &EventTypeDefinition,
) -> Result<(), CreateEventTypeError> {
    let mut request_builder = client.post(url);

    match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => {
            request_builder.header(Authorization(Bearer { token }));
        }
        Ok(None) => (),
        Err(err) => return Err(CreateEventTypeError::Other(err.to_string())),
    };

    match request_builder.json(event_type).send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::Created => Ok(()),
            StatusCode::Unauthorized => {
                let msg = read_response_body(response);
                Err(CreateEventTypeError::Unauthorized(msg))
            }
            StatusCode::Conflict => {
                let msg = read_response_body(response);
                Err(CreateEventTypeError::Conflict(msg))
            }
            StatusCode::UnprocessableEntity => {
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
    token_provider: &ProvidesAccessToken,
) -> Result<(), DeleteEventTypeError> {
    let mut request_builder = client.delete(url);

    match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => {
            request_builder.header(Authorization(Bearer { token }));
        }
        Ok(None) => (),
        Err(err) => return Err(DeleteEventTypeError::Other(err.to_string())),
    };

    match request_builder.send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::Ok => Ok(()),
            StatusCode::Unauthorized => {
                let msg = read_response_body(response);
                Err(DeleteEventTypeError::Unauthorized(msg))
            }
            StatusCode::Forbidden => {
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
    token_provider: &ProvidesAccessToken,
) -> Result<(), DeleteSubscriptionError> {
    let mut request_builder = client.delete(url);

    match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => {
            request_builder.header(Authorization(Bearer { token }));
        }
        Ok(None) => (),
        Err(err) => return Err(DeleteSubscriptionError::Other(err.to_string())),
    };

    match request_builder.send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::NoContent => Ok(()),
            StatusCode::NotFound => {
                let msg = read_response_body(response);
                Err(DeleteSubscriptionError::NotFound(msg))
            }
            StatusCode::Unauthorized => {
                let msg = read_response_body(response);
                Err(DeleteSubscriptionError::Unauthorized(msg))
            }
            StatusCode::Forbidden => {
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
        .unwrap_or("<Could not read body.>".to_string())
}

fn create_subscription(
    client: &HttpClient,
    url: &str,
    token_provider: &ProvidesAccessToken,
    request: &CreateSubscriptionRequest,
) -> Result<CreateSubscriptionStatus, CreateSubscriptionError> {
    let mut request_builder = client.post(url);

    match token_provider.get_token() {
        Ok(Some(AccessToken(token))) => {
            request_builder.header(Authorization(Bearer { token }));
        }
        Ok(None) => (),
        Err(err) => return Err(CreateSubscriptionError::Other(err.to_string())),
    };

    match request_builder.json(request).send() {
        Ok(ref mut response) => match response.status() {
            StatusCode::Ok => match serde_json::from_reader(response) {
                Ok(sub) => Ok(CreateSubscriptionStatus::AlreadyExists(sub)),
                Err(err) => Err(CreateSubscriptionError::Other(err.to_string())),
            },
            StatusCode::Created => match serde_json::from_reader(response) {
                Ok(sub) => Ok(CreateSubscriptionStatus::Created(sub)),
                Err(err) => Err(CreateSubscriptionError::Other(err.to_string())),
            },
            StatusCode::Unauthorized => {
                let msg = read_response_body(response);
                Err(CreateSubscriptionError::Unauthorized(msg))
            }
            StatusCode::UnprocessableEntity => {
                let msg = read_response_body(response);
                Err(CreateSubscriptionError::UnprocessableEntity(msg))
            }
            StatusCode::BadRequest => {
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

#[derive(Debug, Clone, Serialize)]
pub struct CreateSubscriptionRequest {
    pub owning_application: String,
    pub event_types: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Subscription {
    pub id: SubscriptionId,
    pub owning_application: String,
    pub event_types: Vec<String>,
}

#[derive(Fail, Debug)]
pub enum CreateSubscriptionError {
    #[fail(display = "Unauthorized: {}", _0)] Unauthorized(String),
    /// Already exists
    #[fail(display = "Unprocessable Entity: {}", _0)]
    UnprocessableEntity(String),
    #[fail(display = "Bad request: {}", _0)] BadRequest(String),
    #[fail(display = "An error occured: {}", _0)] Other(String),
}

#[derive(Fail, Debug)]
pub enum DeleteSubscriptionError {
    #[fail(display = "Unauthorized: {}", _0)] Unauthorized(String),
    #[fail(display = "Forbidden: {}", _0)] Forbidden(String),
    #[fail(display = "NotFound: {}", _0)] NotFound(String),
    #[fail(display = "An error occured: {}", _0)] Other(String),
}

#[derive(Debug, Clone)]
pub enum CreateSubscriptionStatus {
    AlreadyExists(Subscription),
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
    #[fail(display = "Unauthorized: {}", _0)] Unauthorized(String),
    /// Already exists
    #[fail(display = "Event type already exists: {}", _0)]
    Conflict(String),
    #[fail(display = "Unprocessable Entity: {}", _0)] UnprocessableEntity(String),
    #[fail(display = "An error occured: {}", _0)] Other(String),
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
    #[fail(display = "Unauthorized: {}", _0)] Unauthorized(String),
    #[fail(display = "Forbidden: {}", _0)] Forbidden(String),
    #[fail(display = "An error occured: {}", _0)] Other(String),
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
                "not a partition stragtegy: {}",
                other
            ))),
        }
    }
}

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
    #[serde(skip_serializing_if = "Option::is_none")] pub partition_key_fields: Option<Vec<String>>,
    pub schema: EventTypeSchema,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_statistic: Option<EventTypeStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeSchema {
    pub version: Option<String>,
    #[serde(rename = "type")] pub schema_type: SchemaType,
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
