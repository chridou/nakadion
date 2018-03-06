use nakadi::model::SubscriptionId;
use std::sync::Arc;
use std::time::Duration;
use std::io::Read;

use serde::{self, Deserialize, Deserializer, Serialize, Serializer};
use serde_json;

use reqwest::{Client as HttpClient, Response};
use reqwest::StatusCode;
use reqwest::header::{Authorization, Bearer, Headers};
use backoff::{Error as BackoffError, ExponentialBackoff, Operation};
use failure::*;

use auth::{AccessToken, ProvidesAccessToken};

pub struct MaintenanceClient {
    nakadi_base_url: String,
    http_client: HttpClient,
    token_provider: Arc<ProvidesAccessToken>,
}

impl MaintenanceClient {
    pub fn new<U: Into<String>, T: ProvidesAccessToken + 'static>(
        nakadi_base_url: U,
        token_provider: T,
    ) -> MaintenanceClient {
        MaintenanceClient {
            nakadi_base_url: nakadi_base_url.into(),
            http_client: HttpClient::new(),
            token_provider: Arc::new(token_provider),
        }
    }

    pub fn with_shared_access_token_provider<U: Into<String>>(
        nakadi_base_url: U,
        token_provider: Arc<ProvidesAccessToken>,
    ) -> MaintenanceClient {
        MaintenanceClient {
            nakadi_base_url: nakadi_base_url.into(),
            http_client: HttpClient::new(),
            token_provider: token_provider,
        }
    }

    pub fn delete_event_type(&self, event_type_name: &str) -> Result<(), DeleteEventTypeError> {
        let url = format!("{}/event-types/{}", self.nakadi_base_url, event_type_name);

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

    pub fn create_event_type(
        &self,
        event_type: &EventTypeDefinition,
    ) -> Result<(), CreateEventTypeError> {
        let url = format!("{}/event-types", self.nakadi_base_url);

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

    pub fn create_subscription(
        &self,
        request: &CreateSubscriptionRequest,
    ) -> Result<CreateSubscriptionStatus, CreateSubscriptionError> {
        let url = format!("{}/subscriptions", self.nakadi_base_url);
        create_subscription(&self.http_client, &url, &*self.token_provider, request)
    }

    pub fn delete_subscription(&self, id: &SubscriptionId) -> Result<(), DeleteSubscriptionError> {
        let url = format!("{}/subscriptions/{}", self.nakadi_base_url, id.0);
        delete_subscription(&self.http_client, &url, &*self.token_provider)
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
    pub partition_strategy: Option<PartitionStrategy>,
    pub compatibility_mode: Option<CompatibilityMode>,
    pub partition_key_fields: Option<Vec<String>>,
    pub schema: EventTypeSchema,
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
