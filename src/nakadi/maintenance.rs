use std::sync::Arc;

use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

use reqwest::{Client as HttpClient, Response};
use reqwest::StatusCode;
use reqwest::header::{Authorization, Bearer, ContentType, Headers};

use auth::ProvidesAccessToken;

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

    //pub fn create_event_type(&self, schema: )
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
    name: String,
    owning_application: String,
    category: EventCategory,
    enrichment_strategies: Vec<EnrichmentStrategy>,
    partition_strategy: Option<PartitionStrategy>,
    compatibility_mode: Option<CompatibilityMode>,
    partition_key_fields: Option<Vec<String>>,
    #[serde(rename = "type")] schema: EventTypeSchema,
    default_statistic: Option<EventTypeStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeSchema {
    version: Option<String>,
    schema_type: SchemaType,
    schema: String,
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
    messages_per_minute: usize,
    message_size: usize,
    read_parallelism: u16,
    write_parallelism: u16,
}
