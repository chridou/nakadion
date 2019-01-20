use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct PartitionId(String);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct StreamId(Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowId(String);

pub mod subscription {
    use uuid::Uuid;

    pub struct SubscriptionId(Uuid);

    impl SubscriptionId {
        pub fn new(id: Uuid) -> Self {
            SubscriptionId(id)
        }
    }

    pub struct Subcription;

    pub struct SubscriptionInput;
}

pub mod event_type {
    use chrono::{DateTime, Utc};

    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct EventTypeName(String);

    impl EventTypeName {
        pub fn new(v: impl Into<String>) -> Self {
            EventTypeName(v.into())
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct OwningApplication(String);

    impl OwningApplication {
        pub fn new(v: impl Into<String>) -> Self {
            OwningApplication(v.into())
        }
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub enum Category {
        Undefined,
        Data,
        Business,
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub enum PartitionStrategy {
        Random,
        Hash,
        UserDefined,
    }

    impl Default for PartitionStrategy {
        fn default() -> Self {
            PartitionStrategy::Random
        }
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub enum CompatibilityMode {
        Compatible,
        Forward,
        None,
    }

    impl Default for CompatibilityMode {
        fn default() -> Self {
            CompatibilityMode::Forward
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PartitionKey(String);

    impl PartitionKey {
        pub fn new(v: impl Into<String>) -> Self {
            PartitionKey(v.into())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PartitionKeyFields(Vec<PartitionKey>);

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub enum CleanupPolicy {
        Delete,
        Compact,
    }

    impl Default for CleanupPolicy {
        fn default() -> Self {
            CleanupPolicy::Delete
        }
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub struct RetentionTime(u64);

    impl RetentionTime {
        pub fn new(v: u64) -> Self {
            RetentionTime(v)
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct EventTypeSchema {
        version: String,
        created_at: DateTime<Utc>,
        #[serde(rename = "type")]
        schema_type: String,
        schema: String,
    }

    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct EventTypeSchemaInput {
        #[serde(rename = "type")]
        schema_type: String,
        schema: String,
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    pub struct EventTypeOptions {
        retention_time: RetentionTime,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EventTypeAuthorization {
        admins: Vec<AuthorizationAttribute>,
        readers: Vec<AuthorizationAttribute>,
        writers: Vec<AuthorizationAttribute>,
    }

    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct AuthorizationAttribute {
        data_type: String,
        value: String,
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub enum EventTypeAudience {
        ComponentInternal,
        BusinessUnitInternal,
        CompanyInternal,
        ExternalPartner,
        ExternalPublic,
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub enum EnrichmentStrategy {
        MetadataEnrichment,
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
    pub struct EventTypeStatistics {
        messages_per_minute: u64,
        message_size: u64,
        read_parallelism: u64,
        write_parallelism: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EventType {
        name: EventTypeName,
        owning_application: OwningApplication,
        category: Category,
        enrichment_strategy: Option<EnrichmentStrategy>,
        partition_strategy: PartitionStrategy,
        compatibility_mode: CompatibilityMode,
        schema: EventTypeSchema,
        partition_key_fields: Option<PartitionKeyFields>,
        cleanup_policy: CleanupPolicy,
        default_statistic: Option<EventTypeStatistics>,
        options: Option<EventTypeOptions>,
        authorization: Option<EventTypeAuthorization>,
        audience: Option<EventTypeAudience>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EventTypeInput {
        name: EventTypeName,
        owning_application: OwningApplication,
        category: Category,
        enrichment_strategy: Option<EnrichmentStrategy>,
        partition_strategy: PartitionStrategy,
        compatibility_mode: CompatibilityMode,
        schema: EventTypeSchemaInput,
        partition_key_fields: Option<PartitionKeyFields>,
        cleanup_policy: CleanupPolicy,
        default_statistic: Option<EventTypeStatistics>,
        options: Option<EventTypeOptions>,
        authorization: Option<EventTypeAuthorization>,
        audience: Option<EventTypeAudience>,
    }

}
