//! Types for defining and monitoring event types
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::misc::{AuthorizationAttribute, AuthorizationAttributes, OwningApplication};

mod event_type_input;
pub use event_type_input::*;

new_type! {
    #[doc=r#"Name of an EventType. The name is constrained by a regular expression.

Note: the name can encode the owner/responsible for this EventType and ideally should
follow a common pattern that makes it easy to read and understand, but this level of
structure is not enforced. For example a team name and data type can be used such as
‘acme-team.price-change’.

See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*name)"#]
    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
    pub struct EventTypeName(String, env="EVENT_TYPE_NAME");
}

/// Defines the category of this EventType.
///
/// The value set will influence, if not set otherwise, the default set of
/// validations, enrichment-strategies, and the effective schema for validation.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*category)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    /// No predefined changes apply. The effective schema for the validation is
    /// exactly the same as the EventTypeSchema.
    Undefined,
    /// Events of this category will be DataChangeEvents. The effective schema during
    /// the validation contains metadata, and adds fields data_op and data_type. The
    /// passed EventTypeSchema defines the schema of data.
    Data,
    /// Events of this category will be BusinessEvents. The effective schema for
    /// validation contains metadata and any additionally defined properties passed in the
    /// EventTypeSchema directly on top level of the Event. If name conflicts arise, creation
    /// of this EventType will be rejected.
    Business,
}

/// Determines how the assignment of the event to a partition should be handled.
///
/// The default is `random`.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/registry/partition-strategies_get)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// Resolution of the target partition happens randomly (events are evenly
    /// distributed on the topic’s partitions).
    Random,
    /// Resolution of the partition follows the computation of a hash from the value of
    /// the fields indicated in the EventType’s partition_key_fields, guaranteeing that Events
    /// with same values on those fields end in the same partition. Given the event type’s category
    /// is DataChangeEvent, field path is considered relative to “data”.
    Hash,
    /// Target partition is defined by the client. As long as the indicated
    /// partition exists, Event assignment will respect this value. Correctness of the relative
    /// ordering of events is under the responsibility of the Producer. Requires that the client
    /// provides the target partition on metadata.partition (See EventMetadata). Failure to do
    /// so will reject the publishing of the Event.
    UserDefined,
}

impl Default for PartitionStrategy {
    fn default() -> Self {
        PartitionStrategy::Random
    }
}

/// Compatibility mode provides a mean for event owners to evolve their schema, given changes respect the
/// semantics defined by this field.
///
/// It’s designed to be flexible enough so that producers can evolve their schemas while not
/// inadvertently breaking existent consumers.
///
/// Once defined, the compatibility mode is fixed, since otherwise it would break a predefined contract,
/// declared by the producer.
///
/// The default is `forward`.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*compatibility_mode)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityMode {
    /// Consumers can reliably parse events produced under different versions. Every event published
    /// since the first version is still valid based on the newest schema. When in compatible mode, it’s allowed to
    /// add new optional properties and definitions to an existing schema, but no other changes are allowed.
    /// Under this mode, the following json-schema attributes are not supported: `not`, `patternProperties`,
    /// `additionalProperties` and `additionalItems`. When validating events, additional properties is `false`.
    Compatible,
    /// Compatible schema changes are allowed. It’s possible to use the full json schema specification
    /// for defining schemas. Consumers of forward compatible event types can safely read events tagged with the
    /// latest schema version as long as they follow the robustness principle.
    Forward,
    /// Any schema modification is accepted, even if it might break existing producers or consumers. When
    /// validating events, no additional properties are accepted unless explicitly stated in the schema.
    None,
}

impl Default for CompatibilityMode {
    fn default() -> Self {
        CompatibilityMode::Forward
    }
}

new_type! {
#[doc="Part of `PartitionKeyFields`\n"]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PartitionKeyField(String, env="EVENT_TYPE_PARTITION_KEY_FIELD");
}

/// Required when 'partition_resolution_strategy' is set to ‘hash’. Must be absent otherwise.
/// Indicates the fields used for evaluation the partition of Events of this type.
///
/// If this is set it MUST be a valid required field as defined in the schema.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*partition_key_fields)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PartitionKeyFields(Vec<PartitionKeyField>);

impl PartitionKeyFields {
    pub fn new<I>(items: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<PartitionKeyField>,
    {
        let items = items.into_iter().map(|it| it.into()).collect();
        Self(items)
    }

    pub fn partition_key<T: Into<PartitionKeyField>>(mut self, v: T) -> Self {
        self.push(v);
        self
    }

    pub fn push<T: Into<PartitionKeyField>>(&mut self, v: T) {
        self.0.push(v.into());
    }

    pub fn into_inner(self) -> Vec<PartitionKeyField> {
        self.0
    }

    pub fn iter(&self) -> impl Iterator<Item = &PartitionKeyField> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut PartitionKeyField> {
        self.0.iter_mut()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl<A> From<A> for PartitionKeyFields
where
    A: Into<PartitionKeyField>,
{
    fn from(k: A) -> Self {
        Self(vec![k.into()])
    }
}

impl<A, B> From<(A, B)> for PartitionKeyFields
where
    A: Into<PartitionKeyField>,
    B: Into<PartitionKeyField>,
{
    fn from((a, b): (A, B)) -> Self {
        Self(vec![a.into(), b.into()])
    }
}

impl<A, B, C> From<(A, B, C)> for PartitionKeyFields
where
    A: Into<PartitionKeyField>,
    B: Into<PartitionKeyField>,
    C: Into<PartitionKeyField>,
{
    fn from((a, b, c): (A, B, C)) -> Self {
        Self(vec![a.into(), b.into(), c.into()])
    }
}

impl AsRef<[PartitionKeyField]> for PartitionKeyFields {
    fn as_ref(&self) -> &[PartitionKeyField] {
        &self.0
    }
}

/// Event type cleanup policy. There are two possible values.
///
/// It’s not possible to change the value of this field for existing event type.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*cleanup_policy)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupPolicy {
    /// This cleanup policy will delete old events after retention time expires. Nakadi guarantees that each
    /// event will be available for at least the retention time period. However Nakadi doesn’t guarantee that event
    /// will be deleted right after retention time expires.
    Delete,
    /// This cleanup policy will keep only the latest event for each event key. The compaction is performed per
    /// partition, there is no compaction across partitions. The key that will be used as a compaction key should be
    /// specified in ‘partition_compaction_key’ field of event metadata. This cleanup policy is not available for
    /// ‘undefined’ category of event types.
    ///
    /// The compaction can be not applied to events that were published recently and located at the head of the
    /// queue, which means that the actual amount of events received by consumers can be different depending on time
    /// when the consumption happened.
    ///
    /// When using ‘compact’ cleanup policy user should consider that different Nakadi endpoints showing the amount
    /// of events will actually show the original amount of events published, not the actual amount of events that
    /// are currently there.
    /// E.g. subscription /stats endpoint will show the value ‘unconsumed_events’ - but that may not match with the
    /// actual amount of events unconsumed in that subscription as ‘compact’ cleanup policy may delete older events
    /// in the middle of queue if there is a newer event for the same key published.
    ///
    /// For more details about compaction implementation please read the documentation of Log Compaction in
    /// [Kafka](https://kafka.apache.org/documentation/#compaction), Nakadi currently relies on this implementation.
    Compact,
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        CleanupPolicy::Delete
    }
}

/// The type of schema definition. Currently only json_schema (JSON Schema v04) is supported, but in the
/// future there could be others.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum SchemaType {
    #[serde(rename = "json_schema")]
    JsonSchema,
}

/// The most recent schema for this EventType. Submitted events will be validated against it.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventTypeSchema {
    /// This field is automatically generated by Nakadi. Values are based on semantic versioning. Changes to title
    /// or description are considered PATCH level changes. Adding new optional fields is considered a MINOR level
    /// change. All other changes are considered MAJOR level.
    pub version: String,
    /// Creation timestamp of the schema. This is generated by Nakadi. It should not be
    /// specified when updating a schema and sending it may result in a client error.
    pub created_at: DateTime<Utc>,
    ///The type of schema definition. Currently only json_schema (JSON Schema v04) is supported, but in the
    ///future there could be others.
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    /// The schema as string in the syntax defined in the field type. Failure to respect the
    /// syntax will fail any operation on an EventType.
    pub schema: SchemaSyntax,
}

new_type! {
#[doc=r#"
The schema as string in the syntax defined in the field type.

Failure to respect the
syntax will fail any operation on an EventType.

"#]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub struct SchemaSyntax(String, env="EVENT_TYPE_SCHEMA_SYNTAX");
}

new_type! {
    #[doc="Number of milliseconds that Nakadi stores events published to this event type.\n\n\
    See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventTypeOptions*retention_time)"]
    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
    pub copy struct RetentionTime(u64);
}

impl RetentionTime {
    pub fn to_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}

/// Additional parameters for tuning internal behavior of Nakadi.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventTypeOptions)
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct EventTypeOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_time: Option<RetentionTime>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EventTypeAuthorization {
    #[serde(default)]
    pub admins: AuthorizationAttributes,
    #[serde(default)]
    pub readers: AuthorizationAttributes,
    #[serde(default)]
    pub writers: AuthorizationAttributes,
}

impl EventTypeAuthorization {
    pub fn new<A, R, W>(admins: A, readers: R, writers: W) -> Self
    where
        A: Into<AuthorizationAttributes>,
        R: Into<AuthorizationAttributes>,
        W: Into<AuthorizationAttributes>,
    {
        Self {
            admins: admins.into(),
            readers: readers.into(),
            writers: writers.into(),
        }
    }

    pub fn admin<T: Into<AuthorizationAttribute>>(mut self, admin: T) -> Self {
        self.admins.push(admin.into());
        self
    }
    pub fn reader<T: Into<AuthorizationAttribute>>(mut self, reader: T) -> Self {
        self.readers.push(reader.into());
        self
    }
    pub fn writer<T: Into<AuthorizationAttribute>>(mut self, writer: T) -> Self {
        self.writers.push(writer.into());
        self
    }

    pub fn add_admin<T: Into<AuthorizationAttribute>>(&mut self, admin: T) {
        self.admins.push(admin.into())
    }
    pub fn add_reader<T: Into<AuthorizationAttribute>>(&mut self, reader: T) {
        self.readers.push(reader.into())
    }
    pub fn add_writer<T: Into<AuthorizationAttribute>>(&mut self, writer: T) {
        self.writers.push(writer.into())
    }
}

/// Intended target audience of the event type. Relevant for standards around quality of design and documentation,
/// reviews, discoverability, changeability, and permission granting. See the guidelines
/// https://opensource.zalando.com/restful-api-guidelines/#219
///
/// This attribute adds no functionality and is used only to inform users about the usage scope of the event type.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*audience)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventTypeAudience {
    #[serde(rename = "component-internal")]
    ComponentInternal,
    #[serde(rename = "business-unit-internal")]
    BusinessUnitInternal,
    #[serde(rename = "company-internal")]
    CompanyInternal,
    #[serde(rename = "external-partner")]
    ExternalPartner,
    #[serde(rename = "external-public")]
    ExternalPublic,
}

/// Determines the enrichment to be performed on an Event upon reception. Enrichment is
/// performed once upon reception (and after validation) of an Event and is only possible on
/// fields that are not defined on the incoming Event.
///
/// For event types in categories `business` or `data` it’s mandatory to use
/// metadata_enrichment strategy. For `undefined` event types it’s not possible to use this
/// strategy, since metadata field is not required.
///
/// See documentation for the write operation for details on behaviour in case of unsuccessful
/// enrichment.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*enrichment_strategies)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentStrategy {
    MetadataEnrichment,
}

impl Default for EnrichmentStrategy {
    fn default() -> Self {
        EnrichmentStrategy::MetadataEnrichment
    }
}

/// Operational statistics for an EventType. This data may be provided by users on Event Type creation.
/// Nakadi uses this object in order to provide an optimal number of partitions from a throughput perspective.
///
/// This field defines the number of partitions in the underlying Kafka topic of an event type.
/// The amount of partitions is given by the expression max(read_parallelism, write_parallelism).
/// The maximum number of partitions is specific to each deployment of Nakadi
/// and should be referred to in a separated document.
///
/// For historical reasons the way that the number of partitions is defined is not as straighforward as it could.
/// The fields messages_per_minute and message_size could potentially influence the resulting amount of partitions,
/// so it’s recommended to set both of them to 1 (one).
/// Providing values different than 1 could result in a higher number of partitions being created.
///
/// For those interested in why these fields exist, in the beginning of the project the developers
/// run a very rudimentary benchmark to understand how much data could be ingested by a single Kafka topic-partition.
/// This benchmark data was later used by this feature to define the suposedely
/// ideal number of partitions for the user’s needs. Over time the maintainers of
/// the project found this benchmark to be unreliable,
/// usually resulting in fewer partitions than needed.
///  
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventTypeStatistics)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventTypeStatistics {
    /// Write rate for events of this EventType. This rate encompasses all producers of this
    /// EventType for a Nakadi cluster.
    ///
    /// Measured in event count per minute.
    pub messages_per_minute: u64,
    /// Average message size for each Event of this EventType. Includes in the count the whole serialized
    /// form of the event, including metadata.
    /// Measured in bytes.
    pub message_size: u64,
    /// Amount of parallel readers (consumers) to this EventType.
    pub read_parallelism: u64,
    /// Amount of parallel writers (producers) to this EventType.
    pub write_parallelism: u64,
}

impl EventTypeStatistics {
    pub fn new(
        messages_per_minute: u64,
        message_size: u64,
        read_parallelism: u64,
        write_parallelism: u64,
    ) -> Self {
        Self {
            messages_per_minute,
            message_size,
            read_parallelism,
            write_parallelism,
        }
    }
}

/// Definition of an event type
///
/// This struct is only used for querying from Nakadi.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventType {
    /// Name of this EventType. The name is constrained by a regular expression.
    ///
    /// Note: the name can encode the owner/responsible for this EventType and ideally should
    /// follow a common pattern that makes it easy to read and understand, but this level of
    /// structure is not enforced. For example a team name and data type can be used such as
    /// ‘acme-team.price-change’.
    pub name: EventTypeName,
    /// Indicator of the application owning this EventType.
    pub owning_application: Option<OwningApplication>,
    /// Defines the category of this EventType.
    ///
    /// The value set will influence, if not set otherwise, the default set of
    /// validations, enrichment-strategies, and the effective schema for validation.
    pub category: Category,
    /// Determines the enrichment to be performed on an Event upon reception. Enrichment is
    /// performed once upon reception (and after validation) of an Event and is only possible on
    /// fields that are not defined on the incoming Event.
    ///
    /// For event types in categories ‘business’ or ‘data’ it’s mandatory to use
    /// metadata_enrichment strategy. For ‘undefined’ event types it’s not possible to use this
    /// strategy, since metadata field is not required.
    ///
    /// See documentation for the write operation for details on behaviour in case of unsuccessful
    /// enrichment.
    #[serde(default)]
    pub enrichment_strategies: Vec<EnrichmentStrategy>,
    /// Determines how the assignment of the event to a partition should be handled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_strategy: Option<PartitionStrategy>,
    /// Compatibility mode provides a mean for event owners to evolve their schema, given changes respect the
    /// semantics defined by this field.
    ///
    /// It’s designed to be flexible enough so that producers can evolve their schemas while not
    /// inadvertently breaking existent consumers.
    ///
    /// Once defined, the compatibility mode is fixed, since otherwise it would break a predefined contract,
    /// declared by the producer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_mode: Option<CompatibilityMode>,
    pub schema: EventTypeSchema,
    /// Required when ‘partition_resolution_strategy’ is set to ‘hash’. Must be absent otherwise.
    /// Indicates the fields used for evaluation the partition of Events of this type.
    ///
    /// If this is set it MUST be a valid required field as defined in the schema.
    #[serde(default)]
    pub partition_key_fields: PartitionKeyFields,
    /// Event type cleanup policy. There are two possible values:
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_policy: Option<CleanupPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_statistic: Option<EventTypeStatistics>,
    #[serde(default)]
    pub options: EventTypeOptions,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<EventTypeAuthorization>,
    pub audience: Option<EventTypeAudience>,
    /// This is only an informational field. The events are delivered to consumers in the order they were published.
    /// No reordering is done by Nakadi.
    ///
    /// This field is useful in case the producer wants to communicate the complete order accross all the events
    /// published to all partitions. This is the case when there is an incremental generator on the producer side,
    /// for example.
    ///
    /// It differs from partition_key_fields in the sense that it’s not used for partitioning (known as sharding in
    /// some systems). The order indicated by ordering_key_fields can also differ from the order the events are in
    /// each partition, in case of out-of-order submission.
    ///
    /// In most cases, this would have just a single item (the path of the field
    /// by which this is to be ordered), but can have multiple items, in which case
    /// those are considered as a compound key, with lexicographic ordering (first
    /// item is most significant).
    #[serde(default)]
    pub ordering_key_fields: Vec<String>,
    #[serde(default)]
    pub ordering_instance_ids: Vec<String>,
    /// Date and time when this event type was created.
    pub created_at: DateTime<Utc>,
    /// Date and time when this event type was updated.
    pub updated_at: DateTime<Utc>,
}
