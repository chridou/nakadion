use serde::{
    de::{Deserializer, Error as SError},
    ser::Serializer,
    Deserialize, Serialize,
};
use serde_json::Value;

use crate::helpers::mandatory;
use crate::model::misc::OwningApplication;
use crate::Error;

use super::{
    Category, CleanupPolicy, CompatibilityMode, EnrichmentStrategy, EventTypeAudience,
    EventTypeAuthorization, EventTypeName, EventTypeOptions, EventTypeStatistics,
    PartitionKeyFields, PartitionStrategy, SchemaSyntax, SchemaType,
};

/// Definition of an event type
///
/// This struct is only used for submitting data to Nakadi.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTypeInput {
    /// Name of this EventType. The name is constrained by a regular expression.
    ///
    /// Note: the name can encode the owner/responsible for this EventType and ideally should
    /// follow a common pattern that makes it easy to read and understand, but this level of
    /// structure is not enforced. For example a team name and data type can be used such as
    /// ‘acme-team.price-change’.
    pub name: EventTypeName,
    /// Indicator of the application owning this EventType.
    pub owning_application: OwningApplication,
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
    pub enrichment_strategies: Vec<EnrichmentStrategy>,
    /// Determines how the assignment of the event to a partition should be handled.
    pub partition_strategy: PartitionStrategy,
    /// Required when ‘partition_resolution_strategy’ is set to ‘hash’. Must be absent otherwise.
    /// Indicates the fields used for evaluation the partition of Events of this type.
    ///
    /// If this is set it MUST be a valid required field as defined in the schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key_fields: Option<PartitionKeyFields>,
    /// Compatibility mode provides a mean for event owners to evolve their schema, given changes respect the
    /// semantics defined by this field.
    ///
    /// It’s designed to be flexible enough so that producers can evolve their schemas while not
    /// inadvertently breaking existent consumers.
    ///
    /// Once defined, the compatibility mode is fixed, since otherwise it would break a predefined contract,
    /// declared by the producer.
    pub compatibility_mode: CompatibilityMode,

    pub schema: EventTypeSchemaInput,
    /// Event type cleanup policy. There are two possible values:
    pub cleanup_policy: CleanupPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_statistic: Option<EventTypeStatistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<EventTypeOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<EventTypeAuthorization>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<EventTypeAudience>,
}

impl EventTypeInput {
    /// returns a builder with default values
    pub fn builder() -> EventTypeInputBuilder {
        EventTypeInputBuilder::default()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EventTypeInputBuilder {
    /// Name of this EventType. The name is constrained by a regular expression.
    ///
    /// Note: the name can encode the owner/responsible for this EventType and ideally should
    /// follow a common pattern that makes it easy to read and understand, but this level of
    /// structure is not enforced. For example a team name and data type can be used such as
    /// ‘acme-team.price-change’.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<EventTypeName>,
    /// Indicator of the application owning this EventType.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owning_application: Option<OwningApplication>,
    /// Defines the category of this EventType.
    ///
    /// The value set will influence, if not set otherwise, the default set of
    /// validations, enrichment-strategies, and the effective schema for validation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<Category>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enrichment_strategies: Option<Vec<EnrichmentStrategy>>,
    /// Determines how the assignment of the event to a partition should be handled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_strategy: Option<PartitionStrategy>,
    /// Required when ‘partition_resolution_strategy’ is set to ‘hash’. Must be absent otherwise.
    /// Indicates the fields used for evaluation the partition of Events of this type.
    ///
    /// If this is set it MUST be a valid required field as defined in the schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key_fields: Option<PartitionKeyFields>,
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

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub schema: Option<EventTypeSchemaInput>,
    /// Event type cleanup policy. There are two possible values:
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_policy: Option<CleanupPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_statistic: Option<EventTypeStatistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<EventTypeOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<EventTypeAuthorization>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<EventTypeAudience>,
}

impl EventTypeInputBuilder {
    /// Name of this EventType. The name is constrained by a regular expression.
    ///
    /// Note: the name can encode the owner/responsible for this EventType and ideally should
    /// follow a common pattern that makes it easy to read and understand, but this level of
    /// structure is not enforced. For example a team name and data type can be used such as
    /// ‘acme-team.price-change’.
    pub fn name<T: Into<EventTypeName>>(mut self, v: T) -> Self {
        self.name = Some(v.into());
        self
    }
    /// Indicator of the application owning this EventType.
    pub fn owning_application<T: Into<OwningApplication>>(mut self, v: T) -> Self {
        self.owning_application = Some(v.into());
        self
    }
    /// Defines the category of this EventType.
    ///
    /// The value set will influence, if not set otherwise, the default set of
    /// validations, enrichment-strategies, and the effective schema for validation.
    pub fn category<T: Into<Category>>(mut self, v: T) -> Self {
        self.category = Some(v.into());
        self
    }
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
    pub fn enrichment_strategy<T: Into<EnrichmentStrategy>>(mut self, v: T) -> Self {
        if let Some(ref mut strategies) = self.enrichment_strategies {
            strategies.push(v.into());
        } else {
            self.enrichment_strategies = Some(vec![v.into()]);
        }
        self
    }

    /// Determines how the assignment of the event to a partition should be handled.
    pub fn partition_strategy<T: Into<PartitionStrategy>>(mut self, v: T) -> Self {
        self.partition_strategy = Some(v.into());
        self
    }
    /// Required when ‘partition_resolution_strategy’ is set to ‘hash’. Must be absent otherwise.
    /// Indicates the fields used for evaluation the partition of Events of this type.
    ///
    /// If this is set it MUST be a valid required field as defined in the schema.
    pub fn partition_key_fields<T: Into<PartitionKeyFields>>(mut self, v: T) -> Self {
        self.partition_key_fields = Some(v.into());
        self
    }
    /// Compatibility mode provides a mean for event owners to evolve their schema, given changes respect the
    /// semantics defined by this field.
    ///
    /// It’s designed to be flexible enough so that producers can evolve their schemas while not
    /// inadvertently breaking existent consumers.
    ///
    /// Once defined, the compatibility mode is fixed, since otherwise it would break a predefined contract,
    /// declared by the producer.
    pub fn compatibility_mode<T: Into<CompatibilityMode>>(mut self, v: T) -> Self {
        self.compatibility_mode = Some(v.into());
        self
    }

    pub fn schema<T: Into<EventTypeSchemaInput>>(mut self, v: T) -> Self {
        self.schema = Some(v.into());
        self
    }
    /// Event type cleanup policy. There are two possible values:
    pub fn cleanup_policy<T: Into<CleanupPolicy>>(mut self, v: T) -> Self {
        self.cleanup_policy = Some(v.into());
        self
    }
    pub fn default_statistic<T: Into<EventTypeStatistics>>(mut self, v: T) -> Self {
        self.default_statistic = Some(v.into());
        self
    }
    pub fn options<T: Into<EventTypeOptions>>(mut self, v: T) -> Self {
        self.options = Some(v.into());
        self
    }
    pub fn authorization<T: Into<EventTypeAuthorization>>(mut self, v: T) -> Self {
        self.authorization = Some(v.into());
        self
    }
    pub fn audience<T: Into<EventTypeAudience>>(mut self, v: T) -> Self {
        self.audience = Some(v.into());
        self
    }

    /// Validates the data and returns an `EventTypeInput` if valid.
    pub fn build(self) -> Result<EventTypeInput, Error> {
        let name = mandatory(self.name, "name")?;
        let owning_application = mandatory(self.owning_application, "owning_application")?;
        let category = mandatory(self.category, "category")?;
        let enrichment_strategies = self.enrichment_strategies.unwrap_or_default();
        let partition_strategy = mandatory(self.partition_strategy, "partition_strategy")?;
        let partition_key_fields = self.partition_key_fields;
        let compatibility_mode = mandatory(self.compatibility_mode, "compatibility_mode")?;
        let schema = mandatory(self.schema, "schema")?;
        let cleanup_policy = mandatory(self.cleanup_policy, "cleanup_policy")?;
        let default_statistic = self.default_statistic;
        let options = self.options;
        let authorization = self.authorization;
        let audience = self.audience;

        if partition_strategy == PartitionStrategy::Hash {
            if let Some(ref partition_key_fields) = partition_key_fields {
                if partition_key_fields.is_empty() {
                    return Err(Error::new(
                        "'partition_key_fields' is set but must not be empty if partition strategy is 'Hash'",
                    ));
                }
            } else {
                return Err(Error::new(
                    "'partition_key_fields' must be set if partition strategy is 'Hash'",
                ));
            }
        } else {
            return Err(Error::new(
                "'partition_key_fields' must be none if partition strategy is not 'Hash'",
            ));
        }

        Ok(EventTypeInput {
            name,
            owning_application,
            category,
            enrichment_strategies,
            partition_strategy,
            partition_key_fields,
            compatibility_mode,
            schema,
            cleanup_policy,
            default_statistic,
            options,
            authorization,
            audience,
        })
    }
}

/// The most recent schema for this EventType. Submitted events will be validated against it.
#[derive(Debug, Clone, PartialEq)]
pub enum EventTypeSchemaInput {
    Json(Value),
}

impl EventTypeSchemaInput {
    pub fn json_schema<T: Into<Value>>(v: T) -> Self {
        Self::Json(v.into())
    }

    pub fn json_schema_parsed(v: &str) -> Result<Self, Error> {
        let parsed = serde_json::from_str(v)?;
        Ok(Self::Json(parsed))
    }

    pub fn schema_type(&self) -> SchemaType {
        match self {
            EventTypeSchemaInput::Json(_) => SchemaType::JsonSchema,
        }
    }

    pub fn schema_syntax(&self) -> SchemaSyntax {
        match self {
            EventTypeSchemaInput::Json(ref syntax) => {
                SchemaSyntax(serde_json::to_string(syntax).unwrap())
            }
        }
    }
}

impl Serialize for EventTypeSchemaInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let wrapper = EventTypeSchemaInputSer {
            schema_type: self.schema_type(),
            schema: self.schema_syntax(),
        };

        wrapper.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EventTypeSchemaInput {
    fn deserialize<D>(deserializer: D) -> Result<EventTypeSchemaInput, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wrapper = EventTypeSchemaInputSer::deserialize(deserializer)?;

        match wrapper.schema_type {
            SchemaType::JsonSchema => {
                let schema_syntax =
                    serde_json::from_str(wrapper.schema.as_ref()).map_err(SError::custom)?;
                Ok(EventTypeSchemaInput::Json(schema_syntax))
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct EventTypeSchemaInputSer {
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub schema: SchemaSyntax,
}

#[test]
fn a_schema_input_can_be_parsed() {
    let input = EventTypeSchemaInput::json_schema_parsed(r#"{"description":"test event b","properties":{"count":{"type":"integer"}},"required":["count"]}"#).unwrap();
    assert_eq!(input.schema_type(), SchemaType::JsonSchema);
}
