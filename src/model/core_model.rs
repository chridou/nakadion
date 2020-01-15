//! Essential types
use std::convert::AsRef;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::env_vars::*;
use crate::helpers::MessageError;

use must_env;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PartitionId(String);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Partition {
    event_type: EventTypeName,
    partition: PartitionId,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct StreamId(Uuid);

impl FromStr for StreamId {
    type Err = Box<dyn Error + 'static>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(StreamId(s.parse().map_err(|err| {
            MessageError::new(format!("could not parse stream id: {}", err))
        })?))
    }
}

/// The flow id of the request, which is written into the logs and passed to called services. Helpful
/// for operational troubleshooting and log analysis.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get*x-flow-id)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowId(String);

impl FlowId {
    pub fn new<T: Into<String>>(s: T) -> Self {
        FlowId(s.into())
    }
}

impl Default for FlowId {
    fn default() -> Self {
        FlowId(uuid::Uuid::new_v4().to_string())
    }
}

impl fmt::Display for FlowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T> From<T> for FlowId
where
    T: Into<String>,
{
    fn from(v: T) -> Self {
        FlowId::new(v)
    }
}

impl AsRef<str> for FlowId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Name of an EventType. The name is constrained by a regular expression.
///
/// Note: the name can encode the owner/responsible for this EventType and ideally should
/// follow a common pattern that makes it easy to read and understand, but this level of
/// structure is not enforced. For example a team name and data type can be used such as
/// ‘acme-team.price-change’.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventType*name)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct EventTypeName(String);

impl EventTypeName {
    pub fn new(v: impl Into<String>) -> Self {
        EventTypeName(v.into())
    }

    pub fn from_env() -> Result<Self, Box<dyn Error + 'static>> {
        Self::from_env_named(NAKADION_EVENT_TYPE_ENV_VAR)
    }

    pub fn from_env_named<T: AsRef<str>>(name: T) -> Result<Self, Box<dyn Error + 'static>> {
        Ok(Self::new(must_env!(name.as_ref())?))
    }
}

impl fmt::Display for EventTypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for EventTypeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Represents event-type:partition pair.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_EventTypePartition)
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct EventTypePartition {
    pub event_type: EventTypeName,
    pub partition: PartitionId,
}