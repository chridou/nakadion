use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{de::Error as SerdeError, Deserialize, Deserializer};

mod model;

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
