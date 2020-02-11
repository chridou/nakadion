//! # Nakadi-Types
//!
//! `nakadi-types` contains types for interacting with the [Nakadi](https://nakadi.io) Event Broker.

use std::error::Error as StdError as StdError;
use std::fmt;
use std::str::FromStr;

use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize};
use url::Url;

pub(crate) mod env_vars;
pub mod model;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NakadiBaseUrl(Url);

impl NakadiBaseUrl {
    pub fn new<T: Into<Url>>(url: T) -> Self {
        NakadiBaseUrl(url.into())
    }

    env_funs!("NAKADI_BASE_URL");

    pub fn as_url(&self) -> &Url {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0.as_ref()
    }

    pub fn into_inner(self) -> Url {
        self.0
    }
}

impl FromStr for NakadiBaseUrl {
    type Err = GenericError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(NakadiBaseUrl(s.parse().map_err(|err| {
            GenericError::new(format!("could not parse nakadi base url: {}", err))
        })?))
    }
}

impl AsRef<str> for NakadiBaseUrl {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// The flow id of the request, which is written into the logs and passed to called services. Helpful
/// for operational troubleshooting and log analysis.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types_get*x-flow-id)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct FlowId(String);

impl FlowId {
    pub fn new<T: Into<String>>(s: T) -> Self {
        FlowId(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0.as_ref()
    }

    pub fn into_inner(self) -> String {
        self.0
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

impl FromStr for FlowId {
    type Err = GenericError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(FlowId::new(s))
    }
}

#[derive(Debug)]
pub struct GenericError(String);

impl GenericError {
    pub fn new<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }

    pub fn from_error<E: StdError + Send + 'static>(err: E) -> Self {
        Self::new(err.to_string())
    }

    pub fn boxed(self) -> Box<dyn StdError + Send + 'static> {
        Box::new(self)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl StdError for GenericError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl From<serde_json::Error> for GenericError {
    fn from(err: serde_json::Error) -> Self {
        Self::new(err.to_string())
    }
}

fn deserialize_empty_string_is_none<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        Ok(None)
    } else {
        let parsed = s
            .parse::<T>()
            .map_err(|err| SerdeError::custom(format!("deserialization error: {}", err)))?;
        Ok(Some(parsed))
    }
}
