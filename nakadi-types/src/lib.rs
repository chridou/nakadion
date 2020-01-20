//! # Nakadi-Types
//!
//! `nakadi-types` contains types for interacting with the [Nakadi](https://nakadi.io) Event Broker.

use std::error::Error;
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

    pub fn from_env() -> Result<Self, GenericError> {
        from_env!(
            postfix => env_vars::NAKADI_BASE_URL_ENV_VAR
        )
    }

    pub fn from_env_named<T: AsRef<str>>(name: T) -> Result<Self, GenericError> {
        from_env!(name.as_ref())
    }

    pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, GenericError> {
        from_env!(
            prefix => prefix.as_ref() , postfix => env_vars::NAKADI_BASE_URL_ENV_VAR
        )
    }

    pub fn as_url(&self) -> &Url {
        &self.0
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

#[derive(Debug)]
pub struct GenericError(pub String);

impl GenericError {
    pub fn new<T: Into<String>>(msg: T) -> Self {
        Self(msg.into())
    }

    pub fn boxed(self) -> Box<dyn Error> {
        Box::new(self)
    }
}

impl fmt::Display for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl Error for GenericError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl<T> From<T> for GenericError
where
    T: Into<String>,
{
    fn from(msg: T) -> Self {
        Self::new(msg)
    }
}

pub fn deserialize_empty_string_is_none<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
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
            .map_err(|err| SerdeError::custom(err.to_string()))?;
        Ok(Some(parsed))
    }
}
