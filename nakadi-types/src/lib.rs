//! # Nakadi-Types
//!
//! `nakadi-types` contains types for interacting with the [Nakadi](https://nakadi.io) Event Broker.

use std::error::Error as StdError;
use std::fmt;
use std::str::FromStr;

use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize};
use url::Url;

pub(crate) mod helpers;
pub mod model;

new_type! {
    #[doc("The base URL to the Nakadi API.")]
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct NakadiBaseUrl(Url, env="NAKADI_BASE_URL");
}

impl NakadiBaseUrl {
    pub fn as_url(&self) -> &Url {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0.as_ref()
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

    pub fn new_disp<T: fmt::Display>(s: T) -> Self {
        FlowId(s.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0.as_ref()
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn random() -> Self {
        FlowId(uuid::Uuid::new_v4().to_string())
    }
}

/// Crates a random `FlowId` when converted into a `FlowId`
#[derive(Debug, Clone, Copy)]
pub struct RandomFlowId;

impl From<RandomFlowId> for FlowId {
    fn from(_: RandomFlowId) -> Self {
        Self::random()
    }
}

impl From<()> for FlowId {
    fn from(_: ()) -> Self {
        Self::random()
    }
}

impl From<String> for FlowId {
    fn from(v: String) -> Self {
        Self::new(v)
    }
}

impl From<&str> for FlowId {
    fn from(v: &str) -> Self {
        Self::new(v)
    }
}

impl From<uuid::Uuid> for FlowId {
    fn from(v: uuid::Uuid) -> Self {
        Self::new(v.to_string())
    }
}

impl fmt::Display for FlowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for FlowId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for FlowId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(FlowId::new(s))
    }
}

/// An error for cases where further investigation is not necessary
#[derive(Debug)]
pub struct Error(String);

impl Error {
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl From<serde_json::Error> for Error {
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
