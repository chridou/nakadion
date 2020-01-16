use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

mod core_model;
mod cursor;
mod event_type;
mod misc;
mod publishing;
mod subscription;

pub use self::core_model::*;
pub use self::cursor::*;
pub use self::event_type::*;
pub use self::misc::*;
pub use self::publishing::*;
pub use self::subscription::*;

use crate::env_vars::*;
use crate::helpers::MessageError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NakadiHost(Url);

impl NakadiHost {
    pub fn new(url: Url) -> Self {
        Self(url)
    }

    pub fn into_inner(self) -> Url {
        self.0
    }

    pub fn from_env() -> Result<Self, Box<dyn Error + 'static>> {
        Self::from_env_named(NAKADION_NAKADI_HOST_ENV_VAR)
    }

    pub fn from_env_named<T: AsRef<str>>(name: T) -> Result<Self, Box<dyn Error + 'static>> {
        must_env_parsed!(name.as_ref())
    }
}

impl AsRef<str> for NakadiHost {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl FromStr for NakadiHost {
    type Err = Box<dyn Error + 'static>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(NakadiHost(s.parse().map_err(|err| {
            MessageError::new(format!("could not parse nakadi host id: {}", err))
        })?))
    }
}

impl From<Url> for NakadiHost {
    fn from(url: Url) -> Self {
        Self(url)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventId(pub Uuid);
