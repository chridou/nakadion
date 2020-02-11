use std::error::Error as StdError;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::GenericError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventId(Uuid);

impl EventId {
    pub fn new<T: Into<Uuid>>(id: T) -> Self {
        Self(id.into())
    }

    pub fn into_inner(self) -> Uuid {
        self.0
    }
}

impl FromStr for EventId {
    type Err = GenericError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(EventId(s.parse().map_err(|err| {
            GenericError::new(format!("could not parse event id: {}", err))
        })?))
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
