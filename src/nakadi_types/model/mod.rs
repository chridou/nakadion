use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{de::Error as SerdeError, Deserialize, Deserializer};

pub mod event;
pub mod partition;
pub mod publishing;
pub mod support_types;

#[derive(Debug)]
pub struct MessageError(pub String);

impl MessageError {
    pub fn new<T: Into<String>>(msg: T) -> Self {
        Self(msg.into())
    }

    pub fn boxed(self) -> Box<dyn Error> {
        Box::new(self)
    }
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl Error for MessageError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl<T> From<T> for MessageError
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
