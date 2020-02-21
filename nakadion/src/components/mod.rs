//! Components to directly interact with Nakadi
use std::error::Error as StdError;
use std::fmt;

use self::committer::ProvidesCommitter;
use self::connector::ProvidesConnector;

pub mod committer;
pub mod connector;
pub mod streams;

pub trait NakadionEssentials:
    ProvidesConnector + ProvidesCommitter + Send + Sync + 'static
{
}

impl<T> NakadionEssentials for T where
    T: ProvidesConnector + ProvidesCommitter + Send + Sync + 'static
{
}

#[derive(Debug)]
pub struct IoError(pub String);

impl IoError {
    pub fn new<T: Into<String>>(s: T) -> Self {
        Self(s.into())
    }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

impl StdError for IoError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}
