use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::model::PartitionId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    pub partition: PartitionId,
    pub offset: CursorOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorOffset {
    Begin,
    N(String),
}

impl CursorOffset {
    pub fn new<T: AsRef<str>>(offset: T) -> Self {
        match offset.as_ref() {
            "begin" => CursorOffset::Begin,
            other => CursorOffset::N(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            CursorOffset::Begin => "begin",
            CursorOffset::N(ref v) => v.as_ref(),
        }
    }
}

impl fmt::Display for CursorOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CursorOffset::Begin => write!(f, "begin")?,
            CursorOffset::N(ref v) => write!(f, "{}", v)?,
        }
        Ok(())
    }
}

impl FromStr for CursorOffset {
    type Err = Box<dyn Error + 'static>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

impl Serialize for CursorOffset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CursorOffset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;

        Ok(Self::new(value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorDistanceQuery {
    pub initial_cursor: Cursor,
    pub final_cursor: Cursor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorDistanceResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorLagResult;
