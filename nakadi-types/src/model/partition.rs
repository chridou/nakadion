//! Types defining a partition.
//!
//! This covers the partoitions of an `EventType`. Subscriptions, monitoring
//! etc. might extend the types in here with more fields.
use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PartitionId(String);

impl PartitionId {
    pub fn new<T: Into<String>>(id: T) -> Self {
        Self(id.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A cursor with an offset
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Cursor)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cursor {
    pub partition: PartitionId,
    pub offset: CursorOffset,
}

/// Offset of the event being pointed to.
///
/// Note that if you want to specify beginning position of a stream with first event at offset N,
/// you should specify offset N-1.
/// This applies in cases when you create new subscription or reset subscription offsets.
/// Also for stream start offsets one can use special value:
///
/// begin - read from the oldest available event.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Cursor*offset)
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

/// A query for cursor distances
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorDistanceQuery {
    pub initial_cursor: Cursor,
    pub final_cursor: Cursor,
}

/// A result for `CursorDistanceQuery`
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/event-types/name/cursor-distances_post)
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CursorDistanceResult {
    #[serde(flatten)]
    pub query: CursorDistanceQuery,
    /// Number of events between two offsets. Initial offset is exclusive. It’s only zero when both provided offsets
    /// are equal.
    pub distance: u64,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::model::partition::CursorOffset;

    use serde_json::{self, json};

    #[test]
    fn cursor_distance_result() {
        let json = json!({
            "initial_cursor": {
                "partition": "the partition",
                "offset": "12345",
            },
            "final_cursor": {
                "partition": "another partition",
                "offset": "6789",
            },
            "distance": 12345,
        });

        let sample = CursorDistanceResult {
            query: CursorDistanceQuery {
                initial_cursor: Cursor {
                    partition: PartitionId::new("the partition"),
                    offset: CursorOffset::new("12345"),
                },
                final_cursor: Cursor {
                    partition: PartitionId::new("another partition"),
                    offset: CursorOffset::new("6789"),
                },
            },
            distance: 12345,
        };

        assert_eq!(
            serde_json::to_value(sample.clone()).unwrap(),
            json,
            "serialize"
        );
        assert_eq!(
            serde_json::from_value::<CursorDistanceResult>(json).unwrap(),
            sample,
            "deserialize"
        );
    }
}
