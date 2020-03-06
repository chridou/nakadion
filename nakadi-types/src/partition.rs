//! Types defining a partition.
//!
//! This covers the partitions of an `EventType`. Subscriptions, monitoring
//! etc. might extend the types in here with more fields.
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

new_type! {
    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
    pub struct PartitionId(String, env="PARTITION_ID");
}

impl PartitionId {
    pub fn join_into_cursor(self, offset: CursorOffset) -> Cursor {
        Cursor::new(self, offset)
    }
}

impl From<Partition> for PartitionId {
    fn from(p: Partition) -> Self {
        p.into_partition_id()
    }
}

/// Partition information. Can be helpful when trying to start a stream using an unmanaged API.
///
/// This information is not related to the state of the consumer clients.
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#definition_Partition)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub partition: PartitionId,
    /// An offset of the oldest available Event in that partition. This value will be changing
    /// upon removal of Events from the partition by the background archiving/cleanup mechanism.
    pub oldest_available_offset: CursorOffset,
    /// An offset of the newest available Event in that partition. This value will be changing
    /// upon reception of new events for this partition by Nakadi.
    ///
    /// This value can be used to construct a cursor when opening streams (see
    /// GET /event-type/{name}/events for details).
    ///
    /// Might assume the special name BEGIN, meaning a pointer to the offset of the oldest
    /// available event in the partition.
    pub newest_available_offset: CursorOffset,
    /// Approximate number of events unconsumed by the client. This is also known as consumer lag and is used for
    /// monitoring purposes by consumers interested in keeping an eye on the number of unconsumed events.
    ///
    /// If the event type uses ‘compact’ cleanup policy - then the actual number of unconsumed events in this
    /// partition can be lower than the one reported in this field.
    pub unconsumed_events: Option<u64>,
}

impl Partition {
    pub fn into_partition_id(self) -> PartitionId {
        self.partition
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

impl Cursor {
    pub fn new<A: Into<PartitionId>, B: Into<CursorOffset>>(partition: A, offset: B) -> Self {
        Cursor {
            partition: partition.into(),
            offset: offset.into(),
        }
    }
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
    type Err = Error;

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

impl CursorDistanceQuery {
    pub fn new<A: Into<Cursor>, B: Into<Cursor>>(initial_cursor: A, final_cursor: B) -> Self {
        CursorDistanceQuery {
            initial_cursor: initial_cursor.into(),
            final_cursor: final_cursor.into(),
        }
    }
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
    use crate::partition::CursorOffset;

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
