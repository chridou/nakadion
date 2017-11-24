/// A `StreamId` identifies a subscription. It must be provided for checkpointing with
/// a `Cursor`.
#[derive(Clone, Debug, Deserialize)]
pub struct StreamId(pub String);

impl StreamId {
    pub fn new(id: String) -> Self {
        StreamId(id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Offset<'a>(pub &'a str);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CursorToken<'a>(pub &'a str);

/// A `Cursor` describes a position in the stream. The cursor is used for checkpointing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cursor<'a> {
    #[serde(borrow)]
    pub partition: PartitionId<'a>,
    #[serde(borrow)]
    pub offset: Offset<'a>,
    #[serde(borrow)]
    pub event_type: EventType<'a>,
    #[serde(borrow)]
    pub cursor_token: CursorToken<'a>,
}

/// Information on a current batch. This might be
/// useful for a `Handler` that wants to do checkpointing on its own.
#[derive(Clone, Debug)]
pub struct BatchCommitData<'a> {
    pub stream_id: StreamId,
    pub cursor: Cursor<'a>,
}

/// The [`Nakadi Event Type`](https://github.com/zalando/nakadi#creating-event-types).
/// Similiar to a topic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventType<'a>(pub &'a str);

impl<'a> EventType<'a> {
    /// Creates a new instance of an
    /// [`EventType`](https://github.com/zalando/nakadi#creating-event-types).
    pub fn new(value: &'a str) -> EventType {
        EventType(value)
    }
}

/// A partition id that comes with a `Cursor`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionId<'a>(pub &'a str);

mod info {
    /// Information on a partition
    #[derive(Debug, Deserialize)]
    pub struct PartitionInfo {
        pub partition: String,
        pub stream_id: String,
        pub unconsumed_events: usize,
    }

    /// An `EventType` can be published on multiple partitions.
    #[derive(Debug, Deserialize)]
    pub struct EventTypeInfo {
        pub event_type: String,
        pub partitions: Vec<PartitionInfo>,
    }

    impl EventTypeInfo {
        /// Returns the number of partitions this `EventType` is
        /// published over.
        pub fn num_partitions(&self) -> usize {
            self.partitions.len()
        }
    }

    /// A stream can provide multiple `EventTypes` where each of them can have
    /// its own partitioning setup.
    #[derive(Debug, Deserialize, Default)]
    pub struct StreamInfo {
        #[serde(rename = "items")]
        pub event_types: Vec<EventTypeInfo>,
    }

    impl StreamInfo {
        /// Returns the number of partitions of the `EventType`
        /// that has the most partitions.
        pub fn max_partitions(&self) -> usize {
            self.event_types
                .iter()
                .map(|et| et.num_partitions())
                .max()
                .unwrap_or(0)
        }
    }
}
