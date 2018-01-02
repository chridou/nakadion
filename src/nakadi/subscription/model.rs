use AfterBatchAction;

/// A `StreamId` identifies a subscription. It must be provided for checkpointing with
/// a `Cursor`.
#[derive(Clone, Debug)]
pub struct StreamId(pub String);

impl StreamId {
    pub fn new<T: Into<String>>(id: T) -> Self {
        StreamId(id.into())
    }
}

/// Information on a current batch. This might be
/// useful for a `Handler` that wants to do checkpointing on its own.
#[derive(Clone, Debug)]
pub struct BatchCommitData<'a> {
    pub stream_id: StreamId,
    pub cursor: &'a [u8],
}

/// The [`Nakadi Event Type`](https://github.com/zalando/nakadi#creating-event-types).
/// Similiar to a topic.
#[derive(Clone, Debug)]
pub struct EventType<'a>(pub &'a str);

impl<'a> EventType<'a> {
    /// Creates a new instance of an
    /// [`EventType`](https://github.com/zalando/nakadi#creating-event-types).
    pub fn new(value: &'a str) -> EventType {
        EventType(value)
    }
}

/// A `SubscriptionId` is used to guarantee a continous flow of events for a client.
#[derive(Clone, Debug)]
pub struct SubscriptionId(pub String);

pub trait BatchHandler {
    fn handle(&self, event_type: EventType, data: &[u8]) -> AfterBatchAction;
}

pub trait HandlerFactory {
    type Handler: BatchHandler;
    fn create_handler(&self) -> BatchHandler;
}
