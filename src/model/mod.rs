use uuid::Uuid;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamId(Uuid);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionId(Uuid);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventId(Uuid);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct CursorToken(Uuid);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionId(String);

pub struct PartitionIdBorrowed<'a>(&'a str);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventType(String);

pub struct EventTypeBorrowed<'a>(&'a str);

impl EventTypeBorrowed<'_> {
    pub fn to_event_type(&self) -> EventType {
        EventType(self.0.into())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Offset(String);

pub struct OffsetBorrowed<'a>(&'a str);

pub struct Cursor {
    pub partition: PartitionId,
    pub offset: Offset,
    pub event_type: EventType,
    pub cursor_token: CursorToken,
}

pub struct CursorBorrowed<'a> {
    pub partition: PartitionIdBorrowed<'a>,
    pub offset: OffsetBorrowed<'a>,
    pub event_type: EventTypeBorrowed<'a>,
    pub cursor_token: CursorToken,
}

mod input {}
