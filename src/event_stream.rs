use std::io::Error as IoError;
use std::time::Instant;

use bytes::Bytes;
use futures::stream::Stream;

use crate::model::StreamId;

pub type BatchBytes = Vec<u8>;
pub type BatchResult = Result<RawBatch, IoError>;

/// A line as received from Nakadi plus a timestamp.
pub struct RawBatch {
    /// The bytes received as a line from Nakadi
    pub bytes: BatchBytes,
    /// The timestamp for when this line was received
    pub received_at: Instant,
}

pub type ByteStream = Box<dyn Stream<Item = Result<Bytes, ()>> + Send + Sync + 'static>;

pub struct EventStream {
    pub stream_id: StreamId,
    pub stream: ByteStream,
}
