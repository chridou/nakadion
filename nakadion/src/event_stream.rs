use std::io::Error as IoError;
use std::time::Instant;

use bytes::Bytes;
use futures::stream::Stream;

use nakadion_types::model::subscription::StreamId;

use crate::api::BytesStream;

pub type BatchBytes = Vec<u8>;
pub type BatchResult = Result<RawBatch, IoError>;

/// A line as received from Nakadi plus a timestamp.
pub struct RawBatch {
    /// The bytes received as a line from Nakadi
    pub bytes: BatchBytes,
    /// The timestamp for when this line was received
    pub received_at: Instant,
}

pub struct NakadiBytesStream<'a> {
    stream_id: StreamId,
    bytes_stream: BytesStream<'a>,
}

impl<'a> NakadiBytesStream<'a> {
    pub fn new(stream_id: StreamId, bytes_stream: BytesStream<'a>) -> Self {
        Self {
            stream_id,
            bytes_stream,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn into_stream(self) -> BytesStream<'a> {
        self.bytes_stream
    }

    pub fn explode(self) -> (StreamId, BytesStream<'a>) {
        (self.stream_id, self.bytes_stream)
    }
}
