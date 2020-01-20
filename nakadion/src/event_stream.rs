use std::io::Error as IoError;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::stream::Stream;

use nakadi_types::model::subscription::StreamId;

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

pub struct NakadiBytesStream {
    stream_id: StreamId,
    bytes_stream: BytesStream,
}

impl NakadiBytesStream {
    pub fn new(stream_id: StreamId, bytes_stream: BytesStream) -> Self {
        Self {
            stream_id,
            bytes_stream,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn into_stream(self) -> BytesStream {
        self.bytes_stream
    }

    pub fn explode(self) -> (StreamId, BytesStream) {
        (self.stream_id, self.bytes_stream)
    }

    pub fn framed(self) -> FramedBytesStream<BytesStream> {
        FramedBytesStream {
            stream_id: self.stream_id(),
            stream: self.bytes_stream,
        }
    }
}

pub struct FramedBytesStream<S> {
    stream_id: StreamId,
    stream: S,
}

impl<S> FramedBytesStream<S> {}

impl<S> Stream for FramedBytesStream<S>
where
    S: Stream<Item = Result<Bytes, IoError>>,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }
}
