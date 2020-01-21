use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::{
    ready,
    stream::{Fuse, Stream, StreamExt, TryStream, TryStreamExt},
};
use pin_utils::unsafe_pinned;

use nakadi_types::model::subscription::StreamId;

use crate::api::{BytesStream, IoError};

pub type BatchBytes = Vec<u8>;
pub type BatchResult = Result<RawBatch, IoError>;

/// A line as received from Nakadi plus a timestamp.
pub struct RawBatch {
    /// The bytes received as a line from Nakadi
    pub bytes: BatchBytes,
    /// The timestamp for when this line was received
    pub received_at: Instant,
}

pub struct NakadiFrame {
    bytes: Bytes,
    stream_id: StreamId,
    frame_id: usize,
}

pub struct NakadiBytesStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    stream_id: StreamId,
    bytes_stream: Fuse<St>,
    frame_id: usize,
}

impl<St> NakadiBytesStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    unsafe_pinned!(bytes_stream: Fuse<St>);

    pub fn new(stream_id: StreamId, fused_bytes_stream: Fuse<St>) -> Self {
        Self {
            stream_id,
            bytes_stream: fused_bytes_stream,
            frame_id: 0,
        }
    }

    pub fn new_fused(stream_id: StreamId, bytes_stream: St) -> Self {
        Self {
            stream_id,
            bytes_stream: bytes_stream.fuse(),
            frame_id: 0,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Acquires a pinned mutable reference to the underlying stream that this
    /// combinator is pulling from.
    fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut Fuse<St>> {
        self.bytes_stream()
    }

    /*  pub fn framed(self) -> FramedBytesStream<BytesStream> {
        FramedBytesStream {
            stream_id: self.stream_id(),
            stream: self.bytes_stream.fused(),
        }
    }*/
}

impl<St> Stream for NakadiBytesStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    type Item = Result<NakadiFrame, IoError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let next_frame_id = self.frame_id;
        let stream_id = self.stream_id;
        self.frame_id = next_frame_id + 1;
        let stream = self.get_pin_mut();
        match ready!(stream.poll_next(cx)) {
            None => Poll::Ready(None),
            Some(Ok(bytes)) => {
                let frame = NakadiFrame {
                    bytes,
                    stream_id,
                    frame_id: next_frame_id,
                };
                Poll::Ready(Some(Ok(frame)))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
        }
    }
}
