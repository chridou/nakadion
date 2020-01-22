use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::{
    ready,
    stream::{Fuse, Stream, StreamExt, TryStream, TryStreamExt},
};
use pin_utils::{unsafe_pinned, unsafe_unpinned};

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

#[derive(Clone)]
pub struct NakadiFrame {
    bytes: Bytes,
    stream_id: StreamId,
    frame_id: usize,
}

impl fmt::Debug for NakadiFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NakadiFrame")
            .field("stream_id", &self.stream_id.into_inner())
            .field("frame_id", &self.frame_id)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

pub struct NakadiBytesStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    stream_id: StreamId,
    bytes_stream: Fuse<St>,
    state: State,
}

struct State {
    frame_id: usize,
    frames: VecDeque<NakadiFrame>,
    buffered: Bytes,
}

impl<St> NakadiBytesStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    unsafe_pinned!(bytes_stream: Fuse<St>);
    unsafe_unpinned!(state: State);

    pub fn new(stream_id: StreamId, fused_bytes_stream: Fuse<St>) -> Self {
        Self {
            stream_id,
            bytes_stream: fused_bytes_stream,
            state: State {
                frame_id: 0,
                frames: VecDeque::new(),
                buffered: Bytes::default(),
            },
        }
    }

    pub fn new_fused(stream_id: StreamId, bytes_stream: St) -> Self {
        Self {
            stream_id,
            bytes_stream: bytes_stream.fuse(),
            state: State {
                frame_id: 0,
                frames: VecDeque::new(),
                buffered: Bytes::default(),
            },
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }
}

impl<St> Stream for NakadiBytesStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    type Item = Result<NakadiFrame, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(frame) = self.as_mut().state().frames.pop_front() {
            return Poll::Ready(Some(Ok(frame)));
        }

        // Here there are no frames left.

        match ready!(self.as_mut().bytes_stream().poll_next(cx)) {
            Some(Ok(bytes)) => {
                unimplemented!()
                // Poll::Ready(Some(Ok(frame)))
            }
            None => {
                let unframed_bytes = self.state.buffered.len();
                if unframed_bytes == 0 {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(IoError(format!(
                        "unexpected end of stream, {} unframed bytes left",
                        unframed_bytes
                    )))))
                }
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
        }
    }
}
