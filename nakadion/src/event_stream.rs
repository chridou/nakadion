use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::{Buf as _, Bytes};
use futures::stream::Stream;
use log::warn;
use pin_utils::{unsafe_pinned, unsafe_unpinned};

use nakadi_types::model::subscription::StreamId;

use crate::api::IoError;

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
    received_at: Instant,
    stream_id: StreamId,
    frame_id: usize,
}

impl NakadiFrame {
    pub fn new(bytes: Vec<u8>, received_at: Instant, stream_id: StreamId, frame_id: usize) -> Self {
        Self {
            bytes: bytes.into(),
            received_at,
            stream_id,
            frame_id,
        }
    }
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
    bytes_stream: St,
    state: State,
}

struct State {
    frame_id: usize,
    frames: VecDeque<NakadiFrame>,
    unfinished_frame: Vec<u8>,
    first_byte_received_at: Instant,
    is_source_done: bool,
    done_err: Option<IoError>,
}

impl<St> NakadiBytesStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    unsafe_pinned!(bytes_stream: St);
    unsafe_unpinned!(state: State);

    pub fn new(stream_id: StreamId, bytes_stream: St) -> Self {
        let now = Instant::now();
        Self {
            stream_id,
            bytes_stream,
            state: State {
                frame_id: 0,
                frames: VecDeque::new(),
                unfinished_frame: Vec::with_capacity(4096),
                first_byte_received_at: now,
                is_source_done: false,
                done_err: None,
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
        if self.state.is_source_done {
            let state = self.as_mut().state();
            if let Some(frame) = state.frames.pop_front() {
                return Poll::Ready(Some(Ok(frame)));
            }

            if let Some(err) = state.done_err.take() {
                return Poll::Ready(Some(Err(err)));
            }

            Poll::Ready(None)
        } else {
            loop {
                match self.as_mut().bytes_stream().poll_next(cx) {
                    Poll::Ready(Some(Ok(mut bytes))) => {
                        if bytes.is_empty() {
                            continue;
                        }

                        let stream_id = self.stream_id;
                        let state = self.as_mut().state();
                        if state.unfinished_frame.is_empty() {
                            state.first_byte_received_at = Instant::now();
                        }

                        loop {
                            if bytes.is_empty() {
                                break;
                            }

                            if let Some(pos) = bytes.iter().position(|b| *b == b'\n') {
                                let to_append = bytes.split_to(pos);
                                bytes.advance(1);

                                state.unfinished_frame.extend_from_slice(&to_append);

                                let finished_frame = std::mem::replace(
                                    &mut state.unfinished_frame,
                                    Vec::with_capacity(4096),
                                );

                                state.frames.push_back(NakadiFrame {
                                    bytes: finished_frame.into(),
                                    received_at: state.first_byte_received_at,
                                    stream_id,
                                    frame_id: state.frame_id,
                                });

                                state.frame_id += 1;
                            } else {
                                state.unfinished_frame.extend_from_slice(&bytes);
                                break;
                            }
                        }

                        if let Some(frame) = state.frames.pop_front() {
                            return Poll::Ready(Some(Ok(frame)));
                        }
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        let unframed_bytes = self.state.unfinished_frame.len();
                        if unframed_bytes > 0 {
                            warn!(
                                "unexpected end of stream '{}', {} unframed bytes left",
                                self.stream_id, unframed_bytes
                            )
                        }

                        let state = self.as_mut().state();
                        state.is_source_done = true;
                        if let Some(frame) = state.frames.pop_front() {
                            return Poll::Ready(Some(Ok(frame)));
                        }
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Some(Err(err))) => {
                        let state = self.as_mut().state();
                        state.is_source_done = true;

                        if let Some(frame) = state.frames.pop_front() {
                            state.done_err = Some(err);
                            return Poll::Ready(Some(Ok(frame)));
                        }

                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Pending => {
                        if let Some(frame) = self.as_mut().state().frames.pop_front() {
                            return Poll::Ready(Some(Ok(frame)));
                        }
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {}
