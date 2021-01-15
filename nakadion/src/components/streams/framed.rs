use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::{Buf as _, Bytes};
use futures::stream::Stream;
use pin_utils::{unsafe_pinned, unsafe_unpinned};
#[cfg(test)]
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::api::IoError;
use crate::instrumentation::{Instrumentation, Instruments};

/// A frame (line) from Nakadi
#[derive(Clone)]
pub struct NakadiFrame {
    pub bytes: Bytes,
    /// Timestamp when the first byte was received
    pub started_at: Instant,
    /// Timestamp when the frame was completed
    pub completed_at: Instant,
    pub frame_id: usize,
}

impl NakadiFrame {
    #[allow(dead_code)]
    pub fn new(
        bytes: Vec<u8>,
        started_at: Instant,
        completed_at: Instant,
        frame_id: usize,
    ) -> Self {
        Self {
            bytes: bytes.into(),
            started_at,
            completed_at,
            frame_id,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl fmt::Debug for NakadiFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NakadiFrame")
            .field("frame_id", &self.frame_id)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

impl AsRef<[u8]> for NakadiFrame {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

pub struct FramedStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    bytes_stream: St,
    state: State,
    instrumentation: Instrumentation,
}

struct State {
    frame_id: usize,
    frames: VecDeque<NakadiFrame>,
    unfinished_frame: Vec<u8>,
    first_byte_received_at: Instant,
    is_source_done: bool,
    done_err: Option<IoError>,
}

impl<St> FramedStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    unsafe_pinned!(bytes_stream: St);
    unsafe_unpinned!(state: State);

    pub fn new(bytes_stream: St, instrumentation: Instrumentation) -> Self {
        let now = Instant::now();
        Self {
            bytes_stream,
            instrumentation,
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
}

impl<St> Stream for FramedStream<St>
where
    St: Stream<Item = Result<Bytes, IoError>>,
{
    type Item = Result<NakadiFrame, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.state.is_source_done {
            // If the source stream is finished flush all
            // collected frames or return otherwise return a
            // previous error
            let state = self.as_mut().state();
            if let Some(frame) = state.frames.pop_front() {
                return Poll::Ready(Some(Ok(frame)));
            }

            if let Some(err) = state.done_err.take() {
                return Poll::Ready(Some(Err(err)));
            }

            Poll::Ready(None)
        } else {
            let instrumentation = self.instrumentation.clone();
            loop {
                // Receive the next chunk
                match self.as_mut().bytes_stream().poll_next(cx) {
                    Poll::Ready(Some(Ok(mut bytes))) => {
                        // do not process empty chunks
                        if bytes.is_empty() {
                            println!("EMPTY, IS THIS THE END MY FRIEND?!");
                            continue;
                        }

                        instrumentation.stream_chunk_received(bytes.len());

                        let state = self.as_mut().state();

                        loop {
                            // If there are no bytes left to process
                            // proceed with the potentially next chunk
                            if bytes.is_empty() {
                                break;
                            }

                            // If we have nothing collected so far
                            // the non empty chunk must start a new frame
                            if state.unfinished_frame.is_empty() {
                                state.first_byte_received_at = Instant::now();
                            }

                            if let Some(pos) = bytes.iter().position(|b| *b == b'\n') {
                                // Extract the missing part of the chunk
                                // up to before the new line
                                let to_append_to_complete = bytes.split_to(pos);
                                // Skip the new line
                                bytes.advance(1);

                                // append the rest to the current unfinished chunk
                                // to complete it
                                state
                                    .unfinished_frame
                                    .extend_from_slice(&to_append_to_complete);

                                // if the completed chunk is not empty
                                // "collect" it
                                if !state.unfinished_frame.is_empty() {
                                    // Take the completed chunk and prepare a new
                                    // buffer for the next frame
                                    let finished_frame = std::mem::replace(
                                        &mut state.unfinished_frame,
                                        Vec::with_capacity(4096),
                                    );

                                    instrumentation.stream_frame_completed(
                                        finished_frame.len(),
                                        state.first_byte_received_at.elapsed(),
                                    );

                                    // Append the new frame to the collected frame
                                    state.frames.push_back(NakadiFrame {
                                        bytes: finished_frame.into(),
                                        started_at: state.first_byte_received_at,
                                        completed_at: Instant::now(),
                                        frame_id: state.frame_id,
                                    });

                                    state.frame_id += 1;
                                }
                            } else {
                                // No new line, new bytes for the unfinished chunk
                                state.unfinished_frame.extend_from_slice(&bytes);
                                break;
                            }
                        }

                        if let Some(frame) = state.frames.pop_front() {
                            // deliver the first of the completed chunks
                            return Poll::Ready(Some(Ok(frame)));
                        }
                    }
                    Poll::Ready(None) => {
                        /*
                        let unframed_bytes = self.state.unfinished_frame.len();
                        if unframed_bytes > 0 {
                             warn!(
                                "unexpected end of stream, {} unframed bytes left",
                                unframed_bytes
                            )
                        }*/

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
mod test {
    use crate::instrumentation::Instrumentation;
    use bytes::Bytes;
    use futures::stream::{self, BoxStream, Stream, StreamExt, TryStreamExt};

    use super::*;

    fn stream_from_bytes<I, It>(
        items: I,
    ) -> FramedStream<BoxStream<'static, Result<Bytes, IoError>>>
    where
        I: IntoIterator<Item = It> + 'static + Send,
        It: AsRef<[u8]>,
    {
        let iter: Vec<_> = items
            .into_iter()
            .map(|x| Bytes::copy_from_slice(x.as_ref()))
            .map(Ok)
            .collect();
        let stream = stream::iter(iter).boxed();
        FramedStream::new(stream, Instrumentation::default())
    }

    fn stream_from_results<I, It>(
        items: I,
    ) -> FramedStream<BoxStream<'static, Result<Bytes, IoError>>>
    where
        I: IntoIterator<Item = Result<It, IoError>> + 'static + Send,
        It: AsRef<[u8]>,
    {
        let iter: Vec<_> = items
            .into_iter()
            .map(|x| x.map(|x| Bytes::copy_from_slice(x.as_ref())))
            .collect();
        let stream = stream::iter(iter).boxed();
        FramedStream::new(stream, Instrumentation::default())
    }

    async fn poll_all<St>(mut stream: FramedStream<St>) -> Result<Vec<NakadiFrame>, IoError>
    where
        St: Stream<Item = Result<Bytes, IoError>> + Unpin,
    {
        let mut collected = Vec::new();

        while let Some(r) = stream.try_next().await? {
            collected.push(r);
        }

        Ok(collected)
    }

    async fn poll_for_err<St>(
        mut stream: FramedStream<St>,
    ) -> Result<Vec<NakadiFrame>, Vec<NakadiFrame>>
    where
        St: Stream<Item = Result<Bytes, IoError>> + Unpin,
    {
        let mut collected = Vec::new();

        let mut is_err = false;
        while let Some(r) = stream.next().await {
            if let Ok(frame) = r {
                collected.push(frame);
            } else {
                is_err = true;
            }
        }

        if is_err {
            Err(collected)
        } else {
            Ok(collected)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_frames_empty_stream() {
        let input: Vec<&[u8]> = vec![];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert!(frames.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_frames_stream_of_one_input_bytes() {
        let input = vec![b""];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert!(frames.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_frames_stream_of_one_line_feed_bytes() {
        let input = vec![b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_frames_stream_of_multiple_line_feed_bytes_1() {
        let input = vec![b"\n\n\n\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_frames_stream_of_multiple_line_feed_bytes_2() {
        let input = vec![b"\n", b"\n", b"\n", b"\n", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_one_byte_1() {
        let input = vec![b"0\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_one_byte_2() {
        let input = vec![b"\n0\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_one_byte_3() {
        let input = vec![b"0", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_one_byte_4() {
        let input = vec![b"\n", b"0", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_one_byte_5() {
        let input = vec![b"\n0", b"\n\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_multiple_bytes_1() {
        let input = vec![b"012345\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_multiple_bytes_2() {
        let input = vec![b"0", b"1", b"2", b"3", b"4", b"5", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_multiple_bytes_3() {
        let input = vec![&b"0"[..], &b"1"[..], &b"234"[..], &b"5"[..], &b"\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_multiple_bytes_4() {
        let input = vec![&b"012"[..], &b"34"[..], &b"5"[..], &b"\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_multiple_bytes_5() {
        let input = vec![
            &b""[..],
            &b"012"[..],
            &b""[..],
            &b"34"[..],
            &b"5"[..],
            &b""[..],
            &b"\n"[..],
            &b""[..],
        ];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_frame_with_multiple_bytes_6() {
        let input = vec![b"\n012345\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_frames_with_multiple_bytes_1() {
        let input = vec![b"012345\nabc\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_frames_with_multiple_bytes_2() {
        let input = vec![&b"012345"[..], &b"\n"[..], &b"abc\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_frames_with_multiple_bytes_3() {
        let input = vec![
            &b"012345"[..],
            &b""[..],
            &b"\n"[..],
            &b""[..],
            &b"abc\n"[..],
        ];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_frames_with_multiple_bytes_4() {
        let input = vec![&b"012345"[..], &b"\n"[..], &b"abc"[..], &b"\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_frames_with_multiple_bytes_5() {
        let input = vec![
            &b""[..],
            &b"\n"[..],
            &b"012345"[..],
            &b""[..],
            &b""[..],
            &b"\nabc\n"[..],
        ];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn frame_not_finished_1() {
        let input = vec![b"012345"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn frame_not_finished_2() {
        let input = vec![b"012345\nabc"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn frame_not_finished_3() {
        let input = vec![b"012345\nabc\nxyz"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_1() {
        let input = vec![Err::<&[u8], _>(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_2() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"123\n"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        let frames = result.unwrap_err();
        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"123");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_3() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"123"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_4() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"123\nabc\n"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        let frames = result.unwrap_err();
        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"123");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_5() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"123\nabc"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        let frames = result.unwrap_err();
        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"123");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_6() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b""), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_7() {
        let input: Vec<Result<&[u8], IoError>> = vec![Err(IoError::new("x")), Ok(b"")];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_8() {
        let input: Vec<Result<&[u8], IoError>> = vec![Err(IoError::new("x")), Ok(b"123\nab\nc")];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_9() {
        let input: Vec<Result<&[u8], IoError>> = vec![Err(IoError::new("x")), Ok(b"123\nabc")];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn error_10() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"\n"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        let frames = result.unwrap_err();
        assert_eq!(frames.len(), 0);
    }

    fn frames_are_emitted_immediately_after_new_line() {
        let (send_bytes, receive_bytes) = tokio::sync::mpsc::unbounded_channel::<&'static [u8]>();
        let (send_frame, receive_frame) = crossbeam::channel::unbounded();

        let stream_f = FramedStream::new(
            UnboundedReceiverStream::new(receive_bytes).map(|v| Ok(v.into())),
            Instrumentation::default(),
        )
        .for_each(move |f| {
            if let Ok(f) = f {
                let _ = send_frame.send(f);
            }
            futures::future::ready(())
        });

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(stream_f);

        send_bytes.send(b"").unwrap();
        assert!(receive_frame.try_recv().is_err());
        send_bytes.send(b"a").unwrap();
        assert!(receive_frame.try_recv().is_err());
        send_bytes.send(b"\n").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"a");

        send_bytes.send(b"abc\n").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"abc");

        send_bytes.send(b"abc\ndef").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"abc");
        send_bytes.send(b"\n").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"def");

        send_bytes.send(b"abc\ndef\n").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"abc");
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"def");

        send_bytes.send(b"abc\ndef\nghi").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"abc");
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"def");
        send_bytes.send(b"\n").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"ghi");

        send_bytes.send(b"abc\ndef\nghi\n").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"abc");
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"def");
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"ghi");

        send_bytes.send(b"abc\nde").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"abc");
        send_bytes.send(b"f\nghi\n").unwrap();
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"def");
        assert_eq!(receive_frame.recv().unwrap().bytes.as_ref(), b"ghi");
    }
}
