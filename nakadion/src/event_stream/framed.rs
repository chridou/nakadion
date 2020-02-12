use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::{Buf as _, Bytes};
use futures::stream::Stream;
use pin_utils::{unsafe_pinned, unsafe_unpinned};

use crate::api::IoError;

#[derive(Clone)]
pub struct NakadiFrame {
    pub bytes: Bytes,
    pub received_at: Instant,
    pub frame_id: usize,
}

impl NakadiFrame {
    #[allow(dead_code)]
    pub fn new(bytes: Vec<u8>, received_at: Instant, frame_id: usize) -> Self {
        Self {
            bytes: bytes.into(),
            received_at,
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

    pub fn new(bytes_stream: St) -> Self {
        let now = Instant::now();
        Self {
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
}

impl<St> Stream for FramedStream<St>
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

                                if !state.unfinished_frame.is_empty() {
                                    let finished_frame = std::mem::replace(
                                        &mut state.unfinished_frame,
                                        Vec::with_capacity(4096),
                                    );

                                    state.frames.push_back(NakadiFrame {
                                        bytes: finished_frame.into(),
                                        received_at: state.first_byte_received_at,
                                        frame_id: state.frame_id,
                                    });

                                    state.frame_id += 1;
                                }
                            } else {
                                state.unfinished_frame.extend_from_slice(&bytes);
                                break;
                            }
                        }

                        if let Some(frame) = state.frames.pop_front() {
                            return Poll::Ready(Some(Ok(frame)));
                        }
                    }
                    Poll::Ready(None) => {
                        let unframed_bytes = self.state.unfinished_frame.len();
                        if unframed_bytes > 0 {
                            /* warn!(
                                "unexpected end of stream, {} unframed bytes left",
                                unframed_bytes
                            )*/
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
mod test {
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
        FramedStream::new(stream)
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
        FramedStream::new(stream)
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

    #[tokio::test(basic_scheduler)]
    async fn no_frames_empty_stream() {
        let input: Vec<&[u8]> = vec![];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert!(frames.is_empty());
    }

    #[tokio::test(basic_scheduler)]
    async fn no_frames_stream_of_one_input_bytes() {
        let input = vec![b""];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert!(frames.is_empty());
    }

    #[tokio::test(basic_scheduler)]
    async fn no_frames_stream_of_one_line_feed_bytes() {
        let input = vec![b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(basic_scheduler)]
    async fn no_frames_stream_of_multiple_line_feed_bytes_1() {
        let input = vec![b"\n\n\n\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(basic_scheduler)]
    async fn no_frames_stream_of_multiple_line_feed_bytes_2() {
        let input = vec![b"\n", b"\n", b"\n", b"\n", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_one_byte_1() {
        let input = vec![b"0\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_one_byte_2() {
        let input = vec![b"\n0\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_one_byte_3() {
        let input = vec![b"0", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_one_byte_4() {
        let input = vec![b"\n", b"0", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_one_byte_5() {
        let input = vec![b"\n0", b"\n\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"0");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_multiple_bytes_1() {
        let input = vec![b"012345\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_multiple_bytes_2() {
        let input = vec![b"0", b"1", b"2", b"3", b"4", b"5", b"\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_multiple_bytes_3() {
        let input = vec![&b"0"[..], &b"1"[..], &b"234"[..], &b"5"[..], &b"\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_multiple_bytes_4() {
        let input = vec![&b"012"[..], &b"34"[..], &b"5"[..], &b"\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(basic_scheduler)]
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

    #[tokio::test(basic_scheduler)]
    async fn one_frame_with_multiple_bytes_6() {
        let input = vec![b"\n012345\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(basic_scheduler)]
    async fn two_frames_with_multiple_bytes_1() {
        let input = vec![b"012345\nabc\n"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(basic_scheduler)]
    async fn two_frames_with_multiple_bytes_2() {
        let input = vec![&b"012345"[..], &b"\n"[..], &b"abc\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(basic_scheduler)]
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

    #[tokio::test(basic_scheduler)]
    async fn two_frames_with_multiple_bytes_4() {
        let input = vec![&b"012345"[..], &b"\n"[..], &b"abc"[..], &b"\n"[..]];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(basic_scheduler)]
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

    #[tokio::test(basic_scheduler)]
    async fn frame_not_finished_1() {
        let input = vec![b"012345"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 0);
    }

    #[tokio::test(basic_scheduler)]
    async fn frame_not_finished_2() {
        let input = vec![b"012345\nabc"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"012345");
    }

    #[tokio::test(basic_scheduler)]
    async fn frame_not_finished_3() {
        let input = vec![b"012345\nabc\nxyz"];
        let stream = stream_from_bytes(input);
        let frames = poll_all(stream).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].as_bytes(), b"012345");
        assert_eq!(&frames[1].as_bytes(), b"abc");
    }

    #[tokio::test(basic_scheduler)]
    async fn error_1() {
        let input = vec![Err::<&[u8], _>(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(basic_scheduler)]
    async fn error_2() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"123\n"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        let frames = result.unwrap_err();
        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"123");
    }

    #[tokio::test(basic_scheduler)]
    async fn error_3() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"123"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(basic_scheduler)]
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

    #[tokio::test(basic_scheduler)]
    async fn error_5() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b"123\nabc"), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        let frames = result.unwrap_err();
        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].as_bytes(), b"123");
    }

    #[tokio::test(basic_scheduler)]
    async fn error_6() {
        let input: Vec<Result<&[u8], IoError>> = vec![Ok(b""), Err(IoError::new("x"))];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(basic_scheduler)]
    async fn error_7() {
        let input: Vec<Result<&[u8], IoError>> = vec![Err(IoError::new("x")), Ok(b"")];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(basic_scheduler)]
    async fn error_8() {
        let input: Vec<Result<&[u8], IoError>> = vec![Err(IoError::new("x")), Ok(b"123\nab\nc")];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }

    #[tokio::test(basic_scheduler)]
    async fn error_9() {
        let input: Vec<Result<&[u8], IoError>> = vec![Err(IoError::new("x")), Ok(b"123\nabc")];
        let stream = stream_from_results(input);
        let result = poll_for_err(stream).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_empty());
    }
}
