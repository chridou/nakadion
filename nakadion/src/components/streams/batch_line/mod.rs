use std::error::Error as StdError;
use std::fmt;
use std::pin::Pin;
use std::str;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::{ready, stream::Stream};
use pin_utils::{unsafe_pinned, unsafe_unpinned};
use serde::de::DeserializeOwned;
use serde_json;

use crate::Error;

mod line_parser;

use crate::api::IoError;
use crate::components::streams::NakadiFrame;
use crate::instrumentation::{Instrumentation, Instruments};
use crate::nakadi_types::model::subscription::EventTypePartition;

use line_parser::{parse_line, LineItems, ParseLineError};

/// A stream of analyzed Nakadi Frames
pub struct BatchLineStream<St>
where
    St: Stream<Item = Result<NakadiFrame, IoError>>,
{
    frame_stream: St,
    instrumentation: Instrumentation,
    is_source_done: bool,
}

impl<St> BatchLineStream<St>
where
    St: Stream<Item = Result<NakadiFrame, IoError>>,
{
    unsafe_pinned!(frame_stream: St);
    unsafe_unpinned!(is_source_done: bool);

    pub fn new(frame_stream: St, instrumentation: Instrumentation) -> Self {
        Self {
            frame_stream,
            is_source_done: false,
            instrumentation,
        }
    }
}

impl<St> Stream for BatchLineStream<St>
where
    St: Stream<Item = Result<NakadiFrame, IoError>>,
{
    type Item = Result<BatchLine, BatchLineError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.is_source_done {
            Poll::Ready(None)
        } else {
            let next_frame = ready!(self.as_mut().frame_stream().poll_next(cx));

            match next_frame {
                Some(Ok(frame)) => {
                    self.instrumentation
                        .stream_frame_received(frame.bytes.len());

                    match BatchLine::try_from_frame(frame) {
                        Ok(line) => Poll::Ready(Some(Ok(line))),
                        Err(err) => {
                            *self.as_mut().is_source_done() = true;
                            Poll::Ready(Some(Err(err.into())))
                        }
                    }
                }
                Some(Err(err)) => {
                    *self.as_mut().is_source_done() = true;
                    Poll::Ready(Some(Err(err.into())))
                }
                None => Poll::Ready(None),
            }
        }
    }
}

impl<St> From<St> for BatchLineStream<St>
where
    St: Stream<Item = Result<NakadiFrame, IoError>>,
{
    fn from(stream: St) -> Self {
        Self::new(stream, Instrumentation::default())
    }
}

/// An analyzed line (frame) from Nakadi
#[derive(Debug)]
pub struct BatchLine {
    bytes: Bytes,
    items: LineItems,
    frame_id: usize,
    received_at: Instant,
}

impl BatchLine {
    #[allow(dead_code)]
    pub fn new<T: Into<Bytes>>(bytes: T) -> Result<BatchLine, ParseLineError> {
        let bytes = bytes.into();

        let items = parse_line(bytes.as_ref())?;

        if let Err(err) = items.validate() {
            return Err(ParseLineError::new(format!("frame is invalid: {}", err)));
        }

        Ok(BatchLine {
            bytes,
            items,
            frame_id: 0,
            received_at: Instant::now(),
        })
    }

    #[allow(dead_code)]
    pub fn try_from_slice<T: AsRef<[u8]>>(slice: T) -> Result<BatchLine, ParseLineError> {
        let items = parse_line(slice.as_ref())?;

        if let Err(err) = items.validate() {
            return Err(ParseLineError::new(format!("frame  is invalid: {}", err)));
        }

        Ok(BatchLine {
            bytes: Bytes::copy_from_slice(slice.as_ref()),
            items,
            frame_id: 0,
            received_at: Instant::now(),
        })
    }

    #[allow(dead_code)]
    pub fn try_from_frame(frame: NakadiFrame) -> Result<BatchLine, ParseLineError> {
        let items = parse_line(frame.as_ref())?;

        if let Err(err) = items.validate() {
            return Err(ParseLineError::new(format!("frame is invalid: {}", err)));
        }

        Ok(BatchLine {
            bytes: frame.bytes,
            items,
            frame_id: frame.frame_id,
            received_at: frame.received_at,
        })
    }

    pub fn with_frame_id(mut self, frame_id: usize) -> Self {
        self.frame_id = frame_id;
        self
    }

    pub fn frame_id(&self) -> usize {
        self.frame_id
    }

    pub fn received_at(&self) -> Instant {
        self.received_at
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn cursor_str(&self) -> &str {
        self.items.cursor_str(self.bytes.as_ref())
    }

    pub fn cursor_bytes(&self) -> Bytes {
        self.items.cursor_bytes(&self.bytes)
    }

    pub fn partition_bytes(&self) -> Bytes {
        self.items.cursor().partition_bytes(&self.bytes)
    }

    pub fn partition_str(&self) -> &str {
        self.items.cursor().partition_str(self.bytes.as_ref())
    }

    pub fn event_type_bytes(&self) -> Bytes {
        self.items.cursor().event_type_bytes(&self.bytes)
    }

    pub fn event_type_str(&self) -> &str {
        self.items.cursor().event_type_str(self.bytes.as_ref())
    }

    pub fn to_event_type_partition(&self) -> EventTypePartition {
        EventTypePartition::new(self.event_type_str(), self.partition_str())
    }

    pub fn events_bytes(&self) -> Option<Bytes> {
        self.items.events_bytes(&self.bytes)
    }

    pub fn events_str(&self) -> Option<&str> {
        self.items.events_str(self.bytes.as_ref())
    }

    pub fn info_bytes(&self) -> Option<Bytes> {
        self.items.info_bytes(&self.bytes)
    }

    pub fn info_str(&self) -> Option<&str> {
        self.items.info_str(self.bytes.as_ref())
    }

    pub fn is_keep_alive_line(&self) -> bool {
        !self.items.has_events()
    }

    pub fn has_events(&self) -> bool {
        self.items.has_events()
    }

    pub fn has_info(&self) -> bool {
        self.items.has_info()
    }

    pub fn cursor_deserialized<T: DeserializeOwned>(&self) -> Result<T, Error> {
        Ok(serde_json::from_slice(self.cursor_bytes().as_ref())?)
    }
}

#[derive(Debug)]
pub struct BatchLineError {
    message: String,
    kind: BatchLineErrorKind,
}

impl BatchLineError {
    pub fn new<T: Into<String>>(message: T, kind: BatchLineErrorKind) -> Self {
        Self {
            message: message.into(),
            kind,
        }
    }

    pub fn kind(&self) -> BatchLineErrorKind {
        self.kind
    }
}

impl StdError for BatchLineError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl fmt::Display for BatchLineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            BatchLineErrorKind::Io => {
                write!(f, "io error - ")?;
            }
            BatchLineErrorKind::Parser => {
                write!(f, "parser error - ")?;
            }
        }
        write!(f, "{}", self.message)?;

        Ok(())
    }
}

impl From<IoError> for BatchLineError {
    fn from(err: IoError) -> Self {
        Self::new(err.to_string(), BatchLineErrorKind::Io)
    }
}

impl From<ParseLineError> for BatchLineError {
    fn from(err: ParseLineError) -> Self {
        Self::new(err.to_string(), BatchLineErrorKind::Parser)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchLineErrorKind {
    Io,
    Parser,
}

#[test]
fn parse_subscription_batch_line_with_info() {
    let line_sample = r#"{"cursor":{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"events":[{"metadata":"#
        + r#"{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7"#
        + r#"-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","#
        + r#""received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"#
        + r#""data_op":"C","data":{"order_number":"abc","id":"111"},"#
        + r#""data_type":"blah"}],"info":{"debug":"Stream started"}}"#;

    let cursor_sample = r#"{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}"#;

    let events_sample = r#"[{"metadata":"#.to_owned()
        + r#"{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7"#
        + r#"-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","#
        + r#""received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"#
        + r#""data_op":"C","data":{"order_number":"abc","id":"111"},"#
        + r#""data_type":"blah"}]"#;

    let info_sample = r#"{"debug":"Stream started"}"#;

    let line = BatchLine::try_from_slice(line_sample.as_bytes()).unwrap();

    assert_eq!(line.bytes(), line_sample.as_bytes());
    assert_eq!(line.cursor_bytes(), cursor_sample.as_bytes());
    assert_eq!(line.partition_str(), "6", "partition");
    assert_eq!(line.event_type_str(), "order.ORDER_RECEIVED");
    assert_eq!(line.events_str(), Some(events_sample.as_ref()));
    assert_eq!(line.info_str(), Some(&info_sample[..]));
    assert_eq!(line.is_keep_alive_line(), false);
}

#[test]
fn parse_subscription_batch_line_without_info() {
    let line_sample = r#"{"cursor":{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"events":[{"metadata":"#
        + r#"{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7"#
        + r#"-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","#
        + r#""received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"#
        + r#""data_op":"C","data":{"order_number":"abc","id":"111"},"#
        + r#""data_type":"blah"}]}"#;

    let cursor_sample = r#"{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}"#;

    let events_sample = r#"[{"metadata":"#.to_owned()
        + r#"{"occurred_at":"1996-10-15T16:39:57+07:00","eid":"1f5a76d8-db49-4144-ace7"#
        + r#"-e683e8ff4ba4","event_type":"aruha-test-hila","partition":"5","#
        + r#""received_at":"2016-09-30T09:19:00.525Z","flow_id":"blahbloh"},"#
        + r#""data_op":"C","data":{"order_number":"abc","id":"111"},"#
        + r#""data_type":"blah"}]"#;

    let line = BatchLine::try_from_slice(line_sample.as_bytes()).unwrap();

    assert_eq!(line.bytes(), line_sample.as_bytes());
    assert_eq!(line.cursor_bytes(), cursor_sample.as_bytes());
    assert_eq!(line.partition_str(), "6", "partition");
    assert_eq!(line.event_type_str(), "order.ORDER_RECEIVED");
    assert_eq!(line.events_str(), Some(events_sample.as_ref()));
    assert_eq!(line.info_bytes(), None);
    assert_eq!(line.is_keep_alive_line(), false);
}

#[test]
fn parse_subscription_batch_line_keep_alive_with_info() {
    let line_sample = r#"{"cursor":{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"},"info":{"debug":"Stream started"}}"#;

    let cursor_sample = r#"{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}"#;

    let info_sample = r#"{"debug":"Stream started"}"#;

    let line = BatchLine::try_from_slice(line_sample.as_bytes()).unwrap();

    assert_eq!(line.bytes(), line_sample.as_bytes());
    assert_eq!(line.cursor_bytes(), cursor_sample.as_bytes());
    assert_eq!(line.partition_str(), "6");
    assert_eq!(line.event_type_str(), "order.ORDER_RECEIVED");
    assert_eq!(line.info_str(), Some(info_sample));
    assert_eq!(line.is_keep_alive_line(), true);
}

#[test]
fn parse_subscription_batch_line_keep_alive_without_info() {
    let line_sample = r#"{"cursor":{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}}"#;

    let cursor_sample = r#"{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}"#;

    let line = BatchLine::try_from_slice(line_sample.as_bytes()).unwrap();

    assert_eq!(line.bytes(), line_sample.as_bytes(), "line bytes");
    assert_eq!(
        line.cursor_bytes(),
        cursor_sample.as_bytes(),
        "cursor bytes"
    );
    assert_eq!(line.partition_str(), "6");
    assert_eq!(line.event_type_str(), "order.ORDER_RECEIVED");
    assert_eq!(line.info_bytes(), None);
    assert_eq!(line.is_keep_alive_line(), true);
}

#[test]
fn deserialize_subscription_cursor() {
    use crate::nakadi_types::model::subscription::SubscriptionCursor;
    let line_sample = r#"{"cursor":{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}}"#;

    let line = BatchLine::try_from_slice(line_sample.as_bytes()).unwrap();

    let _ = line.cursor_deserialized::<SubscriptionCursor>().unwrap();
}
