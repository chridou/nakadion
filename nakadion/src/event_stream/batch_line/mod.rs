use std::str;

use bytes::Bytes;

mod line_parser;

use line_parser::{parse_line, LineItems, ParseLineError};

#[derive(Debug, PartialEq, Eq)]
pub struct BatchLine {
    bytes: Bytes,
    items: LineItems,
}

impl BatchLine {
    pub fn new<T: Into<Bytes>>(bytes: T) -> Result<BatchLine, ParseLineError> {
        let bytes = bytes.into();

        let items = parse_line(bytes.as_ref())?;

        if let Err(err) = items.validate() {
            return Err(ParseLineError::new(format!("line is invalid: {}", err)));
        }

        Ok(BatchLine { bytes, items })
    }

    pub fn from_slice<T: AsRef<[u8]>>(slice: T) -> Result<BatchLine, ParseLineError> {
        let items = parse_line(slice.as_ref())?;

        if let Err(err) = items.validate() {
            return Err(ParseLineError::new(format!("line is invalid: {}", err)));
        }

        Ok(BatchLine {
            bytes: Bytes::copy_from_slice(slice.as_ref()),
            items,
        })
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

    let line = BatchLine::from_slice(line_sample.as_bytes()).unwrap();

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

    let line = BatchLine::from_slice(line_sample.as_bytes()).unwrap();

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

    let line = BatchLine::from_slice(line_sample.as_bytes()).unwrap();

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

    let line = BatchLine::from_slice(line_sample.as_bytes()).unwrap();

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
