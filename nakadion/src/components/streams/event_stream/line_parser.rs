use bytes::Bytes;
use std::error::Error as StdError;
use std::fmt;

const OBJ_OPEN: u8 = b'{';
const OBJ_CLOSE: u8 = b'}';
const ARRAY_OPEN: u8 = b'[';
const ARRAY_CLOSE: u8 = b']';
const DOUBLE_QUOTE: u8 = b'"';
const ESCAPE: u8 = b'\\';
const COMMA: u8 = b',';

const CURSOR_LABEL: &[u8] = b"cursor";
const EVENTS_LABEL: &[u8] = b"events";
const INFO_LABEL: &[u8] = b"info";

const CURSOR_PARTITION_LABEL: &[u8] = b"partition";
const CURSOR_EVENT_TYPE_LABEL: &[u8] = b"event_type";
const CURSOR_OFFSET_LABEL: &[u8] = b"offset";
const CURSOR_TOKEN_LABEL: &[u8] = b"cursor_token";

#[derive(Debug, PartialEq, Eq)]
pub struct LineItems {
    cursor: Cursor,
    events: Option<(usize, usize)>,
    num_events: usize,
    info: Option<(usize, usize)>,
}

impl LineItems {
    pub fn validate(&self) -> Result<(), String> {
        self.cursor.validate()?;

        if let Some((a, b)) = self.events {
            if a >= b {
                return Err(format!("events: ({},{})", a, b));
            }
        }

        if let Some((a, b)) = self.info {
            if a >= b {
                return Err(format!("events: ({},{})", a, b));
            }
        }
        Ok(())
    }

    pub fn cursor_bytes(&self, bytes: &Bytes) -> Bytes {
        pos_to_bytes(self.cursor.line_position, bytes)
    }

    #[allow(dead_code)]
    pub fn cursor_str<'a>(&self, bytes: &'a [u8]) -> &'a str {
        pos_to_str(self.cursor.line_position, bytes)
    }

    pub fn events_bytes(&self, bytes: &Bytes) -> Option<Bytes> {
        self.events.map(|pos| pos_to_bytes(pos, bytes))
    }

    #[allow(dead_code)]
    pub fn events_str<'a>(&self, bytes: &'a [u8]) -> Option<&'a str> {
        self.events.map(|pos| pos_to_str(pos, bytes))
    }

    #[allow(dead_code)]
    pub fn info_bytes(&self, bytes: &Bytes) -> Option<Bytes> {
        self.info.map(|pos| pos_to_bytes(pos, bytes))
    }

    pub fn info_str<'a>(&self, bytes: &'a [u8]) -> Option<&'a str> {
        self.info.map(|pos| pos_to_str(pos, bytes))
    }

    #[allow(dead_code)]
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    pub fn has_events(&self) -> bool {
        self.events.is_some()
    }

    pub fn num_events(&self) -> usize {
        self.num_events
    }

    #[allow(dead_code)]
    pub fn has_info(&self) -> bool {
        self.info.is_some()
    }
}

impl Default for LineItems {
    fn default() -> LineItems {
        LineItems {
            cursor: Default::default(),
            events: None,
            num_events: 0,
            info: None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Cursor {
    line_position: (usize, usize),
    partition: (usize, usize),
    event_type: (usize, usize),
    offset: (usize, usize),
    cursor_token: (usize, usize),
}

impl Cursor {
    pub fn validate(&self) -> Result<(), String> {
        let (a, b) = self.line_position;
        if a >= b {
            return Err(format!("line_position: ({},{})", a, b));
        }

        let (a, b) = self.partition;
        if a >= b {
            return Err(format!("partition: ({},{})", a, b));
        }

        let (a, b) = self.event_type;
        if a >= b {
            return Err(format!("event_type: ({},{})", a, b));
        }

        let (a, b) = self.offset;
        if a >= b {
            return Err(format!("offset: ({},{})", a, b));
        }

        let (a, b) = self.cursor_token;
        if a >= b {
            return Err(format!("cursor_token: ({},{})", a, b));
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn self_bytes(&self, bytes: &Bytes) -> Bytes {
        pos_to_bytes(self.line_position, bytes)
    }

    #[allow(dead_code)]
    pub fn self_str<'a>(&self, bytes: &'a [u8]) -> &'a str {
        pos_to_str(self.line_position, bytes)
    }

    #[allow(dead_code)]
    pub fn partition_bytes(&self, bytes: &Bytes) -> Bytes {
        pos_to_bytes(self.partition, bytes)
    }

    #[allow(dead_code)]
    pub fn partition_str<'a>(&self, bytes: &'a [u8]) -> &'a str {
        pos_to_str(self.partition, bytes)
    }

    #[allow(dead_code)]
    pub fn event_type_bytes(&self, bytes: &Bytes) -> Bytes {
        pos_to_bytes(self.event_type, bytes)
    }

    #[allow(dead_code)]
    pub fn event_type_str<'a>(&self, bytes: &'a [u8]) -> &'a str {
        pos_to_str(self.event_type, bytes)
    }

    #[allow(dead_code)]
    pub fn offset_bytes(&self, bytes: &Bytes) -> Bytes {
        pos_to_bytes(self.offset, bytes)
    }

    #[allow(dead_code)]
    pub fn offset_str<'a>(&self, bytes: &'a [u8]) -> &'a str {
        pos_to_str(self.offset, bytes)
    }

    #[allow(dead_code)]
    pub fn cursor_token_bytes(&self, bytes: &Bytes) -> Bytes {
        pos_to_bytes(self.cursor_token, bytes)
    }

    #[allow(dead_code)]
    pub fn cursor_token_str<'a>(&self, bytes: &'a [u8]) -> &'a str {
        pos_to_str(self.cursor_token, bytes)
    }
}

fn pos_to_str(pos: (usize, usize), bytes: &[u8]) -> &str {
    let (a, b) = pos;
    let slice = &bytes[a..b];
    unsafe { std::str::from_utf8_unchecked(slice) }
}

fn pos_to_bytes(pos: (usize, usize), bytes: &Bytes) -> Bytes {
    let (a, b) = pos;
    assert!(a < b, "invalid line parse indexes");
    bytes.slice(a..b)
}

impl Default for Cursor {
    fn default() -> Cursor {
        Cursor {
            line_position: (0, 0),
            partition: (0, 0),
            event_type: (0, 0),
            offset: (0, 0),
            cursor_token: (0, 0),
        }
    }
}

#[derive(Debug)]
pub struct ParseBatchError(String);

impl ParseBatchError {
    pub fn new<T: Into<String>>(t: T) -> Self {
        Self(t.into())
    }
}

impl<T> From<T> for ParseBatchError
where
    T: Into<String>,
{
    fn from(v: T) -> Self {
        ParseBatchError(v.into())
    }
}

impl StdError for ParseBatchError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl fmt::Display for ParseBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

pub fn parse_line<T: AsRef<[u8]>>(json_bytes: T) -> Result<LineItems, ParseBatchError> {
    let mut line_items = LineItems::default();
    let json_bytes = json_bytes.as_ref();

    let mut next_byte = 0;
    while next_byte < json_bytes.len() {
        if let Some(end) = parse_next_item(json_bytes, next_byte, &mut line_items)? {
            next_byte = end + 1;
        } else {
            break;
        }
    }

    if line_items.cursor.line_position.1 == 0 {
        Err("No Cursor".into())
    } else {
        Ok(line_items)
    }
}

fn parse_next_item(
    json_bytes: &[u8],
    start: usize,
    line_items: &mut LineItems,
) -> Result<Option<usize>, ParseBatchError> {
    if let Ok(Some((begin, end))) = next_string(json_bytes, start) {
        if end - begin < 3 {
            return Err("String can not be a label if len < 3".into());
        }

        let label = &json_bytes[begin + 1..end];
        let last = match label {
            CURSOR_LABEL => {
                let (a, b) = find_next_obj(json_bytes, end)?;
                line_items.cursor.line_position = (a, b + 1);
                parse_cursor_fields(json_bytes, &mut line_items.cursor, a, b)?;
                b
            }
            EVENTS_LABEL => {
                let (a, b, num_events) = find_next_array(json_bytes, end)?;
                line_items.events = Some((a, b + 1));
                line_items.num_events = num_events;
                b
            }
            INFO_LABEL => {
                let (a, b) = find_next_obj(json_bytes, end)?;
                line_items.info = Some((a, b + 1));
                b
            }
            _ => end,
        };
        Ok(Some(last))
    } else {
        Ok(None)
    }
}

fn next_string(json_bytes: &[u8], start: usize) -> Result<Option<(usize, usize)>, ParseBatchError> {
    if start == json_bytes.len() {
        return Ok(None);
    }

    let mut idx_begin = start;
    while idx_begin < json_bytes.len() {
        if json_bytes[idx_begin] == DOUBLE_QUOTE {
            break;
        }
        idx_begin += 1;
    }

    if idx_begin == json_bytes.len() {
        return Ok(None);
    }

    if idx_begin >= json_bytes.len() - 1 {
        return Err(format!("Not a string. Missing starting `\"` after pos {}", start).into());
    }

    let mut idx_end = idx_begin + 1;
    let mut escaping = false;
    while idx_end < json_bytes.len() {
        let c = json_bytes[idx_end];
        if c == ESCAPE {
            escaping = true;
            idx_end += 1;
            continue;
        } else if escaping {
            idx_end += 1;
            escaping = false;
            continue;
        } else if c == DOUBLE_QUOTE {
            break;
        } else {
            idx_end += 1;
        }
    }

    if idx_end == json_bytes.len() {
        let start_seq = ::std::str::from_utf8(&json_bytes[start..idx_end])
            .map_err(|err| ParseBatchError::from(format!("Not UTF-8: {}", err)))?;
        return Err(format!(
            "Not a string. Missing ending `\"` after pos {} but before {} in {}",
            start, idx_end, start_seq
        )
        .into());
    }

    Ok(Some((idx_begin, idx_end)))
}

fn find_next_obj(json_bytes: &[u8], start: usize) -> Result<(usize, usize), ParseBatchError> {
    if start == json_bytes.len() {
        return Err("Reached end".into());
    }

    let mut idx_begin = start;
    while idx_begin < json_bytes.len() {
        if json_bytes[idx_begin] == OBJ_OPEN {
            break;
        }
        idx_begin += 1;
    }

    if idx_begin >= json_bytes.len() - 1 {
        return Err("Not an object. Missing starting `{{`.".into());
    }

    let mut idx_end = idx_begin + 1;
    let mut level = 0;
    while idx_end < json_bytes.len() {
        let c = json_bytes[idx_end];
        if c == DOUBLE_QUOTE {
            let (_, end) = next_string(json_bytes, idx_end)?.unwrap();
            idx_end = end + 1;
            continue;
        } else if c == OBJ_OPEN {
            level += 1;
            idx_end += 1;
            continue;
        } else if c == OBJ_CLOSE {
            if level == 0 {
                break;
            } else {
                level -= 1;
                idx_end += 1;
                continue;
            }
        } else {
            idx_end += 1;
        }
    }

    if idx_end == json_bytes.len() {
        return Err("Not an object. Missing ending `}}`.".into());
    }

    Ok((idx_begin, idx_end))
}

/// returns (begin idx, end_idx, num elements)
fn find_next_array(
    json_bytes: &[u8],
    start: usize,
) -> Result<(usize, usize, usize), ParseBatchError> {
    if start == json_bytes.len() {
        return Err("Reached end".into());
    }

    let mut idx_begin = start;
    while idx_begin < json_bytes.len() {
        if json_bytes[idx_begin] == ARRAY_OPEN {
            break;
        }
        idx_begin += 1;
    }

    if idx_begin >= json_bytes.len() - 1 {
        return Err("Not an array. Missing starting `[`.".into());
    }

    let mut idx_end = idx_begin + 1;
    let mut level = 0;
    let mut num_commas_found = 0;
    let mut something_found = 0;
    while idx_end < json_bytes.len() {
        let c = json_bytes[idx_end];
        if c == DOUBLE_QUOTE {
            if level == 0 {
                something_found = 1;
            }
            let (_, end) = next_string(json_bytes, idx_end)?.unwrap();
            idx_end = end + 1;
        } else if c == ARRAY_OPEN {
            if level == 0 {
                something_found = 1;
            }
            level += 1;
            idx_end += 1;
        } else if c == ARRAY_CLOSE {
            if level == 0 {
                break;
            } else {
                level -= 1;
                idx_end += 1;
            }
        } else if c == OBJ_OPEN {
            if level == 0 {
                something_found = 1;
            }
            level += 1;
            idx_end += 1;
        } else if c == OBJ_CLOSE {
            level -= 1;
            idx_end += 1;
        } else if c == COMMA {
            idx_end += 1;
            if level == 0 {
                num_commas_found += 1;
            }
        } else {
            if level == 0 && !is_whitespace(c) {
                something_found = 1;
            }
            idx_end += 1;
        }
    }

    if idx_end == json_bytes.len() {
        return Err("Not an array. Missing ending `]`.".into());
    }

    Ok((idx_begin, idx_end, num_commas_found + something_found))
}

#[inline]
fn is_whitespace(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == b'\n' || b == b'\r'
}

fn parse_cursor_fields<T: AsRef<[u8]>>(
    json_bytes: T,
    cursor: &mut Cursor,
    start: usize,
    end: usize,
) -> Result<(), ParseBatchError> {
    let mut next_byte = start;
    while next_byte <= end {
        if let Some(end) = parse_next_cursor_item(json_bytes.as_ref(), next_byte, cursor)? {
            next_byte = end + 1
        } else {
            break;
        }
    }
    if cursor.partition.0 == 0 {
        Err(format!("Partition missing in cursor @ {}", next_byte).into())
    } else {
        Ok(())
    }
}

fn parse_next_cursor_item(
    json_bytes: &[u8],
    start: usize,
    cursor: &mut Cursor,
) -> Result<Option<usize>, ParseBatchError> {
    if let Ok(Some((begin, end))) = next_string(json_bytes, start) {
        if end - begin < 2 {
            return Err("String can not be a label if len<2".into());
        }

        let label = &json_bytes[begin + 1..end];
        let last = match label {
            CURSOR_PARTITION_LABEL => {
                if let Some((a, b)) = next_string(json_bytes, end + 1)? {
                    if b - a < 2 {
                        return Err("Empty String for partition".into());
                    } else {
                        cursor.partition = (a + 1, b);
                        b
                    }
                } else {
                    return Err("No String for partition".into());
                }
            }
            CURSOR_EVENT_TYPE_LABEL => {
                if let Some((a, b)) = next_string(json_bytes, end + 1)? {
                    if b - a < 2 {
                        return Err("Empty String for event_type".into());
                    } else {
                        cursor.event_type = (a + 1, b);
                        b
                    }
                } else {
                    return Err("No String for event_type".into());
                }
            }
            CURSOR_OFFSET_LABEL => {
                if let Some((a, b)) = next_string(json_bytes, end + 1)? {
                    if b - a < 2 {
                        return Err("Empty String for offset".into());
                    } else {
                        cursor.offset = (a + 1, b);
                        b
                    }
                } else {
                    return Err("No String for offset".into());
                }
            }
            CURSOR_TOKEN_LABEL => {
                if let Some((a, b)) = next_string(json_bytes, end + 1)? {
                    if b - a < 2 {
                        return Err("Empty String for cursor token".into());
                    } else {
                        cursor.cursor_token = (a + 1, b);
                        b
                    }
                } else {
                    return Err("No String for cursor token".into());
                }
            }
            _ => end,
        };
        Ok(Some(last))
    } else {
        Ok(None)
    }
}

#[test]
fn test_next_string_1() {
    let sample = b"\"\"";
    let r = next_string(sample, 0).unwrap().unwrap();
    assert_eq!(r, (0, 1));
}

#[test]
fn test_next_string_len1() {
    let sample = b"\"a\"";
    let r = next_string(sample, 0).unwrap().unwrap();
    assert_eq!(r, (0, 2));
}

#[test]
fn test_next_string_2() {
    let sample = b"xxx\"\"yyy";
    let r = next_string(sample, 0).unwrap().unwrap();
    assert_eq!(r, (3, 4));
}

#[test]
fn test_next_string_3() {
    let sample = b"xxx\"abc\"yyy";
    let r = next_string(sample, 0).unwrap().unwrap();
    assert_eq!(r, (3, 7));
}

#[test]
fn test_next_string_4() {
    let sample = b"xxx\"a\\\"bc\"yyy";
    let r = next_string(sample, 0).unwrap().unwrap();
    assert_eq!(r, (3, 9));
}

#[test]
fn test_next_string_5() {
    let sample = b"xxx\"a\\\nb\\\"c\"yyy";
    let r = next_string(sample, 0).unwrap().unwrap();
    assert_eq!(r, (3, 11));
}

#[test]
fn test_next_string_none_1() {
    let sample = b"";
    let r = next_string(sample, 0).unwrap();
    assert!(r.is_none());
}

#[test]
fn test_next_string_fail_2() {
    let sample = b"\"hallo";
    let r = next_string(sample, 0);
    assert!(r.is_err());
}

#[test]
fn test_next_string_fail_3() {
    let sample = b"hallo\"";
    let r = next_string(sample, 0);
    assert!(r.is_err());
}

#[test]
fn test_find_next_obj_1() {
    let sample = b"\"field\":{\"number\": 1}, more...";
    let r = find_next_obj(sample, 0).unwrap();
    assert_eq!(r, (8, 20));
}

#[test]
fn test_find_next_obj_2() {
    let sample = b"\"field\":{\"number\": 1}, more...";
    let r = find_next_obj(sample, 7).unwrap();
    assert_eq!(r, (8, 20));
}

#[test]
fn test_find_next_obj_3() {
    let sample = b"\"field\":{\"number\": 1}, more...";
    let r = find_next_obj(sample, 8).unwrap();
    assert_eq!(r, (8, 20));
}

#[test]
fn test_find_next_obj_fail_1() {
    let sample = b"\"field\":{\"number\": 1}, more...";
    let r = find_next_obj(sample, 9);
    assert!(r.is_err());
}

#[test]
fn test_find_next_obj_fail_2() {
    let sample = b"\"field\":{\"number\": 1}, more...";
    let r = find_next_obj(sample, 12);
    assert!(r.is_err());
}

#[test]
fn test_find_next_array_1() {
    let sample = b"\"field\":[\"number\", 1, {}], more...";
    let r = find_next_array(sample, 0).unwrap();
    assert_eq!(r, (8, 24, 3));
}

#[test]
fn test_find_next_array_2() {
    let sample = b"\"field\":[\"number\", 1, {}], more...";
    let r = find_next_array(sample, 7).unwrap();
    assert_eq!(r, (8, 24, 3));
}

#[test]
fn test_find_next_array_3() {
    let sample = b"\"field\":[\"number\", 1, {}], more...";
    let r = find_next_array(sample, 8).unwrap();
    assert_eq!(r, (8, 24, 3));
}

#[test]
fn test_find_next_array_fail_1() {
    let sample = b"\"field\":[\"number\", 1, {}], more...";
    let r = find_next_array(sample, 9);
    assert!(r.is_err());
}

#[test]
fn test_find_next_array_fail_2() {
    let sample = b"\"field\":[\"number\", 1, {}], more...";
    let r = find_next_array(sample, 12);
    assert!(r.is_err());
}

#[test]
fn parse_cursor() {
    let cursor_sample = r#"{"partition":"6","offset":"543","#.to_owned()
        + r#""event_type":"order.ORDER_RECEIVED","cursor_token":"#
        + r#""b75c3102-98a4-4385-a5fd-b96f1d7872f2"}"#;

    let mut cursor: Cursor = Default::default();

    let cursor_sample = cursor_sample.as_str();
    parse_cursor_fields(cursor_sample, &mut cursor, 0, cursor_sample.len()).unwrap();

    assert_eq!(cursor.partition_str(cursor_sample.as_ref()), "6");
    assert_eq!(cursor.offset_str(cursor_sample.as_ref()), "543");
    assert_eq!(
        cursor.event_type_str(cursor_sample.as_ref()),
        "order.ORDER_RECEIVED"
    );
    assert_eq!(
        cursor.cursor_token_str(cursor_sample.as_ref()),
        "b75c3102-98a4-4385-a5fd-b96f1d7872f2"
    );
}
