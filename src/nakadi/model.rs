/// A `StreamId` identifies a subscription. It must be provided for checkpointing with
/// a `Cursor`.
#[derive(Clone, Debug)]
pub struct StreamId(pub String);

impl StreamId {
    pub fn new<T: Into<String>>(id: T) -> Self {
        StreamId(id.into())
    }
}

/// Information on a current batch. This might be
/// useful for a `Handler` that wants to do checkpointing on its own.
#[derive(Clone, Debug)]
pub struct BatchCommitData<'a> {
    pub stream_id: StreamId,
    pub cursor: &'a [u8],
}

/// The [`Nakadi Event Type`](https://github.com/zalando/nakadi#creating-event-types).
/// Similiar to a topic.
#[derive(Clone, Debug)]
pub struct EventType<'a>(pub &'a str);

impl<'a> EventType<'a> {
    /// Creates a new instance of an
    /// [`EventType`](https://github.com/zalando/nakadi#creating-event-types).
    pub fn new(value: &'a str) -> EventType {
        EventType(value)
    }
}

/// A partition id that comes with a `Cursor`
#[derive(Clone, Debug)]
pub struct PartitionId<'a>(pub &'a [u8]);

pub mod lineparsing {
    const OBJ_OPEN: u8 = b'{';
    const OBJ_CLOSE: u8 = b'}';
    const ARRRAY_OPEN: u8 = b'[';
    const ARRAY_CLOSE: u8 = b']';
    const DOUBLE_QUOTE: u8 = b'"';
    const ESCAPE: u8 = b'\\';

    fn shrink_to_obj(json_bytes: &[u8], begin: usize, end: usize) -> Result<(usize, usize), String> {
        let mut idx_begin = begin;
        while idx_begin < end {
            if json_bytes[idx_begin] == OBJ_OPEN {
                break;
            }
            idx_begin += 1;
        };

        if end <= idx_begin {
            return Err("No JSON object. Opening brace not found or last char.".into())
        }

        let mut idx_end = end;
        while idx_end > idx_begin {
           if json_bytes[idx_end] == OBJ_CLOSE {
                break;
            }
            idx_end -= 1;
        }

        if idx_end == idx_begin {
            return Err("No JSON object. No closing brace after opening brace.".into())
        }

        Ok((idx_begin, idx_end))
    }

    fn find_obj(json_bytes: &[u8]) -> Result<(usize, usize), String> {
        unimplemented!()
    }

    fn skip_string(json_bytes: &[u8], start: usize) -> Result<(usize, usize), String> {
        unimplemented!()
    }

    #[test]
    fn test_trim_shrink_to_obj_fail_1() {
        let sample = b"";
        let r = shrink_to_obj(sample, 0, 0);
        assert!(r.is_err());
    } 

    #[test]
    fn test_trim_shrink_to_obj_fail_2() {
        let sample = b" ";
        let r = shrink_to_obj(sample, 0, 0);
        assert!(r.is_err());
    }

    #[test]
    fn test_trim_shrink_to_obj_fail_3() {
        let sample = b"  ";
        let r = shrink_to_obj(sample, 0, 1);
        assert!(r.is_err());
    } 
     
    #[test]
    fn test_trim_shrink_to_obj_fail_4() {
        let sample = b"}{";
        let r = shrink_to_obj(sample, 0, 1);
        assert!(r.is_err());
    } 

    #[test]
    fn test_trim_shrink_to_obj_fail_5() {
        let sample = b" }";
        let r = shrink_to_obj(sample, 0, 1);
        assert!(r.is_err());
    } 

    #[test]
    fn test_trim_shrink_to_obj_fail_6() {
        let sample = b"{ ";
        let r = shrink_to_obj(sample, 0, 1);
        assert!(r.is_err());
    } 
    
    #[test]
    fn test_trim_shrink_to_obj_fail_7() {
        let sample = b"{";
        let r = shrink_to_obj(sample, 0, 0);
        assert!(r.is_err());
    } 
    
    #[test]
    fn test_trim_shrink_to_obj_fail_8() {
        let sample = b"}";
        let r = shrink_to_obj(sample, 0, 0);
        assert!(r.is_err());
    } 

    #[test]
    fn test_trim_shrink_to_obj_1() {
        let sample = b"{}";
        let r = shrink_to_obj(sample, 0, 1).unwrap();
        assert_eq!(r, (0,1));
    } 
   
    #[test]
    fn test_trim_shrink_to_obj_2() {
        let sample = b"{ }";
        let r = shrink_to_obj(sample, 0, 2).unwrap();
        assert_eq!(r, (0,2));
    } 

    #[test]
    fn test_trim_shrink_to_obj_3() {
        let sample = b"aa{ }aa";
        let r = shrink_to_obj(sample, 0, 6).unwrap();
        assert_eq!(r, (2,4));
    } 
   
    #[test]
    fn test_trim_shrink_to_obj_4() {
        let sample = b"aa{}aa";
        let r = shrink_to_obj(sample, 0, 5).unwrap();
        assert_eq!(r, (2,3));
    } 

    #[test]
    fn test_trim_shrink_to_obj_5() {
        let sample = b"aa{{{}}{}}aa";
        let r = shrink_to_obj(sample, 0, 11).unwrap();
        assert_eq!(r, (2,9));
    } 
   
}