pub trait BatchProcessor {
    fn more_batches_requested(&self) -> bool;
    fn process_batch(line: Vec<u8>);

    fn stop(&self);
}

pub mod lineparsing {
    const OBJ_OPEN: u8 = b'{';
    const OBJ_CLOSE: u8 = b'}';
    const ARRAY_OPEN: u8 = b'[';
    const ARRAY_CLOSE: u8 = b']';
    const DOUBLE_QUOTE: u8 = b'"';
    const ESCAPE: u8 = b'\\';

    const CURSOR_LABEL: &'static [u8] = b"cursor";
    const EVENTS_LABEL: &'static [u8] = b"events";
    const INFO_LABEL: &'static [u8] = b"info";

    #[derive(Debug, PartialEq, Eq)]
    pub struct LineItems {
        pub cursor: (usize, usize),
        pub events: Option<(usize, usize)>,
        pub info: Option<(usize, usize)>,
    }

    pub fn parse_line(json_bytes: &[u8]) -> Result<LineItems, String> {
        let mut line_items = LineItems {
            cursor: (0, 0),
            events: None,
            info: None,
        };

        let next = parse_next_item(json_bytes, 0, &mut line_items)?;
        let next = parse_next_item(json_bytes, next, &mut line_items)?;
        let _ = parse_next_item(json_bytes, next, &mut line_items)?;

        if line_items.cursor.1 == 0 {
            Err("No cursor".into())
        } else {
            Ok(line_items)
        }
    }

    fn parse_next_item(
        json_bytes: &[u8],
        start: usize,
        line_items: &mut LineItems,
    ) -> Result<usize, String> {
        if let Ok(Some((begin, end))) = next_string(json_bytes, start) {
            if end - begin < 3 {
                return Err("String can not be a label if len<3".into());
            }

            let label = &json_bytes[begin + 1..end];
            let last = match label {
                CURSOR_LABEL => {
                    let (a, b) = find_next_obj(json_bytes, end)?;
                    line_items.cursor = (a, b);
                    b
                }
                EVENTS_LABEL => {
                    let (a, b) = find_next_array(json_bytes, end)?;
                    line_items.events = Some((a, b));
                    b
                }
                INFO_LABEL => {
                    let (a, b) = find_next_obj(json_bytes, end)?;
                    line_items.info = Some((a, b));
                    b
                }
                _ => end,
            };
            Ok(last)
        } else {
            Err("No string found that could be the label for the next item.".into())
        }
    }


    fn next_string(json_bytes: &[u8], start: usize) -> Result<Option<(usize, usize)>, String> {
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
            return Err("Not a string. Missing starting `\"`.".into());
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
            return Err("Not a string. Missing ending `\"`.".into());
        }

        Ok(Some((idx_begin, idx_end)))
    }

    fn find_next_obj(json_bytes: &[u8], start: usize) -> Result<(usize, usize), String> {
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
            return Err("Not an object. Missing starting `{`.".into());
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
            return Err("Not an oject. Missing ending `}`.".into());
        }

        Ok((idx_begin, idx_end))
    }

    fn find_next_array(json_bytes: &[u8], start: usize) -> Result<(usize, usize), String> {
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
        while idx_end < json_bytes.len() {
            let c = json_bytes[idx_end];
            if c == DOUBLE_QUOTE {
                let (_, end) = next_string(json_bytes, idx_end)?.unwrap();
                idx_end = end + 1;
                continue;
            } else if c == ARRAY_OPEN {
                level += 1;
                idx_end += 1;
                continue;
            } else if c == ARRAY_CLOSE {
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
            return Err("Not an array. Missing ending `]`.".into());
        }

        Ok((idx_begin, idx_end))
    }

    #[test]
    fn test_next_string_1() {
        let sample = b"\"\"";
        let r = next_string(sample, 0).unwrap().unwrap();
        assert_eq!(r, (0, 1));
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
        assert_eq!(r, (8, 24));
    }

    #[test]
    fn test_find_next_array_2() {
        let sample = b"\"field\":[\"number\", 1, {}], more...";
        let r = find_next_array(sample, 7).unwrap();
        assert_eq!(r, (8, 24));
    }

    #[test]
    fn test_find_next_array_3() {
        let sample = b"\"field\":[\"number\", 1, {}], more...";
        let r = find_next_array(sample, 8).unwrap();
        assert_eq!(r, (8, 24));
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
    fn parse_line_1() {
        let sample = br#"{"cursor":{"partition":"5"},"events":[{"metadata":"blah"}],"info":{"debug":"Stream started"}}"#;
        let r = parse_line(sample).unwrap();

        let expected = LineItems {
            cursor: (10, 26),
            events: Some((37, 57)),
            info: Some((66, 91)),
        };

        assert_eq!(r, expected);
        assert_eq!(&sample[10..27], br#"{"partition":"5"}"#);
        assert_eq!(&sample[37..58], br#"[{"metadata":"blah"}]"#);
        assert_eq!(&sample[66..92], br#"{"debug":"Stream started"}"#);
    }

    #[test]
    fn parse_line_2() {
        let sample = br#"{"cursor":{"partition":"5"},"events":[{"metadata":"blah"}]"#;
        let r = parse_line(sample).unwrap();

        let expected = LineItems {
            cursor: (10, 26),
            events: Some((37, 57)),
            info: None,
        };

        assert_eq!(r, expected);
        assert_eq!(&sample[10..27], br#"{"partition":"5"}"#);
        assert_eq!(&sample[37..58], br#"[{"metadata":"blah"}]"#);
    }


    #[test]
    fn parse_line_3() {
        let sample = br#"{"cursor":{"partition":"5"}"#;
        let r = parse_line(sample).unwrap();

        let expected = LineItems {
            cursor: (10, 26),
            events: None,
            info: None,
        };

        assert_eq!(r, expected);
        assert_eq!(&sample[10..27], br#"{"partition":"5"}"#);
    }

    #[test]
    fn parse_line_4() {
        let sample = br#"{"cursor":{"partition":"5"},"info":{"debug":"Stream started"}}"#;
        let r = parse_line(sample).unwrap();

        let expected = LineItems {
            cursor: (10, 26),
            events: None,
            info: Some((35, 60)),
        };

        assert_eq!(r, expected);
        assert_eq!(&sample[10..27], br#"{"partition":"5"}"#);
        assert_eq!(&sample[35..60], br#"{"debug":"Stream started"}"#);
    }

}
