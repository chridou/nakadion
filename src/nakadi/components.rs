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


    fn find_obj_content(
        json_bytes: &[u8],
        begin: usize,
        end: usize,
    ) -> Result<(usize, usize), String> {
        let (start, end) = find_obj_bounds(json_bytes, begin, end)?;

        if end - start < 5 {
            // {"":X}
            Err("JSON obj is empty".into())
        } else {
            Ok((start + 1, end - 1))
        }
    }

    /// Tries to find the outer braces of a json obj given
    /// that begin and end are outside(or on) these boundaries
    /// while begin < end
    fn find_obj_bounds(
        json_bytes: &[u8],
        begin: usize,
        end: usize,
    ) -> Result<(usize, usize), String> {
        let mut idx_begin = begin;
        while idx_begin < end {
            if json_bytes[idx_begin] == OBJ_OPEN {
                break;
            }
            idx_begin += 1;
        }

        if end <= idx_begin {
            return Err(
                "No JSON object. Opening brace not found or last char.".into(),
            );
        }

        let mut idx_end = end;
        while idx_end > idx_begin {
            if json_bytes[idx_end] == OBJ_CLOSE {
                break;
            }
            idx_end -= 1;
        }

        if idx_end == idx_begin {
            return Err(
                "No JSON object. No closing brace after opening brace.".into(),
            );
        }

        Ok((idx_begin, idx_end))
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
    fn test_find_obj_bounds_fail_1() {
        let sample = b"";
        let r = find_obj_bounds(sample, 0, 0);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_fail_2() {
        let sample = b" ";
        let r = find_obj_bounds(sample, 0, 0);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_fail_3() {
        let sample = b"  ";
        let r = find_obj_bounds(sample, 0, 1);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_fail_4() {
        let sample = b"}{";
        let r = find_obj_bounds(sample, 0, 1);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_fail_5() {
        let sample = b" }";
        let r = find_obj_bounds(sample, 0, 1);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_fail_6() {
        let sample = b"{ ";
        let r = find_obj_bounds(sample, 0, 1);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_fail_7() {
        let sample = b"{";
        let r = find_obj_bounds(sample, 0, 0);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_fail_8() {
        let sample = b"}";
        let r = find_obj_bounds(sample, 0, 0);
        assert!(r.is_err());
    }

    #[test]
    fn test_find_obj_bounds_1() {
        let sample = b"{}";
        let r = find_obj_bounds(sample, 0, 1).unwrap();
        assert_eq!(r, (0, 1));
    }

    #[test]
    fn test_find_obj_bounds_2() {
        let sample = b"{ }";
        let r = find_obj_bounds(sample, 0, 2).unwrap();
        assert_eq!(r, (0, 2));
    }

    #[test]
    fn test_find_obj_bounds_3() {
        let sample = b"aa{ }aa";
        let r = find_obj_bounds(sample, 0, 6).unwrap();
        assert_eq!(r, (2, 4));
    }

    #[test]
    fn test_find_obj_bounds_4() {
        let sample = b"aa{}aa";
        let r = find_obj_bounds(sample, 0, 5).unwrap();
        assert_eq!(r, (2, 3));
    }

    #[test]
    fn test_find_obj_bounds_5() {
        let sample = b"aa{{{}}{}}aa";
        let r = find_obj_bounds(sample, 0, 11).unwrap();
        assert_eq!(r, (2, 9));
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
}
