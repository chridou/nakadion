use hyper;
use hyper::header::{self, Header, Raw};
use std::fmt;

use nakadi::model::{FlowId, StreamId};

#[derive(Debug, Clone)]
pub struct XNakadiStreamId(pub StreamId);

impl Header for XNakadiStreamId {
    fn header_name() -> &'static str {
        "X-Nakadi-StreamId"
    }

    fn parse_header(raw: &Raw) -> hyper::Result<XNakadiStreamId> {
        if raw.len() > 0 {
            match raw.one().map(::std::str::from_utf8) {
                Some(Ok(value)) => Ok(XNakadiStreamId(StreamId(value.to_string()))),
                Some(Err(_)) => Err(hyper::Error::Header),
                None => Err(hyper::Error::Header),
            }
        } else {
            Err(hyper::Error::Header)
        }
    }

    fn fmt_header(&self, f: &mut header::Formatter) -> fmt::Result {
        f.fmt_line(&(self.0).0)
    }
}

#[derive(Debug, Clone)]
pub struct XFlowId(pub FlowId);

impl Header for XFlowId {
    fn header_name() -> &'static str {
        "X-Flow-Id"
    }

    fn parse_header(raw: &Raw) -> hyper::Result<XFlowId> {
        if raw.len() > 0 {
            match raw.one().map(::std::str::from_utf8) {
                Some(Ok(value)) => Ok(XFlowId(FlowId(value.to_string()))),
                Some(Err(_)) => Err(hyper::Error::Header),
                None => Err(hyper::Error::Header),
            }
        } else {
            Err(hyper::Error::Header)
        }
    }

    fn fmt_header(&self, f: &mut header::Formatter) -> fmt::Result {
        f.fmt_line(&(self.0).0)
    }
}
