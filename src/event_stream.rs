use std::io::Error as IoError;
use std::time::Instant;

use crate::model::StreamId;

pub type BatchBytes = Vec<u8>;
pub type BatchResult = Result<RawBatch, IoError>;

/// A line as received from Nakadi plus a timestamp.
pub struct RawBatch {
    /// The bytes received as a line from Nakadi
    pub bytes: BatchBytes,
    /// The timestamp for when this line was received
    pub received_at: Instant,
}

pub struct EventStream {
    pub stream_id: StreamId,
    flavour: EventStreamFlavour,
}

impl EventStream {
    pub(crate) fn new(stream_id: StreamId, response: reqwest::Response) -> Self {
        Self {
            stream_id,
            flavour: EventStreamFlavour::Reqwest(Box::new(reqwest_stream::ReqwestStream::new(
                response,
            ))),
        }
    }

    pub fn custom<I>(stream_id: StreamId, stream: I) -> Self
    where
        I: Iterator<Item = BatchResult> + Send + 'static,
    {
        Self {
            stream_id,
            flavour: EventStreamFlavour::Custom(Box::new(stream)),
        }
    }
}

impl Iterator for EventStream {
    type Item = BatchResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.flavour {
            EventStreamFlavour::Custom(ref mut it) => it.next(),
            EventStreamFlavour::Reqwest(ref mut it) => it.next(),
        }
    }
}

enum EventStreamFlavour {
    Custom(Box<dyn Iterator<Item = BatchResult> + Send + 'static>),
    Reqwest(Box<self::reqwest_stream::ReqwestStream>),
}

mod reqwest_stream {
    use std::io::{BufRead, BufReader, Split};

    use reqwest::Response;

    use super::*;

    const LINE_SPLIT_BYTE: u8 = b'\n';

    pub struct ReqwestStream(Split<BufReader<Response>>);

    /// An iterator over lines `Nakadion` understands.
    impl ReqwestStream {
        pub fn new(response: Response) -> Self {
            let reader = BufReader::with_capacity(1024 * 1024, response);
            ReqwestStream(reader.split(LINE_SPLIT_BYTE))
        }
    }

    impl Iterator for ReqwestStream {
        type Item = BatchResult;

        fn next(&mut self) -> Option<Self::Item> {
            self.0.next().map(|r| {
                r.map(|bytes| RawBatch {
                    bytes,
                    received_at: Instant::now(),
                })
            })
        }
    }
}
