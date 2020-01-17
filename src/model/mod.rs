use std::error::Error;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

mod core_model;
mod cursor;
mod event_type;
mod misc;
mod publishing;
mod subscription;

pub use self::core_model::*;
pub use self::cursor::*;
pub use self::event_type::*;
pub use self::misc::*;
pub use self::publishing::*;
pub use self::subscription::*;

use crate::env_vars::*;
use crate::helpers::MessageError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NakadiHost(Url);

impl NakadiHost {
    pub fn new(url: Url) -> Self {
        Self(url)
    }

    pub fn into_inner(self) -> Url {
        self.0
    }

    pub fn from_env() -> Result<Self, Box<dyn Error + 'static>> {
        Self::from_env_named(NAKADION_NAKADI_HOST_ENV_VAR)
    }

    pub fn from_env_named<T: AsRef<str>>(name: T) -> Result<Self, Box<dyn Error + 'static>> {
        must_env_parsed!(name.as_ref())
    }
}

impl AsRef<str> for NakadiHost {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl FromStr for NakadiHost {
    type Err = Box<dyn Error + 'static>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(NakadiHost(s.parse().map_err(|err| {
            MessageError::new(format!("could not parse nakadi host id: {}", err))
        })?))
    }
}

impl From<Url> for NakadiHost {
    fn from(url: Url) -> Self {
        Self(url)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventId(pub Uuid);

/// Parameters for starting a new stream on a subscription
///
/// See also [Nakadi Manual](https://nakadi.io/manual.html#/subscriptions/subscription_id/events_post)
#[derive(Debug, Default, Clone, Serialize)]
pub struct StreamParameters {
    /// List of partitions to read from in this stream. If absent or empty - then the partitions will be
    /// automatically assigned by Nakadi.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<EventTypePartition>,
    /// The maximum number of uncommitted events that Nakadi will stream before pausing the stream. When in
    /// paused state and commit comes - the stream will resume.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uncommitted_events: Option<u32>,
    /// Maximum number of Events in each chunk (and therefore per partition) of the stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_limit: Option<u32>,
    /// Maximum number of Events in this stream (over all partitions being streamed in this
    /// connection)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_limit: Option<u32>,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "batch_flush_timeout")]
    pub batch_flush_timeout_secs: Option<u32>,
    /// Useful for batching events based on their received_at timestamp. For example, if `batch_timespan` is 5
    /// seconds then Nakadi would flush a batch as soon as the difference in time between the first and the
    /// last event in the batch exceeds 5 seconds. It waits for an event outside of the window to signal the
    /// closure of a batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "batch_timespan")]
    pub batch_timespan_secs: Option<u32>,
    /// Maximum time in seconds to wait for the flushing of each chunk (per partition).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "stream_timeout")]
    pub stream_timeout_secs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "commit_timeout")]
    /// Maximum amount of seconds that Nakadi will be waiting for commit after sending a batch to a client.
    /// In case if commit does not come within this timeout, Nakadi will initialize stream termination, no
    /// new data will be sent. Partitions from this stream will be assigned to other streams.
    /// Setting commit_timeout to 0 is equal to setting it to the maximum allowed value - 60 seconds.
    pub commit_timeout_secs: Option<u32>,
}
