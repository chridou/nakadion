use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::api::PublishApi;

pub enum PartialFailureStrategy {
    Abort,
    RetryAll,
    RetryFailed,
}

new_type! {
    #[doc="The time a publish attempt for the events batch may take.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct PublishAttemptTimeoutMillis(u64, env="PUBLISH_ATTEMPT_TIMEOUT_MILLIS");
}

impl PublishAttemptTimeoutMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}
impl Default for PublishAttemptTimeoutMillis {
    fn default() -> Self {
        Self(1_000)
    }
}
impl From<PublishAttemptTimeoutMillis> for Duration {
    fn from(v: PublishAttemptTimeoutMillis) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The a publishing the events batch including retries may take.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct PublishTimeoutMillis(u64, env="PUBLISH_TIMEOUT_MILLIS");
}

impl PublishTimeoutMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(5_000)
    }
}
impl Default for PublishTimeoutMillis {
    fn default() -> Self {
        Self(std::u64::MAX)
    }
}
impl From<PublishTimeoutMillis> for Duration {
    fn from(v: PublishTimeoutMillis) -> Self {
        v.into_duration()
    }
}

pub struct Config {
    pub timeout_millis: PublishTimeoutMillis,
    pub attempt_timeout_millis: PublishAttemptTimeoutMillis,
}

pub struct Publisher<C> {
    api_client: C,
}
