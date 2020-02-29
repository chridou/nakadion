use std::time::Duration;

use serde::{Deserialize, Serialize};

new_type! {
    #[doc="The internal tick interval.\n\nThe applied value is always between [100..5_000] ms. \
    When set outside of its bounds it will be adjusted to fit within the bounds.\n\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct TickIntervalMillis(u64, env="TICK_INTERVAL_MILLIS");
}
impl TickIntervalMillis {
    /// Only 100ms up to 5_000ms allowed. We simply adjust the
    /// values because there is no reason to crash if these have been set
    /// to an out of range value.
    pub fn adjust(self) -> TickIntervalMillis {
        std::cmp::min(5_000, std::cmp::max(100, self.0)).into()
    }
}
impl Default for TickIntervalMillis {
    fn default() -> Self {
        1000.into()
    }
}

new_type! {
    #[doc="The time after which a handler is considered inactive.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct HandlerInactivityTimeoutSecs(u64, env="HANDLER_INACTIVITY_TIMEOUT_SECS");
}
impl Default for HandlerInactivityTimeoutSecs {
    fn default() -> Self {
        Self(std::u64::MAX)
    }
}

new_type! {
    #[doc="The time after which a partition is considered inactive.\n\nDefault is 90 seconds\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct PartitionInactivityTimeoutSecs(u64, env="PARTITION_INACTIVITY_TIMEOUT_SECS");
}
impl Default for PartitionInactivityTimeoutSecs {
    fn default() -> Self {
        Self(90)
    }
}

new_type! {
    #[doc="The time after which a stream is considered stuck and has to be aborted.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct StreamDeadTimeoutSecs(u64, env="STREAM_DEAD_TIMEOUT_SECS");
}
new_type! {
    #[doc="Emits a warning when no lines were received from Nakadi.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct WarnStreamStalledSecs(u64, env="WARN_STREAM_STALLED_SECS");
}

new_type! {
    #[doc="If `true` abort the consumer when an auth error occurs while connecting to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct AbortConnectOnAuthError(bool, env="ABORT_CONNECT_ON_AUTH_ERROR");
}
impl Default for AbortConnectOnAuthError {
    fn default() -> Self {
        false.into()
    }
}
new_type! {
    #[doc="If `true` abort the consumer when a subscription does not exist when connection to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct AbortConnectOnSubscriptionNotFound(bool, env="ABORT_CONNECT_ON_SUBSCRIPTION_NOT_FOUND");
}
impl Default for AbortConnectOnSubscriptionNotFound {
    fn default() -> Self {
        true.into()
    }
}
new_type! {
    #[doc="The maximum time for connecting to a stream.\n\n\
    Default is infinite."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct MaxConnectTimeSecs(u64, env="MAX_CONNECT_TIME_SECS");
}

new_type! {
    #[doc="The maximum retry delay between failed attempts to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectStreamRetryMaxDelaySecs(u64, env="CONNECT_STREAM_RETRY_MAX_DELAY_SECS");
}
impl Default for ConnectStreamRetryMaxDelaySecs {
    fn default() -> Self {
        300.into()
    }
}
new_type! {
    #[doc="The timeout for a request made to Nakadi to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct ConnectStreamTimeoutSecs(u64, env="CONNECT_STREAM_TIMEOUT_SECS");
}
impl Default for ConnectStreamTimeoutSecs {
    fn default() -> Self {
        10.into()
    }
}

new_type! {
    #[doc="The timeout for a request made to Nakadi to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitAttemptTimeoutMillis(u64, env="COMMIT_ATTEMPT_TIMEOUT_MILLIS");
}
impl Default for CommitAttemptTimeoutMillis {
    fn default() -> Self {
        1000.into()
    }
}

new_type! {
    #[doc="The delay between failed attempts to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub millis struct CommitRetryDelayMillis(u64, env="COMMIT_RETRY_DELAY_MILLIS");
}
impl Default for CommitRetryDelayMillis {
    fn default() -> Self {
        500.into()
    }
}
