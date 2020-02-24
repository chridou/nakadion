use std::time::Duration;

use serde::{Deserialize, Serialize};

new_type! {
    #[doc="The internal tick interval.\n\nThe applied value is always between [100..5_000] ms. \
    When set outside of its bounds it will be adjusted to fit within the bounds.\n\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct TickIntervalMillis(u64, env="TICK_INTERVAL_MILLIS");
}
impl TickIntervalMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }

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
impl From<TickIntervalMillis> for Duration {
    fn from(v: TickIntervalMillis) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The time after which a handler is considered inactive.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct HandlerInactivityTimeoutSecs(u64, env="HANDLER_INACTIVITY_TIMEOUT_SECS");
}
impl HandlerInactivityTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl Default for HandlerInactivityTimeoutSecs {
    fn default() -> Self {
        Self(std::u64::MAX)
    }
}
impl From<HandlerInactivityTimeoutSecs> for Duration {
    fn from(v: HandlerInactivityTimeoutSecs) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The time after which a partition is considered inactive.\n\nDefault is 90 seconds\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct PartitionInactivityTimeoutSecs(u64, env="PARTITION_INACTIVITY_TIMEOUT_SECS");
}
impl PartitionInactivityTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl Default for PartitionInactivityTimeoutSecs {
    fn default() -> Self {
        Self(90)
    }
}
impl From<PartitionInactivityTimeoutSecs> for Duration {
    fn from(v: PartitionInactivityTimeoutSecs) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The time after which a stream is considered stuck and has to be aborted.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct StreamDeadTimeoutSecs(u64, env="STREAM_DEAD_TIMEOUT_SECS");
}
impl StreamDeadTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl From<StreamDeadTimeoutSecs> for Duration {
    fn from(v: StreamDeadTimeoutSecs) -> Self {
        v.into_duration()
    }
}
new_type! {
    #[doc="Emits a warning when no lines were received from Nakadi.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct WarnStreamStalledSecs(u64, env="WARN_STREAM_STALLED_SECS");
}
impl WarnStreamStalledSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl From<WarnStreamStalledSecs> for Duration {
    fn from(v: WarnStreamStalledSecs) -> Self {
        v.into_duration()
    }
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
    #[doc="The maximum number of attempts to be made to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct MaxConnectAttempts(usize, env="MAX_CONNECT_ATTEMPTS");
}
impl Default for MaxConnectAttempts {
    fn default() -> Self {
        std::usize::MAX.into()
    }
}
new_type! {
    #[doc="The maximum retry delay between failed attempts to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectStreamRetryMaxDelaySecs(u64, env="CONNECT_STREAM_RETRY_MAX_DELAY_SECS");
}
impl Default for ConnectStreamRetryMaxDelaySecs {
    fn default() -> Self {
        300.into()
    }
}
impl ConnectStreamRetryMaxDelaySecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl From<ConnectStreamRetryMaxDelaySecs> for Duration {
    fn from(v: ConnectStreamRetryMaxDelaySecs) -> Self {
        v.into_duration()
    }
}
new_type! {
    #[doc="The timeout for a request made to Nakadi to connect to a stream.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct ConnectStreamTimeoutSecs(u64, env="CONNECT_STREAM_TIMEOUT_SECS");
}
impl ConnectStreamTimeoutSecs {
    pub fn into_duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}
impl Default for ConnectStreamTimeoutSecs {
    fn default() -> Self {
        10.into()
    }
}
impl From<ConnectStreamTimeoutSecs> for Duration {
    fn from(v: ConnectStreamTimeoutSecs) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The timeout for a request made to Nakadi to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitAttemptTimeoutMillis(u64, env="COMMIT_ATTEMPT_TIMEOUT_MILLIS");
}
impl CommitAttemptTimeoutMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}
impl Default for CommitAttemptTimeoutMillis {
    fn default() -> Self {
        1000.into()
    }
}
impl From<CommitAttemptTimeoutMillis> for Duration {
    fn from(v: CommitAttemptTimeoutMillis) -> Self {
        v.into_duration()
    }
}

new_type! {
    #[doc="The delay between failed attempts to commit cursors.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct CommitRetryDelayMillis(u64, env="COMMIT_RETRY_DELAY_MILLIS");
}
impl CommitRetryDelayMillis {
    pub fn into_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}
impl Default for CommitRetryDelayMillis {
    fn default() -> Self {
        500.into()
    }
}
impl From<CommitRetryDelayMillis> for Duration {
    fn from(v: CommitRetryDelayMillis) -> Self {
        v.into_duration()
    }
}
