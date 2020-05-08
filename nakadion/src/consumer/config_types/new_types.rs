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
    #[doc="Emits a warning when no frames (lines) were received from Nakadi.\n\n\
    Default is 30s.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct WarnNoFramesSecs(u64, env="WARN_NO_FRAMES_SECS");
}
impl Default for WarnNoFramesSecs {
    fn default() -> Self {
        Self(30)
    }
}

new_type! {
    #[doc="Emits a warning when no events were received from Nakadi.\n\n\
    Default is 60s.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub secs struct WarnNoEventsSecs(u64, env="WARN_NO_EVENTS_SECS");
}
impl Default for WarnNoEventsSecs {
    fn default() -> Self {
        Self(60)
    }
}

new_type! {
    #[doc="If `true` partition related events (like activation, etc.) will be logged.\n\n\
    Default is true.\n"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub copy struct LogPartitionEvents(bool, env="LOG_PARTITION_EVENTS");
}
impl Default for LogPartitionEvents {
    fn default() -> Self {
        Self(true)
    }
}
