use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::Error;

/// Defines how to dispatch batches to handlers.
///
/// The default is `DispatchMode::AllSeq`
///
/// # FromStr
///
/// ```rust
/// use nakadion::consumer::DispatchMode;
///
/// let strategy = "all_seq".parse::<DispatchMode>().unwrap();
/// assert_eq!(strategy, DispatchMode::AllSeq);
///
/// let strategy = "event_type_par".parse::<DispatchMode>().unwrap();
/// assert_eq!(strategy, DispatchMode::EventTypePar);
///
/// let strategy = "event_type_partition_par".parse::<DispatchMode>().unwrap();
/// assert_eq!(strategy, DispatchMode::EventTypePartitionPar);
/// ```
///
/// JSON is also valid:
///
/// ```rust
/// use nakadion::consumer::DispatchMode;
///
/// let strategy = "\"all_seq\"".parse::<DispatchMode>().unwrap();
/// assert_eq!(strategy, DispatchMode::AllSeq);
///
/// let strategy = "\"event_type_par\"".parse::<DispatchMode>().unwrap();
/// assert_eq!(strategy, DispatchMode::EventTypePar);
///
/// let strategy = "\"event_type_partition_par\"".parse::<DispatchMode>().unwrap();
/// assert_eq!(strategy, DispatchMode::EventTypePartitionPar);
/// ```
///
/// # Environment variables
///
/// Fetching values from the environment uses `FromStr` for parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum DispatchMode {
    /// Dispatch all batches to a single worker(handler)
    ///
    /// This means batches all are processed sequentially by a single handler.
    ///
    /// This will always request a handler with `HandlerAssignment::Unspecified`
    /// from the `BatchHandlerFactory`.
    /// This means that if multiple event types are consumed, the handler is responsible
    /// for determining the event type to be processed from `BatchMeta`.
    ///
    /// It is suggested to not use this strategy if multiple event types
    /// are expected.
    AllSeq,
    /// Dispatch all batches to a dedicated worker for an
    /// event type.
    ///
    /// This means batches are processed sequentially for each event type but
    /// the maximum parallelism is the number of event types.
    ///
    /// This will always request a handler with `HandlerAssignment::EventType(EventTypeName)`
    /// from the `BatchHandlerFactory`.
    ///
    /// This is the default `DispatchMode`.
    EventTypePar,
    /// Dispatch all batches to a dedicated worker for an
    /// partition on each event type.
    ///
    /// This means batches are processed sequentially for each individual partition
    /// of an event type. The maximum parallelism is the sum of each event type multiplied by
    /// its number of partitions.
    ///
    /// This will always request a handler with
    /// `HandlerAssignment::EventTypePartition(EventTypePartitionName)`
    /// from the `BatchHandlerFactory`.
    EventTypePartitionPar,
}

impl DispatchMode {
    env_funs!("DISPATCH_MODE");
}

impl Default for DispatchMode {
    fn default() -> Self {
        DispatchMode::AllSeq
    }
}

impl fmt::Display for DispatchMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DispatchMode::AllSeq => write!(f, "all_seq")?,
            DispatchMode::EventTypePar => write!(f, "event_type_par")?,
            DispatchMode::EventTypePartitionPar => write!(f, "event_type_partition_par")?,
        }

        Ok(())
    }
}

impl FromStr for DispatchMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.starts_with('{') || s.starts_with('\"') {
            return Ok(serde_json::from_str(s)?);
        }

        match s {
            "all_seq" => Ok(DispatchMode::AllSeq),
            "event_type_par" => Ok(DispatchMode::EventTypePar),
            "event_type_partition_par" => Ok(DispatchMode::EventTypePartitionPar),
            _ => Err(Error::new(format!("not a valid dispatch strategy: {}", s))),
        }
    }
}

/// Specifies when a stream is considered dead and has to be aborted.
///
/// Once a stream is considered dead a reconnect for a new stream
/// will be attempted.
///
/// The default is `NoFramesFor { seconds: 300 }`
///
/// # FromStr
///
/// ```rust
/// use nakadion::consumer::StreamDeadPolicy;
///
/// let policy = "never".parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(policy, StreamDeadPolicy::Never);
///
/// let policy = "no_frames_for_seconds 1".parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(
///     policy,
///     StreamDeadPolicy::NoFramesFor { seconds: 1}
/// );
///
/// let policy = "no_events_for_seconds 2".parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(
///     policy,
///     StreamDeadPolicy::NoEventsFor { seconds: 2}
/// );
/// ```
///
/// JSON is also valid:
///
/// ```rust
/// use nakadion::consumer::StreamDeadPolicy;
///
/// let policy = r#""never""#.parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(policy, StreamDeadPolicy::Never);
///
/// let policy = r#"{"no_frames_for":{"seconds": 1}}"#.parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(
///     policy,
///     StreamDeadPolicy::NoFramesFor { seconds: 1}
/// );
///
/// let policy = r#"{"no_events_for":{"seconds": 2}}"#.parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(
///     policy,
///     StreamDeadPolicy::NoEventsFor { seconds: 2}
/// );
/// ```
///
/// # Environment variables
///
/// Fetching values from the environment uses `FromStr` for parsing
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StreamDeadPolicy {
    /// The stream is never considered dead
    Never,
    /// The stream is considered dead if no frames (lines) have
    /// been received from Nakadi for `seconds`
    NoFramesFor { seconds: u32 },
    /// The stream is considered dead if no events (lines with events) have
    /// been received from Nakadi for `seconds`
    NoEventsFor { seconds: u32 },
}

impl StreamDeadPolicy {
    env_funs!("STREAM_DEAD_POLICY");

    pub(crate) fn is_stream_dead(
        self,
        last_frame_received_at: Instant,
        last_events_received_at: Instant,
    ) -> bool {
        match self {
            StreamDeadPolicy::Never => false,
            StreamDeadPolicy::NoFramesFor { seconds } => {
                last_frame_received_at.elapsed() > Duration::from_secs(u64::from(seconds))
            }
            StreamDeadPolicy::NoEventsFor { seconds } => {
                last_events_received_at.elapsed() > Duration::from_secs(u64::from(seconds))
            }
        }
    }

    pub fn validate(self) -> Result<(), Error> {
        match self {
            StreamDeadPolicy::NoFramesFor { seconds: 0 } => Err(Error::new(
                "StreamDeadPolicy::NoFramesFor::seconds may not be 0",
            )),
            StreamDeadPolicy::NoEventsFor { seconds: 0 } => Err(Error::new(
                "StreamDeadPolicy::NoFramesFor::seconds may not be 0",
            )),
            _ => Ok(()),
        }
    }
}

impl fmt::Display for StreamDeadPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamDeadPolicy::Never => write!(f, "never")?,
            StreamDeadPolicy::NoFramesFor { seconds } => {
                write!(f, "no_frames_for_seconds {}", seconds)?
            }
            StreamDeadPolicy::NoEventsFor { seconds } => {
                write!(f, "no_events_for_seconds {}", seconds)?
            }
        }

        Ok(())
    }
}

impl Default for StreamDeadPolicy {
    fn default() -> Self {
        StreamDeadPolicy::NoFramesFor { seconds: 300 }
    }
}

impl FromStr for StreamDeadPolicy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_internal(s: &str) -> Result<StreamDeadPolicy, Error> {
            let parts: Vec<_> = s.split(' ').filter(|s| !s.is_empty()).collect();

            if parts.is_empty() {
                return Err(Error::new("invalid"));
            }

            if parts[0] == "no_frames_for_seconds" {
                return parse_no_frames_for(parts);
            }

            if parts[0] == "no_events_for_seconds" {
                return parse_no_events_for(parts);
            }

            Err(Error::new("not a StreamDeadPolicy"))
        }

        let s = s.trim();
        if s.starts_with('{') || s.starts_with('\"') {
            return Ok(serde_json::from_str(s)?);
        }

        let strategy = match s {
            "never" => StreamDeadPolicy::Never,
            _ => parse_internal(s).map_err(|err| {
                Error::new(format!(
                    "could not parse StreamDeadPolicy from {}: {}",
                    s, err
                ))
            })?,
        };
        Ok(strategy)
    }
}

fn parse_no_frames_for(parts: Vec<&str>) -> Result<StreamDeadPolicy, Error> {
    if parts[0] != "no_frames_for_seconds" {
        return Err(Error::new("not StreamDeadPolicy::NoFramesFor"));
    }

    if parts.len() == 2 {
        let seconds: u32 = parts[1]
            .parse()
            .map_err(|err| Error::new(format!("{} not an u32: {}", parts[0], err)))?;
        Ok(StreamDeadPolicy::NoFramesFor { seconds })
    } else {
        Err(Error::new("not StreamDeadPolicy::NoFramesFor"))
    }
}

fn parse_no_events_for(parts: Vec<&str>) -> Result<StreamDeadPolicy, Error> {
    if parts[0] != "no_events_for_seconds" {
        return Err(Error::new("not StreamDeadPolicy::NoEventsFor"));
    }

    if parts.len() == 2 {
        let seconds: u32 = parts[1]
            .parse()
            .map_err(|err| Error::new(format!("{} not an u32: {}", parts[0], err)))?;
        Ok(StreamDeadPolicy::NoEventsFor { seconds })
    } else {
        Err(Error::new("not StreamDeadPolicy::NoEventsFor"))
    }
}

/// Configures the logging for partition events.
///
/// A partition event occurs if a new partition is discovered for the
/// first time on a stream (`AfterConnect`) or if it was deactivated or reactivated
/// (`ActivityChange`) caused by `PartitionInactivityTimeoutSecs`.
///
/// The default is `LogPartitionEventsMode::All`.
///
/// Disabled log messages will still be logged at DEBUG level.
///
/// # FromStr
///
/// ```rust
/// use nakadion::consumer::LogPartitionEventsMode;
///
/// let strategy = "off".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::Off);
///
/// let strategy = "after_connect".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::AfterConnect);
///
/// let strategy = "activity_change".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::ActivityChange);
///
/// let strategy = "all".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::All);
/// ```
///
/// JSON is also valid:
///
/// ```rust
/// use nakadion::consumer::LogPartitionEventsMode;
///
/// let strategy = "\"off\"".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::Off);
///
/// let strategy = "\"after_connect\"".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::AfterConnect);
///
/// let strategy = "\"activity_change\"".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::ActivityChange);
///
/// let strategy = "\"all\"".parse::<LogPartitionEventsMode>().unwrap();
/// assert_eq!(strategy, LogPartitionEventsMode::All);
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogPartitionEventsMode {
    /// Do not log
    Off,
    /// Log partitions only when encountered the first time after a stream
    /// connect
    AfterConnect,
    /// Log partitions only when inactivated or reactivated
    ActivityChange,
    /// Log all
    All,
}

impl LogPartitionEventsMode {
    env_funs!("LOG_PARTITION_EVENTS_MODE");
}

impl fmt::Display for LogPartitionEventsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogPartitionEventsMode::Off => write!(f, "off")?,
            LogPartitionEventsMode::AfterConnect => write!(f, "after_connect")?,
            LogPartitionEventsMode::ActivityChange => write!(f, "activity_change")?,
            LogPartitionEventsMode::All => write!(f, "all")?,
        }

        Ok(())
    }
}

impl FromStr for LogPartitionEventsMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.starts_with('"') {
            return Ok(serde_json::from_str(s)?);
        }

        match s {
            "off" => Ok(LogPartitionEventsMode::Off),
            "after_connect" => Ok(LogPartitionEventsMode::AfterConnect),
            "activity_change" => Ok(LogPartitionEventsMode::ActivityChange),
            "all" => Ok(LogPartitionEventsMode::All),
            _ => Err(Error::new(format!(
                "not a valid LogPartitionEventsMode: {}",
                s
            ))),
        }
    }
}

impl Default for LogPartitionEventsMode {
    fn default() -> Self {
        LogPartitionEventsMode::All
    }
}
