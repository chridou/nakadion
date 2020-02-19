use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::Error;

/// Defines how to dispatch batches to handlers.
///
/// # FromStr
///
/// ```rust
/// use nakadion::consumer::DispatchStrategy;
///
/// let strategy = "all_seq".parse::<DispatchStrategy>().unwrap();
/// assert_eq!(strategy, DispatchStrategy::AllSeq);
///
/// let strategy = "event_type_par".parse::<DispatchStrategy>().unwrap();
/// assert_eq!(strategy, DispatchStrategy::EventTypePar);
///
/// let strategy = "event_type_partition_par".parse::<DispatchStrategy>().unwrap();
/// assert_eq!(strategy, DispatchStrategy::EventTypePartitionPar);
/// ```
///
/// JSON is also valid:
///
/// ```rust
/// use nakadion::consumer::DispatchStrategy;
///
/// let strategy = "{\"strategy\": \"all_seq\"}".parse::<DispatchStrategy>().unwrap();
/// assert_eq!(strategy, DispatchStrategy::AllSeq);
///
/// let strategy = "{\"strategy\": \"event_type_par\"}".parse::<DispatchStrategy>().unwrap();
/// assert_eq!(strategy, DispatchStrategy::EventTypePar);
///
/// let strategy = "{\"strategy\": \"event_type_partition_par\"}".parse::<DispatchStrategy>().unwrap();
/// assert_eq!(strategy, DispatchStrategy::EventTypePartitionPar);
/// ```
///
/// # Environment variables
///
/// Fetching values from the environment uses `FromStr` for parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
#[serde(tag = "strategy")]
pub enum DispatchStrategy {
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
    /// This is the default `DispatchStrategy`.
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

impl DispatchStrategy {
    env_funs!("DISPATCH_STRATEGY");
}

impl Default for DispatchStrategy {
    fn default() -> Self {
        DispatchStrategy::EventTypePar
    }
}

impl fmt::Display for DispatchStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DispatchStrategy::AllSeq => write!(f, "all_seq")?,
            DispatchStrategy::EventTypePar => write!(f, "event_type_par")?,
            DispatchStrategy::EventTypePartitionPar => write!(f, "event_type_partition_par")?,
        }

        Ok(())
    }
}

impl FromStr for DispatchStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.starts_with('{') {
            return Ok(serde_json::from_str(s)?);
        }

        match s {
            "all_seq" => Ok(DispatchStrategy::AllSeq),
            "event_type_par" => Ok(DispatchStrategy::EventTypePar),
            "event_type_partition_par" => Ok(DispatchStrategy::EventTypePartitionPar),
            _ => Err(Error::new(format!("not a valid dispatch strategy: {}", s))),
        }
    }
}

/// Defines how to commit cursors
///
/// This value should always be set when creating a `Consumer` because otherwise
/// a it will be guessed by `Nakadion` which might not result in best performance.
///
/// # FromStr
///
/// ```rust
/// use nakadion::consumer::CommitStrategy;
///
/// let strategy = "immediately".parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::Immediately);
///
/// let strategy = "latest_possible".parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::LatestPossible);
///
/// let strategy = "after seconds:1 cursors:2 events:3".parse::<CommitStrategy>().unwrap();
/// assert_eq!(
///     strategy,
///     CommitStrategy::After {
///         seconds: Some(1),
///         cursors: Some(2),
///         events: Some(3),
///     }
/// );
///
/// let strategy = "after seconds:1 events:3".parse::<CommitStrategy>().unwrap();
/// assert_eq!(
///     strategy,
///     CommitStrategy::After {
///         seconds: Some(1),
///         cursors: None,
///         events: Some(3),
///     }
/// );
///
/// assert!("after".parse::<CommitStrategy>().is_err());
/// ```
///
/// JSON is also valid:
///
/// ```rust
/// use nakadion::consumer::CommitStrategy;
///
/// let strategy = r#"{"strategy": "immediately"}"#.parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::Immediately);
///
/// let strategy = r#"{"strategy": "latest_possible"}"#.parse::<CommitStrategy>().unwrap();
/// assert_eq!(strategy, CommitStrategy::LatestPossible);
///
/// let strategy = r#"{"strategy": "after","seconds":1, "cursors":3}"#.parse::<CommitStrategy>().unwrap();
/// assert_eq!(
///     strategy,
///     CommitStrategy::After {
///         seconds: Some(1),
///         cursors: Some(3),
///         events: None,
///     }
/// );
/// ```
///
/// # Environment variables
///
/// Fetching values from the environment uses `FromStr` for parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "strategy")]
pub enum CommitStrategy {
    /// Commit cursors immediately
    Immediately,
    /// Commit cursors as late as possible.
    ///
    /// This strategy is determined by the commit timeout defined
    /// via `StreamParameters`
    LatestPossible,
    /// Commit after on of the criteria was met:
    ///
    /// * `seconds`: After `seconds` seconds
    /// * `cursors`: After `cursors` cursors have been received
    /// * `events`: After `events` have been received. This requires the `BatchHandler` to give
    /// a hint on the amount of events processed.
    After {
        #[serde(skip_serializing_if = "Option::is_none")]
        seconds: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cursors: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        events: Option<u32>,
    },
}

impl CommitStrategy {
    env_funs!("COMMIT_STRATEGY");

    pub fn validate(&self) -> Result<(), Error> {
        match self {
            CommitStrategy::After {
                seconds: None,
                cursors: None,
                events: None,
            } => Err(Error::new(
                "'CommitStrategy::After' with all fields set to `None` is not valid",
            )),
            CommitStrategy::After {
                seconds,
                cursors,
                events,
            } => {
                if let Some(seconds) = seconds {
                    if *seconds == 0 {
                        return Err(Error::new("'CommitStrategy::After::seconds' must not be 0"));
                    }
                } else if let Some(cursors) = cursors {
                    if *cursors == 0 {
                        return Err(Error::new("'CommitStrategy::After::cursors' must not be 0"));
                    }
                } else if let Some(events) = events {
                    if *events == 0 {
                        return Err(Error::new("'CommitStrategy::After::events' must not be 0"));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl FromStr for CommitStrategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_after(s: &str) -> Result<CommitStrategy, Error> {
            let parts: Vec<_> = s.split(' ').filter(|s| !s.is_empty()).collect();

            let mut seconds: Option<u32> = None;
            let mut cursors: Option<u32> = None;
            let mut events: Option<u32> = None;

            if parts.is_empty() {
                return Err(Error::new("invalid"));
            }

            if parts[0] != "after" {
                return Err(Error::new("must start with 'after'"));
            }

            for p in parts.into_iter().skip(1) {
                let parts: Vec<_> = p.split(':').collect();
                if parts.len() != 2 {
                    return Err(Error::new(format!("not valid: {}", p)));
                }
                let v: u32 = parts[1]
                    .parse()
                    .map_err(|err| Error::new(format!("{} not an u32: {}", parts[0], err)))?;

                match parts[0] {
                    "seconds" => seconds = Some(v),
                    "cursors" => cursors = Some(v),
                    "events" => events = Some(v),
                    _ => {
                        return Err(Error::new(format!(
                            "not a part of CommitStrategy: {}",
                            parts[0]
                        )))
                    }
                }
            }

            Ok(CommitStrategy::After {
                seconds,
                events,
                cursors,
            })
        }
        let s = s.trim();

        if s.starts_with('{') {
            return Ok(serde_json::from_str(s)?);
        }

        let strategy = match s {
            "immediately" => CommitStrategy::Immediately,
            "latest_possible" => CommitStrategy::LatestPossible,
            _ => parse_after(s).map_err(|err| {
                Error::new(format!(
                    "could not parse CommitStrategy from {}: {}",
                    s, err
                ))
            })?,
        };
        strategy.validate()?;
        Ok(strategy)
    }
}

impl fmt::Display for CommitStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitStrategy::Immediately => write!(f, "immediately")?,
            CommitStrategy::LatestPossible => write!(f, "latest_possible")?,
            CommitStrategy::After {
                seconds: None,
                cursors: None,
                events: None,
            } => {
                write!(f, "after")?;
            }
            CommitStrategy::After {
                seconds,
                cursors,
                events,
            } => {
                write!(f, "after ")?;
                if let Some(seconds) = seconds {
                    write!(f, "seconds:{}", seconds)?;
                    if cursors.is_some() || events.is_some() {
                        write!(f, " ")?;
                    }
                }

                if let Some(cursors) = cursors {
                    write!(f, "cursors:{}", cursors)?;
                    if events.is_some() {
                        write!(f, " ")?;
                    }
                }
                if let Some(events) = events {
                    write!(f, "events:{}", events)?;
                }
            }
        }

        Ok(())
    }
}

/// Specifies when a stream is considered dead and has to be aborted.
///
/// Once a stream is considered dead a reconnect for a new stream
/// will be attempted.
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
/// let policy = r#"{"policy": "never"}"#.parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(policy, StreamDeadPolicy::Never);
///
/// let policy = r#"{"policy": "no_frames_for", "seconds": 1}"#.parse::<StreamDeadPolicy>().unwrap();
/// assert_eq!(
///     policy,
///     StreamDeadPolicy::NoFramesFor { seconds: 1}
/// );
///
/// let policy = r#"{"policy": "no_events_for", "seconds": 2}"#.parse::<StreamDeadPolicy>().unwrap();
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
#[serde(tag = "policy")]
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
        StreamDeadPolicy::Never
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
        if s.starts_with('{') {
            return Ok(serde_json::from_str(s)?);
        }

        let strategy = match s {
            "never" => StreamDeadPolicy::Never,
            _ => parse_internal(s).map_err(|err| {
                Error::new(format!(
                    "could not parse CommitStrategy from {}: {}",
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
