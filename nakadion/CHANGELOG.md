# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.27.2] - 2020-08-26

### CHANGED

- Fixed tracking of active partitions

## [0.27.1] - 2020-08-25

### CHANGED

- Alert triggers with `metrix` stay on for a default of 61 seconds (fromerly 60)

## [0.27.0] - 2020-08-13

### ADDED

- `LifecycleHandler` for the consumer which can be used to get notified
- `Consumer` can report unconsumed events via `Instruments`

### CHANGED

- `Consumer` can not be cloned anymore.

## [0.26.2] - 2020-06-05

### CHANGED

- `Builder` for `ApiClient` is public now

## [0.26.1] - 2020-05-21

### ADDED

- metrics triggering when a stream ends and when a consumer starts or stops

### CHANGED

- Default of `CommitAttemptTimeoutMillis` is now 2.5 seconds
- Default of `CommitTimeoutMillis` is now 10 seconds

## [0.26.0]

### CHANGED

- publishing returns more detailed failure type `FailedSubmission`
- `PartialFailureStrategy` became `SubmissionFailureStrategy` and `non_exhaustive`

### REMOVED

- `PartialFailureStrategy` and also its environment variable mapping

## [0.25.4] - 2020-05-14

### CHANGED

- histograms reset after configurable inactivity

### ADDED

- configurable histogram reset with `HistogramInactivityResetSecs`

## [0.25.3] - 2020-05-14

### CHANGED

- name of env for for `PublishTimeoutMillis` to `PUBLISH_TIMEOUT_MILLIS`.

## [0.25.2] - 2020-05-14

### CHANGED

- made method `Partitioner::partition_for_key` public

## [0.25.1] - 2020-05-14

### CHANGED

- (fix) measurement of batch gaps was increasing processed batches.

## [0.25.0] - 2020-05-14

### CHANGED

- set default timeout for publishing to 31 seconds

### ADDED

- metrics on items committed by trigger

## [0.24.3] - 2020-05-14

### ADDED

- measure time between to batches with events
- collect metrics on reasons for commits

## [0.24.2] - 2020-05-13

### CHANGED

- implemented `PartitionKeyExtractable` in a way so that clients can implenent it

## [0.24.1] - 2020-05-13

### CHANGED

- `Partitioner` should not consume self on assign
- `Partitioner` can be created from a reference of `ApiClient`

## [0.24.0] - 2020-05-13

### ADDED

- batches carry a completion timestamp to track timings without getting bytes from the network

### CHANGED

- renamed `BatchLine` to `EventStreamBatch` to match Nakadi names
- renamed `BatchLineStream` to `EventStream` to match Nakadi names
- renamed `ParseLineError` to `ParseEventsBatchError`
- renamed `BatchLineError` to `EventStreamError`
- renamed boxed stream types in `components::connector`
- renamed all the timestamps in batches and events

## [0.23.3] - 2020-05-11

- added metrics for connector

## [0.23.2] - 2020-05-11

### ADDED

- metric triggered when stream is dead
- metrics for ticks
- metrics for active event type partition (was forgotten in `metrix` feature)

## [0.23.1] - 2020-05-11

### CHANGED

- Committer will not tear down the consumer anymore

## [0.23.0] - 2020-05-11

### CHANGED

- `Instrumentation` trait changed and improved (breaking change)
- Metrix output with `metrix` changed (breaking for feature `metrix`)
- StdLoggers log a timestamp

## [0.22.9] - 2020-05-10

### CHANGED

- debug logging can no longer be enabled with a feature but with `DebugLoggingEnabled` instead.
- internal code reorganization

### ADDED

- `LogPartitionEventsMode` to configure how life cycle events for partitions are logged

### REMOVED

- `LogPartitionEvents` in favour of `LogPartitionEventsMode`

## [0.22.8] - 2020-05-08

### CHANGED

- (fix): trigger metrics for stream error
- Logging of partition activity changes can be turned on and off to control verbosity

## [0.22.7] - 2020-05-08

### ADDED

- metrics for IO and parse errors on the stream

## [0.22.6] - 2020-05-07

### CHANGED

- `BatchLine::received_at` is now the timestamp when the batch was completely received.
- Default of `StreamDeadPolicy` is to cancel the stream after 300s with no frames received. Previous was to never abort
- metrics names on committer (requests renamed to "committed" "not committed")
- default commit config in consumer sets stream commit timeout if not explicitly set as a stream commit timeout

### ADDED

- log why a stream was aborted
- metrics for failed commit attempts

## [0.22.5] - 2020-05-06

### CHANGED

- ensure that stream end message is always sent
- log before shutting down internals

## [0.22.4] - 2020-05-06

### CHANGED

- removed the nasty tick print

## [0.22.3] - 2020-05-06

### CHANGED

- enabled committer instrumentation

### ADDED

- metrics for incoming chunks and frames

## [0.22.1] - 2020-05-05

### CHANGED

- Fixed event type partition tracking
- Improve logging format

### ADDED

- metric for `cursor_received_at` in committer
- `WarnNoFramesSecs` time to emit a warning if no frames were received from Nakadi
- `WarnNoEventsSecs` time to emit a warning if no events were received from Nakadi
- added metric to trigger when no events or frames were received and a warning should be emitted

### REMOVED

- `WarnStreamStalledSecs` removed (this just affects logging)

## [0.22.0] - 2020-04-28

### CHANGED

- unified env config funs and load without prefix
- `ApiClient` fully configurable from environment
- renamed function to initialize structs from env
- functions which load from env with prefix do not add an underscore if prefix is empty

### ADDED
- logging configurable via environment


## [0.21.0] - 2020-04-28

### CHANGED

- renamed `SubscriptionCursorWithoutToken` to `EventTypeCursor` because the name was too long

### ADDED

- Default env name for types as constants
- functions to retrieve values from environment by default type name

## [0.20.4] - 2020-04-27

### CHANGED

- Connect retry keeps interval at 60s for approx 1 hour (incident recovery phase)
- Make errors internally sync

## [0.20.3] - 2020-04-27

### CHANGED

- added public constructor for `TokenError`

## [0.20.2] - 2020-04-27

### Changed
- Improved documentation

### Removed
- Unused type `StreamDeadTimeoutSecs`. `StreamDeadPolicy` is to be used instead.

## [0.20.1] - 2020-03-06

### Changed
- `LoggingAdapter` implements `Logger`

## [0.20.0] - 2020-03-06

### Changed
- Changes were not tracked during the alpha phase
- EVERYTHING. This is a complete rewrite. No guaranteed compatibility to older versions (also alpha and beta)!

## [0.20.0-alpha.X]

### Changed
- Changes are not tracked during the alpha phase
- EVERYTHING. This is a complete rewrite. No guaranteed compatibility to older versions!