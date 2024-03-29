# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.30.0] - 2021-06-14

### CHANGED

- bump http-api-problem to 0.51.0
- bump env-logger to 0.9

## [0.29.2] - 2021-06-14

### CHANGED

- bump http-api-problem to 0.50.2


### FIXES

- some typos
- some clippy warnings

## [0.29.1] - 2021-03-29

### CHANGED

- bump dependencies
- MSRV 1.46

## [0.29.0] - 2021-01-16

### CHANGED

- upgrade to tokio 1
- MSRV 1.45

## [0.28.16] - 2021-01-13

### FIXED

- Retry connect on 503 (was not done before)
- MSRV: 1.42

## [0.28.15] - 2020-12-10

## [0.28.15] - 2020-12-10

### CHANGED

- updated dependencies (excl. tokio)

## [0.28.14] - 2020-10-24

### CHANGED

- If commit `CommitStragtegy::After` has no seconds set, it will default to 10 seconds

## [0.28.13] - 2020-10-23

### CHANGED

- The committer will commit in a fixed interval if "after seconds" is set with the `CommitStragtegy`

## [0.28.12] - 2020-09-30

### CHANGED

- API client builder can fill self from the environment when building the API client

### ADDED

- Methods to construct builder for API client from env
- `Publisher` has methods to be created from the environmemt so that no explicit configuration with the config is necessary

## [0.28.11] - 2020-09-22

### CHANGED

- When logging debug as info show that in the info messages by adding a [DBG] tag

## [0.28.10] - 2020-09-19

### CHANGED

- `LogConfig` got a new field `log_debug_as_info` which causes debug logging to be done at info level for applications which disable debug logging at compile time

### ADDED

- `LogDebugAsInfo` struct for logging config to cause debug logging to be at info level via an environment variable
- `LogDetailLevel` has new variants `debug` which logs all contextual data and enables debug logging
- `LogDetailLevel` has new variants `minimal` which only logs only the stream id

## [0.28.9] - 2020-09-18

### ADDED

- Even more than more debug logging...

## [0.28.8] - 2020-09-18

### Added

- More debug logging on startup of the consumer and make messages really debug

## [0.28.7] - 2020-09-18

### Added

- Debug logging on startup of the consumer

## [0.28.6] - 2020-09-17

### ADDED

- `ConnectOnConflictRetryDelaySecs` to have an individual retry delay after a conflict occurs

### CHANGED

- integration tests ensures that nakadion runs with basic and threaded scheduler (one thread)
- `ConnectConfig` is `non-exhaustive`.

## [0.28.5] - 2020-09-11

### CHANGED

- better output of connect error

## [0.28.4] - 2020-09-08

### FIXED

- Accent bug in Doc Comment

## [0.28.3] - 2020-09-08

### ADDED

- `Connector` can be configured to retry on conflicts (Status 409)

## [0.28.2] - 2020-09-08

### ADDED

- added metrics for `StreamParameters::batch_limit`
- added metrics for `StreamParameters::batch_flush_timeout_secs`
- added metrics for `StreamParameters::stream_timeout_secs`
- added metrics for `StreamParameters::commit_timeout_secs`

## [0.28.1] - 2020-09-08

### CHANGED

- Fixed uncommitted events metrics

## [0.28.0] - 2020-09-08

### ADDED

- Track uncommitted batches and events
- Added metric for `max_uncommitted_events` from `StreamParameters`.

### CHANGED

- Drain events from IO stream in seperate task
- [BREAKING] Instrumentation should not be used by multiple consumers anymore
- Trait `Instruments` has default implementations which do nothing.
- Trait `Instruments` accepts more parameters for in flight metrics.
- In metrics for `CommitTriger` the word "cursors" has been replaced with "batches" since this is whats actually measured.

### Removed

- Background committer

### ADDED

- Metrics for in flight batches and bytes

## [0.27.9] - 2020-09-02

### CHANGED

- Fixed typo in logging of info lines

## [0.27.8] - 2020-09-02

### ADDED

- Publisher: log failed submission before retry

## [0.27.7] - 2020-08-29

### ADDED

- Log frame ids to make sequence of events visible

## [0.27.6] - 2020-08-28

### ADDED

- Log when dangerously old cursors are about to be committed

## [0.27.5] - 2020-08-26

### CHANGED

- Publisher: Tell why events were not retried on submission failure

## [0.27.4] - 2020-08-26

### CHANGED

- FIXED: Send remaining cursors to IO task on shutdown

## [0.27.3] - 2020-08-26

### CHANGED

- Split dispatch and io task in committer

### ADDED

- Metrics: Measure the effective ages of the first and last cursor before making a commit attempt
- Metrics: Emit a warning if the cursor age of the first cursor get close to the stream commit timeout

## [0.27.2] - 2020-08-25

### CHANGED

- Fixed tracking of active partitions

## [0.27.1] - 2020-08-24

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