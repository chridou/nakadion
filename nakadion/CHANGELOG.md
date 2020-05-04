# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.22.1] - 2020-05-05

### CHANGED

- Fixed event type partition tracking
- Improve logging format

### ADDED

- metric for `cursor_received_at` in committer
- `WarnNoFramesSecs` time to emit a warning if no frames were received from Nakadi
- `WarnNoEventsSecs` time to emit a warning if no events were received from Nakadi

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