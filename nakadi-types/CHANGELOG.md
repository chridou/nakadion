# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.27.9] - 2020-09-02

## [0.27.8] - 2020-09-02

## [0.27.7] - 2020-08-29

### ADDED

- Some documentation items

## [0.27.6] - 2020-08-28

### ADDED

- `SubscriptionCursor` can be turned into an `EventTypePartition`

## [0.27.5] - 2020-08-26

## [0.27.4] - 2020-08-26

## [0.27.3] - 2020-08-26

## [0.27.2] - 2020-08-25

## [0.27.1] - 2020-08-24

## FIXED

- Calculation of unconsumed events for a specific stream

## [0.27.0] - 2020-08-13

- Calculate unconsumed events for a specific stream

## [0.26.2] - 2020-06-05

## [0.26.1] - 2020-05-21

## [0.26.0]

### CHANGED

- Result for publishing is now an enum `SubmissionFailure`

### REMOVED

- `BatchResponse` in favour of `FailedSubmission` containing `SubmissionFailure`

## [0.25.4] - 2020-05-14

## [0.25.3] - 2020-05-14

## [0.25.2] - 2020-05-14

## [0.25.1] - 2020-05-14

### ADDED

- `EventTypeCursor` deserializes also with an embedded cursor object since clients were serializing like that

## [0.25.0] - 2020-05-14

## [0.24.3] - 2020-05-14

## [0.24.2] - 2020-05-13

## [0.24.1] - 2020-05-13

## [0.24.0] - 2020-05-13

## [0.23.3] - 2020-05-11

## [0.23.2] - 2020-05-11

## [0.23.1] - 2020-05-11

## [0.23.0] - 2020-05-11

## [0.22.9] - 2020-05-10

## [0.22.8] - 2020-05-08

## [0.22.7] - 2020-05-08

## [0.22.6] - 2020-05-07

## [0.22.5] - 2020-05-06

## [0.22.4] - 2020-05-06

## [0.22.3] - 2020-05-06

## [0.22.1] - 2020-05-05

### CHANGED

- fixed env var name documentation for `StreamParameters`

## [0.22.0] - 2020-04-28

### CHANGED

- unified env config funs and load without prefix
- functions which load from env with prefix do not add an underscore if prefix is empty

## [0.21.0] - 2020-04-28

### CHANGED

- renamed `SubscriptionCursorWithoutToken` to `EventTypeCursor` because the name was too long

### ADDED

- Default env name for types as constants
- functions to retrieve values from environment by default type name

### CHANGED

- Make error internally sync

## [0.20.3] - 2020-04-27

### CHANGED

- just version bump

## [0.20.2] - 2020-04-27

### CHANGED
- Improved documentation

## [0.20.1] - 2020-03-06

### CHANGED
- `SubscriptionStats` have `unconsumed_events` optional

### CHANGED
- moved `Partition` to module `partition
- added various convenience methods

## [0.1.0-alpha.X]

### Added
- This is a new crate
- Changes are not tracked during the alpha phase
