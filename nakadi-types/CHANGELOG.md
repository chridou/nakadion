# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [X]

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
