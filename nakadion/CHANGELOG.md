# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [X]

### CHANGED

- Connect retry keeps interval at 60s for approx 1 hour (incident recovery phase)

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