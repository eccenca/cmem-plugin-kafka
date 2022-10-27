# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Added

- Kafka exception handler to handle exceptions as follows:
  - On `auto.offset.reset` is none.
  - On failure connection or credentials.
- Specific error messages to constants.

### Changed

- Validate connection with exception handler.

## [1.1.0] 2022-10-21

### Added

- Kafka Consumer (Receive Messages) Plugin

### Fixed

- Documentation and help texts

### Changed

- upgrade to confluent-kafka 1.9.2

## [1.0.2] 2022-10-13

### Fixed

- use requests response stream for xml parsing (avoid out of memory)

## [1.0.1] 2022-08-26

### Fixed

- small docu issues

## [1.0.0] 2022-08-26

### Added

- initial version of a kafka producer plugin

