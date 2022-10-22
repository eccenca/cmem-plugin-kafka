# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Added

- `Client_Id` parameter has been added to the advanced section of Kafka Consumer.
- When the `Client_Id` field is empty, the plugin defaults to `DNS:TASK ID`.

### Changed

- The string `CMEM_BASE_URI` has been made constant. As a result, updated `@needs-cmem`.

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

