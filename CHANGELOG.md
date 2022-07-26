# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Added

- `Client_Id` parameter has been added to the advanced section of Kafka Consumer.
- When the `Client_Id` field is empty, the plugin defaults to `DNS:TASK ID`.
- `Local Consumer Queue Size` parameter to limit local consumer queue size
### Changed

- `Messages Dataset` Parameter moved from the advanced section to Main section (will change again in future)
- Replace `none` with `error` in Auto Offset Reset parameter choice list
- moved kakfa producer/consumer configuration check from __init__ to validate() and calling explictly from execute()
- resolved endless loop caused by improper kafka server configuration
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

