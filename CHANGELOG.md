<!-- markdownlint-disable MD012 MD013 MD024 MD033 -->
# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [Unreleased]

### Changed

- Update dependencies and template
- validation of python 3.13 compatability
- python 3.13 needed

## [3.3.0] 2025-10-07

### Changed

- dependency upgrades

### Fixes

- linter fixes


## [3.2.0] 2024-06-07

### Added

- support for sending [tombstone message](https://medium.com/@damienthomlutz/deleting-records-in-kafka-aka-tombstones-651114655a16) from kafka producer plugin


## [3.1.0] 2023-12-30

### Changed

- Upgrade template to V6.0.1
- upgrades to use cmem-plugin-base 4.3.0 (for CMEM 23.3)
- SASL Password parameter type from StringParameterType to PasswordParameterType


## [3.0.0] 2023-09-08

### Fixes

- upload of file resources was broken for 23.2

### Changed

- upgrades to use cmem-plugin-base 4.1.0 (for CMEM 23.2, python 3.11)


## [2.0.0] 2023-08-01

### Added

- `Client_Id` parameter has been added to the advanced section of Kafka Consumer.
- When the `Client_Id` field is empty, the plugin defaults to `DNS:TASK ID`.
- `Local Consumer Queue Size` parameter to limit local consumer queue size
- Consume input entities in KafkaProducerPlugin to generate messages
- Generate output entities in KafkaConsumerPlugin from messages
- Consume JSON dataset in KafkaProducerPlugin to generate messages
- Generate JSON dataset in KafkaConsumerPlugin from messages
- `Compression Type` parameter has added to support standard compression
- `message limit` parameter has been added to the advanced section of kafka consumer
- `disable commit` parameter has been added to the advanced section of kafka consumer

### Changed

- `Messages Dataset` Parameter moved from the advanced section to Main section (will change again in future)
- Replace `none` with `error` in Auto Offset Reset parameter choice list
- moved kakfa producer/consumer configuration check from __init__ to validate() and calling explictly from execute()
- resolved endless loop caused by improper kafka server configuration
- generate entities with "message as plain value"


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

