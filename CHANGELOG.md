# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.8.0] - 2020-01-29

### Added

- support return_record_name [#89](https://github.com/marcosschroh/python-schema-registry-client/pull/89)
- Update pinned fastavro version to match dataclasses-avroschema [#91](https://github.com/marcosschroh/python-schema-registry-client/pull/91)

## [1.7.2] - 2020-12-22

### Fixed

- Checks if Schema is already registered before trying to register. This allows
  Schema Registry to be readonly in production environment, with only CI/CD
  being allowed to make changes.

## [1.7.1] - 2020-12-07

### Fixed

- [faust] extra now depends on [faust-streaming fork](https://github.com/faust-streaming/faust)

## [1.7.0] - 2020-10-17

### Added

- Integration with [dataclasses-avroschema](https://github.com/marcosschroh/dataclasses-avroschema) added to serializers

### Fixed

- Requirements updated: `fastavro==1.0.0.post1` and `mypy==0.782`

## [1.6.1] - 2020-10-16

### Fixed

- Requirements updated: `fastavro==1.0.0.post1` and `mypy==0.782`

## [1.6.0] - 2020-09-18

### Added

- Integration with [dataclasses-avroschema](https://github.com/marcosschroh/dataclasses-avroschema) added

## [1.5.0] - 2020-09-12

### Added

- `AsyncSchemaRegistryClient` added

## [1.4.7] - 2020-09-12

### Fixed

- Submit raw schema instead of the `fastavro-parsed` version [#77](https://github.com/marcosschroh/python-schema-registry-client/issues/77)

## [1.4.6] - 2020-09-07

### Fixed

- `is_key` removed from signature methods
- documentation updated

## [1.4.5] - 2020-08-19

### Fixed

- Pin dependency versions

## [1.4.4] - 2020-08-14

### Fixed

- Corrects `Accept headers` to conform to specification [#73](https://github.com/marcosschroh/python-schema-registry-client/pull/73)

## [1.4.3] - 2020-08-13

### Fixed

- `requests` dependency removed [#70](https://github.com/marcosschroh/python-schema-registry-client/pull/70)

## [1.4.2] - 2020-08-10

### Fixed

- Fix `client.register cache lookup` [#62](https://github.com/marcosschroh/python-schema-registry-client/pull/62)
- Support for new release of `httpx`. For `httpx < 0.14.0` versions usage of `python-schema-registry-client==1.4.1`
- Don't rely on httpx's private config values [#66](https://github.com/marcosschroh/python-schema-registry-client/pull/66)

## [1.4.1] - 2020-07-14

### Changed

- `Avro` serialization for complex `faust` records that contains nested `records`, `Mapping` or `Sequences fixed` [#59](https://github.com/marcosschroh/python-schema-registry-client/issues/59)

## [1.4.0] - 2020-05-07

### Added

- timeout and pool_limits added to client

## [1.3.2] - 2020-05-06

### Fixed

- Allow SchemaRegistryClient to be picklable fixed #24

## [1.3.1] - 2020-05-03

### Changed

- `requests` library has been replaced with `httpx`

## [1.3.0] - 2020-04-25

### Added

- new properties added to AvroSchema: raw_schema, flat_schema and expanded_schema
- documentation updated

### Fixed

## [1.2.10] - 2020-04-20

- bad import check fixed causing faust crash

### Fixed

## [1.2.9] - 2020-04-20

- fixed `Importing MessageSerializer` without Faust is Broken [#47](https://github.com/marcosschroh/python-schema-registry-client/issues/47)

### Fixed

## [1.2.8] - 2020-04-18

- fix Base URL was overwritten [#46](https://github.com/marcosschroh/python-schema-registry-client/issues/46)

### Changed

## [1.2.7] - 2020-03-29

- `faust serializer` updated in order to be compatible with latest Faust version

### Fixed

## [1.2.6] - 2020-03-13

- Incorrect message on get_subjects fixed

### Changed

## [1.2.5] - 2020-02-19

- is_key was removed from serializer, meaning that the subject itself will have to express wheter the schema is key or not. Related to [#40](https://github.com/marcosschroh/python-schema-registry-client/issues/40)
- Requirements updated to latest versions: fastavro and requests

### Changed

## [1.2.4] - 2019-11-16

- Faust requirement updated to <= 1.9.0

### Fixed

## [1.2.3] - 2019-11-02

- fix force `fastavro` to parse always the schemas in order to avoid errors when a process B get the schema from the server that was previously processed by process A.

### Changed

## [1.2.2] - 2019-10-26

- requirements updated

### Changed

## [1.2.1] - 2019-07-23

- Typing added (mypy)

### Added

## [1.2.0] - 2019-07-22

- Missing endpoints added:
  - GET  /subjects
  - GET /subjects/{subject}/versions
  - DELETE /subjects/{subject}/versions/{version}

### Added

## [1.1.0] - 2019-07-19

- Urls manager added to have more control over which endpoint the client is using

### Added

## [1.0.0] - 2019-07-17

- Production ready
- Move to FastAvro
- Dependency `avro-python3` removed
- Support for `logicalTypes` added
- `AvroSchema` class added to manage schema parse and hasing
- Tests added
- Faker lib added to create fixtures
- Documentation updated

### Fixed

## [0.3.1] - 2019-07-17

- Error mapping proxy fixed when try to register a schema that contains `logicalTypes`

### Added

## [0.3.0] - 2019-07-11

- Faust Serializer has been added
- Optional Faust requirement added

### Changed

## [0.2.5] - 2019-06-10

- Documentation about `MessageSerializer` and `Faust` Integration updated

### Changed

## [0.2.4] - 2019-06-05

- Missing Compatibility levels added.
- `ClienError` added to the documentation
- Tests refactored

### Fixed

## [0.2.3] - 2019-05-31

- HTTP code and `server_traceback` added to the `ClientError` when Schema Server returns a not success when a schema compatibility check is requested.

### Fixed

## [0.2.2] - 2019-05-29

- Missing tests added.
- Bug in register fixed.

### Changed

## [0.2.1] - 2019-05-27

- Documentation updated.
- Missing Python syntax highlighted added.

### Added

## [0.2.0] - 2019-05-23

- Now all the tests are run against a `Schema Registry Server` and not a mock server. This allows us to be aware of when the server changes. The requirements to run the tests are Docker and `Docker Compose`

### Changed

## [0.1.1] - 2019-05-22

- Http `Client` now is a subclass of `request.Session`

### Added

## [0.1.0] - 2019-05-22

- Now is possible to inisialize SchemaRegistryClient with custom headers. This headers will be included on every requests.

### Changed

## [0.0.3] - 2019-05-22

- small twaeks

### Changed

## [0.0.2] - 2019-05-19

- First release
- Http `Client` added.
- `MessageSerializer` added.
- tests added
- Documentation added
