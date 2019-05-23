# Python Rest Client Schema Registry

[![Build Status](https://travis-ci.org/marcosschroh/python-schema-registry-client.svg?branch=master)](https://travis-ci.org/marcosschroh/python-schema-registry-client)
[![GitHub license](https://img.shields.io/github/license/marcosschroh/python-schema-registry-client.svg)](https://github.com/marcosschroh/python-schema-registry-client/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/marcosschroh/python-schema-registry-client/branch/master/graph/badge.svg)](https://codecov.io/gh/marcosschroh/python-schema-registry-client)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)


Python Rest Client to interact against [schema-registry](https://docs.confluent.io/current/schema-registry/index.html) confluent server to manage [Avro Schemas](https://docs.oracle.com/database/nosql-12.1.3.1/GettingStartedGuide/avroschemas.html) resources.

## Requirements

python 3.6+, avro-python3, fastavro, requests

## Installation

```
pip install python-schema-registry-client
```

## Client API, Serializer, Faust Integration and Schema Server description


**Documentation**: [https://marcosschroh.github.io/python-schema-registry-client.io](https://marcosschroh.github.io/python-schema-registry-client)

## Run Tests

The tests are run against the `Schema Server` using `docker compose`, so you will need
`Docker` and `Docker Compose` installed.

```bash
./scripts/test.sh
```
