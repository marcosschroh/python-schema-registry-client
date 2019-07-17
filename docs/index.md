# Python Rest Client Schema Registry

[![Build Status](https://travis-ci.org/marcosschroh/python-schema-registry-client.svg?branch=master)](https://travis-ci.org/marcosschroh/python-schema-registry-client)
[![GitHub license](https://img.shields.io/github/license/marcosschroh/python-schema-registry-client.svg)](https://github.com/marcosschroh/python-schema-registry-client/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/marcosschroh/python-schema-registry-client/branch/master/graph/badge.svg)](https://codecov.io/gh/marcosschroh/python-schema-registry-client)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)


Python Rest Client to interact against [schema-registry](https://docs.confluent.io/current/schema-registry/index.html) confluent server to manage [Avro Schemas](https://docs.oracle.com/database/nosql-12.1.3.1/GettingStartedGuide/avroschemas.html) resources.


## Requirements

python 3.6+, fastavro>=0.22.3, requests>=2.22.0

## Installation

```bash
pip install python-schema-registry-client
```

If you want the `Faust` functionality:

```
pip install python-schema-registry-client[faust]
```

## When use this library?

Usually, we have a situacion like this:

![Confluent Architecture](img/confluent_architecture.png)

So, our producers/consumers have to serialize/deserialize messages every time that they send/receive from Kafka topics. In this picture, we can imagine a `Faust` application receiving messages (encoded with an Avro schema) and we want to deserialize them, so we can ask the `schema server` to do that for us. In this scenario, the `MessageSerializer` is perfect.

Also, could be a use case that we would like to have an Application only to administrate `Avro Schemas` (register, update compatibilities, delete old schemas, etc.), so the `SchemaRegistryClient` is perfect.

## Run Tests

The tests are run against the `Schema Server` using `docker compose`, so you will need
`Docker` and `Docker Compose` installed.

```bash
./scripts/test.sh
```