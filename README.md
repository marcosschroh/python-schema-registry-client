# Python Rest Client Schema Registry

[![Build Status](https://travis-ci.org/marcosschroh/python-schema-registry-client.svg?branch=master)](https://travis-ci.org/marcosschroh/python-schema-registry-client)
[![GitHub license](https://img.shields.io/github/license/marcosschroh/python-schema-registry-client.svg)](https://github.com/marcosschroh/python-schema-registry-client/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/marcosschroh/python-schema-registry-client/branch/master/graph/badge.svg)](https://codecov.io/gh/marcosschroh/python-schema-registry-client)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)

Python Rest Client to interact against [schema-registry](https://docs.confluent.io/current/schema-registry/index.html) confluent server to manage [Avro Schemas](https://docs.oracle.com/database/nosql-12.1.3.1/GettingStartedGuide/avroschemas.html) resources.

## Requirements

python 3.6+

## Installation

```bash
pip install python-schema-registry-client
```

If you want the `Faust` functionality:

```bash
pip install python-schema-registry-client[faust]
```

Note that this will automatically add a dependency on the [faust-streaming](https://github.com/faust-streaming/faust) fork of faust. If you want to use the
old faust version, simply install it manually and then install `python-schema-registry-client` without the `faust` extra enabled, the functionality will
be the same.

## Client API, Serializer, Faust Integration and Schema Server description

**Documentation**: [https://marcosschroh.github.io/python-schema-registry-client.io](https://marcosschroh.github.io/python-schema-registry-client)

## Usage

```python
from schema_registry.client import SchemaRegistryClient, schema

client = SchemaRegistryClient(url="http://127.0.0.1:8081")

deployment_schema = {
    "type": "record",
    "namespace": "com.kubertenes",
    "name": "AvroDeployment",
    "fields": [
        {"name": "image", "type": "string"},
        {"name": "replicas", "type": "int"},
        {"name": "port", "type": "int"},
    ],
}

avro_schema = schema.AvroSchema(deployment_schema)

schema_id = client.register("test-deployment", avro_schema)
```

or async

```python
from schema_registry.client import AsyncSchemaRegistryClient, schema

async_client = AsyncSchemaRegistryClient(url="http://127.0.0.1:8081")

deployment_schema = {
    "type": "record",
    "namespace": "com.kubertenes",
    "name": "AvroDeployment",
    "fields": [
        {"name": "image", "type": "string"},
        {"name": "replicas", "type": "int"},
        {"name": "port", "type": "int"},
    ],
}

avro_schema = schema.AvroSchema(deployment_schema)

schema_id = await async_client.register("test-deployment", avro_schema)
```

## Usage with dataclasses-avroschema

You can generate the `avro schema` directely from a python class using [dataclasses-avroschema](https://github.com/marcosschroh/dataclasses-avroschema)
and use it in the API for `register schemas`, `check versions` and `test compatibility`:

```python
import dataclasses

from dataclasses_avroschema import AvroModel, types

from schema_registry.client import SchemaRegistryClient

client = SchemaRegistryClient(url="http://127.0.0.1:8081")


@dataclasses.dataclass
class UserAdvance(AvroModel):
    name: str
    age: int
    pets: typing.List[str] = dataclasses.field(default_factory=lambda: ["dog", "cat"])
    accounts: typing.Dict[str, int] = dataclasses.field(default_factory=lambda: {"key": 1})
    has_car: bool = False
    favorite_colors: types.Enum = types.Enum(["BLUE", "YELLOW", "GREEN"], default="BLUE")
    country: str = "Argentina"
    address: str = None

# register the schema
schema_id = client.register(subject, UserAdvance.avro_schema())

print(schema_id)
# >>> 12

result = client.check_version(subject, UserAdvance.avro_schema())
print(result)
# >>> SchemaVersion(subject='dataclasses-avroschema-subject-2', schema_id=12, schema=1, version={"type":"record" ...')

compatibility = client.test_compatibility(subject, UserAdvance.avro_schema())
print(compatibility)

# >>> True
```

## When use this library

Usually, we have a situacion like this:

![Confluent Architecture](docs/img/confluent_architecture.png)

So, our producers/consumers have to serialize/deserialize messages every time that they send/receive from Kafka topics. In this picture, we can imagine a `Faust` application receiving messages (encoded with an Avro schema) and we want to deserialize them, so we can ask the `schema server` to do that for us. In this scenario, the `MessageSerializer` is perfect.

Also, could be a use case that we would like to have an Application only to administrate `Avro Schemas` (register, update compatibilities, delete old schemas, etc.), so the `SchemaRegistryClient` is perfect.

## Development

Install the project and development utilities in edit mode:

```bash
pip3 install -e ".[tests,docs,faust]
```

The tests are run against the `Schema Server` using `docker compose`, so you will need
`Docker` and `Docker Compose` installed.

```bash
./scripts/test
```

Run code linting:

```bash
./scripts/lint
```

To perform tests using the python shell you can execute `docker-compose up` and the `schema registry server` will run on `http://127.0.0.1:8081`, the you can interact against it using the `SchemaRegistryClient`:

```python
from schema_registry.client import SchemaRegistryClient, schema

client = SchemaRegistryClient(url="http://127.0.0.1:8081")

# do some operations with the client...
```
