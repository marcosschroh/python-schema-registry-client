# Python Rest Client Schema Registry

[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fmarcosschroh%2Fpython-schema-registry-client%2Fbadge%3Fref%3Dmaster&style=flat)](https://actions-badge.atrox.dev/marcosschroh/python-schema-registry-client/goto?ref=master)
[![GitHub license](https://img.shields.io/github/license/marcosschroh/python-schema-registry-client.svg)](https://github.com/marcosschroh/python-schema-registry-client/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/marcosschroh/python-schema-registry-client/branch/master/graph/badge.svg)](https://codecov.io/gh/marcosschroh/python-schema-registry-client)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)

Python Rest Client to interact against [schema-registry](https://docs.confluent.io/current/schema-registry/index.html) confluent server to manage [Avro](https://docs.oracle.com/database/nosql-12.1.3.1/GettingStartedGuide/avroschemas.html) and [JSON](https://json-schema.org/) schemas resources.

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

## Avro Schema Usage

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

## JSON Schema Usage

```python
from schema_registry.client import SchemaRegistryClient, schema

client = SchemaRegistryClient(url="http://127.0.0.1:8081")

deployment_schema = {
    "definitions" : {
        "JsonDeployment" : {
            "type" : "object",
            "required" : ["image", "replicas", "port"],
            "properties" : {
                "image" :       {"type" : "string"},
                "replicas" :    {"type" : "integer"},
                "port" :        {"type" : "integer"}
            }
        }
    },
    "$ref" : "#/definitions/JsonDeployment"
}

json_schema = schema.JsonSchema(deployment_schema)

schema_id = client.register("test-deployment", json_schema)
```

or async

```python
from schema_registry.client import AsyncSchemaRegistryClient, schema

async_client = AsyncSchemaRegistryClient(url="http://127.0.0.1:8081")

deployment_schema = {
    "definitions" : {
        "JsonDeployment" : {
            "type" : "object",
            "required" : ["image", "replicas", "port"],
            "properties" : {
                "image" :       {"type" : "string"},
                "replicas" :    {"type" : "integer"},
                "port" :        {"type" : "integer"}
            }
        }
    },
    "$ref" : "#/definitions/JsonDeployment"
}

json_schema = schema.JsonSchema(deployment_schema)

schema_id = await async_client.register("test-deployment", json_schema)
```

## Usage with dataclasses-avroschema for avro schemas

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

### Usage with pydantic for json schemas
You can generate the json schema directely from a python class using pydantic and use it in the API for register schemas, check versions and test compatibility:

```python
import typing

from enum import Enum

from pydantic import BaseModel

from schema_registry.client import SchemaRegistryClient

client = SchemaRegistryClient(url="http://127.0.0.1:8081")

class ColorEnum(str, Enum):
  BLUE = "BLUE"
  YELLOW = "YELLOW"
  GREEN = "GREEN"


class UserAdvance(BaseModel):
    name: str
    age: int
    pets: typing.List[str] = ["dog", "cat"]
    accounts: typing.Dict[str, int] = {"key": 1}
    has_car: bool = False
    favorite_colors: ColorEnum = ColorEnum.BLUE
    country: str = "Argentina"
    address: str = None

# register the schema
schema_id = client.register(subject, UserAdvance.schema_json(), schema_type="JSON")

print(schema_id)
# >>> 12

result = client.check_version(subject, UserAdvance.schema_json(), schema_type="JSON")
print(result)
# >>> SchemaVersion(subject='pydantic-jsonschema-subject', schema_id=12, schema=1, version=<schema_registry.client.schema.JsonSchema object at 0x7f40354550a0>)

compatibility = client.test_compatibility(subject, UserAdvance.schema_json(), schema_type="JSON")
print(compatibility)

# >>> True
```

## When use this library

Usually, we have a situacion like this:

![Confluent Architecture](img/confluent_architecture.png)

So, our producers/consumers have to serialize/deserialize messages every time that they send/receive from Kafka topics. In this picture, we can imagine a `Faust` application receiving messages (encoded with an Avro schema) and we want to deserialize them, so we can ask the `schema server` to do that for us. In this scenario, the `MessageSerializer` is perfect.

Also, could be a use case that we would like to have an Application only to administrate `Avro Schemas` (register, update compatibilities, delete old schemas, etc.), so the `SchemaRegistryClient` is perfect.

## Development

The tests are run against the `Schema Server` using `docker compose`, so you will need
`Docker` and `Docker Compose` installed.

```bash
./scripts/test
```

Run code linting:

```bash
./scripts/lint

To perform tests using the python shell you can execute `docker-compose up` and the `schema registry server` will run on `http://127.0.0.1:8081`, the you can interact against it using the `SchemaRegistryClient`:

```python
from schema_registry.client import SchemaRegistryClient, schema

client = SchemaRegistryClient(url="http://127.0.0.1:8081")

# do some operations with the client...
```
