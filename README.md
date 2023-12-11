# Python Rest Client Schema Registry

[![Python package](https://github.com/marcosschroh/python-schema-registry-client/actions/workflows/python-package.yml/badge.svg)](https://github.com/marcosschroh/python-schema-registry-client/actions/workflows/python-package.yml)
[![GitHub license](https://img.shields.io/github/license/marcosschroh/python-schema-registry-client.svg)](https://github.com/marcosschroh/python-schema-registry-client/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/marcosschroh/python-schema-registry-client/branch/master/graph/badge.svg)](https://codecov.io/gh/marcosschroh/python-schema-registry-client)
[![Python Version](https://img.shields.io/badge/python-3.7+-blue.svg)](https://img.shields.io/badge/python-3.7+-blue.svg)

Python Rest Client to interact against [schema-registry](https://docs.confluent.io/current/schema-registry/index.html) confluent server to manage [Avro](https://docs.oracle.com/database/nosql-12.1.3.1/GettingStartedGuide/avroschemas.html) and [JSON](https://json-schema.org/) schemas resources.

## Requirements

python 3.8+

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

## Serializers

You can use `AvroMessageSerializer` to encode/decode messages in `avro`

```python
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import AvroMessageSerializer


client = SchemaRegistryClient("http://127.0.0.1:8081")
avro_message_serializer = AvroMessageSerializer(client)

avro_user_schema = schema.AvroSchema({
    "type": "record",
    "namespace": "com.example",
    "name": "AvroUsers",
    "fields": [
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "age", "type": "int"},

    ],
})

# We want to encode the user_record with avro_user_schema
user_record = {
    "first_name": "my_first_name",
    "last_name": "my_last_name",
    "age": 20,
}

# Encode the record
message_encoded = avro_message_serializer.encode_record_with_schema(
    "user", avro_user_schema, user_record)

print(message_encoded)
# >>> b'\x00\x00\x00\x00\x01\x1amy_first_name\x18my_last_name('
```

or with `json schemas`

```python
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import JsonMessageSerializer


client = SchemaRegistryClient("http://127.0.0.1:8081")
json_message_serializer = JsonMessageSerializer(client)

json_schema = schema.JsonSchema({
  "definitions" : {
    "record:python.test.basic.basic" : {
      "description" : "basic schema for tests",
      "type" : "object",
      "required" : [ "number", "name" ],
      "properties" : {
        "number" : {
          "oneOf" : [ {
            "type" : "integer"
          }, {
            "type" : "null"
          } ]
        },
        "name" : {
          "oneOf" : [ {
            "type" : "string"
          } ]
        }
      }
    }
  },
  "$ref" : "#/definitions/record:python.test.basic.basic"
})

# Encode the record
basic_record = {
    "number": 10,
    "name": "a_name",
}

message_encoded = json_message_serializer.encode_record_with_schema(
    "basic", json_schema, basic_record)

print(message_encoded)
# >>> b'\x00\x00\x00\x00\x02{"number": 10, "name": "a_name"}'
```

## When use this library

Usually, we have a situation like this:

![Confluent Architecture](docs/img/confluent_architecture.png)

So, our producers/consumers have to serialize/deserialize messages every time that they send/receive from Kafka topics. In this picture, we can imagine a `Faust` application receiving messages (encoded with an Avro schema) and we want to deserialize them, so we can ask the `schema server` to do that for us. In this scenario, the `MessageSerializer` is perfect.

Also, could be a use case that we would like to have an Application only to administrate `Avro Schemas` (register, update compatibilities, delete old schemas, etc.), so the `SchemaRegistryClient` is perfect.

## Development

[Poetry](https://python-poetry.org/docs/) is needed to install the dependencies and develope locally

1. Install dependencies: `poetry install --all-extras`
2. Code linting: `./scripts/format`
3. Run tests: `./scripts/test`

For commit messages we use [commitizen](https://commitizen-tools.github.io/commitizen/) in order to standardize a way of committing rules

*Note*: The tests are run against the `Schema Server` using `docker compose`, so you will need
`Docker` and `Docker Compose` installed.

In a terminal run `docker-compose up`. Then in a different terminal run the tests:

```bash
./scripts/test
```

All additional args will be passed to pytest, for example:

```bash
./scripts/test ./tests/client/
```

### Tests usind the python shell

To perform tests using the python shell you can run the project using `docker-compose`.

1. Execute `docker-compose up`. Then, the `schema registry server` will run on `http://127.0.0.1:8081`, then you can interact against it using the `SchemaRegistryClient`:
1. Use the python interpreter (get a python shell typing `python` in your command line)
1. Play with the `schema server`

```python
from schema_registry.client import SchemaRegistryClient, schema

client = SchemaRegistryClient(url="http://127.0.0.1:8081")

# do some operations with the client...
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
client.register("test-deployment", avro_schema)
# >>>> Out[5]: 1
```

Then, you can check the schema using your browser going to the url `http://127.0.0.1:8081/schemas/ids/1`
