# Message Serializers

To serialize and deserialize messages you can use `AvroMessageSerializer` and `JsonMessageSerializer`. They interact with the `SchemaRegistryClient` to get `avro Schemas`  and `json schemas` in order to process messages.

*If you want to run the following examples run `docker-compose up` and the `schema registry server` will run on `http://127.0.0.1:8081`*

!!! warning
    The `AvroMessageSerializer` uses the same `protocol` as confluent, meaning that the event will contain the schema id in the payload.
    If you produce an event with the `AvroMessageSerializer` you have to consume it with the `AvroMessageSerializer` as well, otherwise you have to
    implement the parser on the consumer side.

::: schema_registry.serializers.AvroMessageSerializer
    :docstring:

## Usage for avro schemas

```python title="Trivial Usage with avro"
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

# We want to serialize the user_record with avro_user_schema
user_record = {
    "first_name": "my_first_name",
    "last_name": "my_last_name",
    "age": 20,
}

# Encode the record
message_serialized = avro_message_serializer.serialize(
    avro_user_schema, user_record, "user")

# this is because the message serialized reserved 5 bytes for the schema_id
assert len(message_serialized) > 5
assert isinstance(message_serialized, bytes)

# Decode the message
message_deserialized = avro_message_serializer.deserialize(message_serialized)
assert message_deserialized == user_record

# Now if we send a bad record
bad_record = {
    "first_name": "my_first_name",
    "last_name": "my_last_name",
    "age": "my_age"
}

avro_message_serializer.serialize(
     avro_user_schema, bad_record, "user")

# >>> TypeError: an integer is required on field age
```

*(This script is complete, it should run "as is")*

## Usage for json schemas

::: schema_registry.serializers.JsonMessageSerializer
    :docstring:

```python title="Trivial Usage with json schemas"
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

message_serialized = json_message_serializer.serialize(
    json_schema, basic_record, "basic")

# this is because the message encoded reserved 5 bytes for the schema_id
assert len(message_serialized) > 5
assert isinstance(message_serialized, bytes)

# Decode the message
message_deserialized = json_message_serializer.deserialize(message_encoded)
assert message_deserialized == basic_record
```

*(This script is complete, it should run "as is")*

## Async implementations

Please note that `JsonMessageSerializer`, `AvroMessageSerializer` and `SchemaRegistryClient` have their asynchronous
counterparts `AsyncJsonMessageSerializer`, `AsyncAvroMessageSerializer` and `AsyncSchemaRegistryClient` and all 
examples above should work if you replace them with their async variations

::: schema_registry.serializers.AsyncAvroMessageSerializer
    :docstring:

::: schema_registry.serializers.AsyncJsonMessageSerializer
    :docstring:

## Classes and Methods

```
AvroMessageSerializer
    Args:
        schemaregistry_client (schema_registry.client.SchemaRegistryClient): Http Client
        
JsonMessageSerializer
    Args:
        schemaregistry_client (schema_registry.client.SchemaRegistryClient): Http Client

AsyncAvroMessageSerializer
    Args:
        schemaregistry_client (schema_registry.client.AsyncSchemaRegistryClient): Http Client
        
AsyncJsonMessageSerializer
    Args:
        schemaregistry_client (schema_registry.client.AsyncSchemaRegistryClient): Http Client
```

### Serialize a record with a `Schema`

```python
def serialize(schema, record, subject):
    """
    Args:
        schema (avro.schema.RecordSchema): Avro Schema
        record (dict): An object to serialize
        subject (str): Subject name

    Returns:
        bytes: Encoded record with schema ID as bytes
    """
```

### Serialize a record with a `schema id`

```python
def serialize(schema_id, record):
    """
    Args:
        schema_id (int): integer ID
        record (dict): An object to serialize

    Returns:
        func: decoder function
    """
```

### Decode a message encoded previously

```python
def deserialize(message):
    """
    Args:
        message (str|bytes or None): message key or value to be decoded

    Returns:
        dict: Decoded message contents.
    """
```
