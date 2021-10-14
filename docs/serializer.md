# Message Serializers

To serialize and deserialize messages you can use `AvroMessageSerializer` and `JsonMessageSerializer`. They interact with the `SchemaRegistryClient` to get `avro Schemas`  and `json schemas` in order to process messages.

*If you want to run the following examples run `docker-compose up` and the `schema registry server` will run on `http://127.0.0.1:8081`*

## Usage for avro schemas

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

# this is because the message encoded reserved 5 bytes for the schema_id
assert len(message_encoded) > 5
assert isinstance(message_encoded, bytes)

# Decode the message
message_decoded = avro_message_serializer.decode_message(message_encoded)
assert message_decoded == user_record

# Now if we send a bad record
bad_record = {
    "first_name": "my_first_name",
    "last_name": "my_last_name",
    "age": "my_age"
}

avro_message_serializer.encode_record_with_schema(
    "user", avro_user_schema, bad_record)

# >>> TypeError: an integer is required on field age
```

*(This script is complete, it should run "as is")*

## Usage for json schemas

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

# this is because the message encoded reserved 5 bytes for the schema_id
assert len(message_encoded) > 5
assert isinstance(message_encoded, bytes)

# Decode the message
message_decoded = json_message_serializer.decode_message(message_encoded)
assert message_decoded == basic_record
```

*(This script is complete, it should run "as is")*

## Class and Methods

```python
MessageSerializer
    Args:
        schemaregistry_client (schema_registry.client.SchemaRegistryClient): Http Client
```

### Encode record with a `Schema`

```python
def encode_record_with_schema(subject, schema, record):
    """
    Args:
        subject (str): Subject name
        schema (avro.schema.RecordSchema): Avro Schema
        record (dict): An object to serialize

    Returns:
        bytes: Encoded record with schema ID as bytes
    """
```

### Encode a record with a `schema id`

```python
def encode_record_with_schema_id(schema_id, record):
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
def decode_message(message):
    """
    Args:
        message (str|bytes or None): message key or value to be decoded

    Returns:
        dict: Decoded message contents.
    """
```
