# Message Serializer

Class that serialize and deserialize messages. It interacts with the `SchemaRegistryClient` to get `Avro Schemas` in order to process messages. In your application you will intereact with it.


Usage:
------

```python
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import MessageSerializer

client = SchemaRegistryClient("http://127.0.0.1:8080")

message_serializer = MessageSerializer(client)

# Let's imagine that we have the foillowing schema.
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

message_encoded = message_serializer.encode_record_with_schema(
    "user", avro_user_schema, user_record)

# this is because the message encoded reserved 5 bytes for the schema_id
assert len(message_encoded) > 5
assert isinstance(message_encoded, bytes)

# now decode the message
message_decoded = message_serializer.decode_message(message_encoded)
assert message_decoded == user_record

# Now if we send a bad record
bad_record = {
    "first_name": "my_first_name",
    "last_name": "my_last_name",
    "age": "my_age"
}

message_serializer.encode_record_with_schema(
    "user", avro_user_schema, bad_record)
# results in an error:
#   TypeError: unsupported operand type(s) for <<: 'str' and 'int'
```

Class and Methods:
-----------------

```python
MessageSerializer
    Args:
        schemaregistry_client (schema_registry.client.SchemaRegistryClient): Http Client
```

#### Encode record with a `Schema`:

```python
def encode_record_with_schema(subject, schema, record, is_key=False):
    """
    Args:
        subject (str): Subject name
        schema (avro.schema.RecordSchema): Avro Schema
        record (dict): An object to serialize
        is_key (bool): If the record is a key

    Returns:
        bytes: Encoded record with schema ID as bytes
    """
```

#### Encode a record with a `schema id`:

```python
def encode_record_with_schema_id(schema_id, record, is_key=False):
    """
    Args:
        schema_id (int): integer ID
        record (dict): An object to serialize
        is_key (bool): If the record is a key

    Returns:
        func: decoder function
    """
```

#### Decode a message encoded previously:

```python
def decode_message(message, is_key=False):
    """
    Args:
        message (str|bytes or None): message key or value to be decoded

    Returns:
        dict: Decoded message contents.
    """
```