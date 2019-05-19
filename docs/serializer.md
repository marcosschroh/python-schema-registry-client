# Message Serializer

```python
MessageSerializer

A helper class that can serialize and deserialize messages

    Args:
        schemaregistry_client (schema_registry.client.SchemaRegistryClient): Http Client
```

Encode record with a `Schema`:

```python
encode_record_with_schema(topic, schema, record, is_key=False)
    Args:
        topic (str): Topic name
        schema (avro.schema.RecordSchema): Avro Schema
        record (dict): An object to serialize
        is_key (bool): If the record is a key

    Returns:
        bytes: Encoded record with schema ID as bytes
```

Encode a record with a `schema id`:

```python
encode_record_with_schema_id(schema_id, record, is_key=False):
    Args:
        schema_id (int): integer ID
        record (dict): An object to serialize
        is_key (bool): If the record is a key

    Returns:
        func: decoder function
```

Decode a message encoded previously:

```python
decode_message(message, is_key=False)
    Args:
        message (str|bytes or None): message key or value to be decoded

    Returns:
        dict: Decoded message contents.
```