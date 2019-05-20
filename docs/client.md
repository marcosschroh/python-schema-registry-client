# Client

The `Schema Registry Client` consumes the API exposed by the `schema-registry` to operate resources that are `avro` schemas.

You probably won't use this but is good to know that exists. The `MessageSerialzer` is whom interact with the `SchemaRegistryClient`


SchemaRegistryClient:
---------------------

```python
SchemaRegistryClient
    A client that talks to a Schema Registry over HTTP

    Args:
        url (str|dict) url: Url to schema registry or dictionary containing client configuration.
        ca_location (str): File or directory path to CA certificate(s) for verifying the Schema Registry key.
        cert_location (str): Path to client's public key used for authentication.
        key_location (str): Path to client's private key used for authentication.
```

Methods:
--------

#### Get Schema

```python
get_schema(subject, version="latest"):
    If the subject is not found a Nametupled (None,None,None) is returned.

    Args:
        subject (str): subject name
        version (int, optional): version id. If is None, the latest schema is returned

    Returns:
        SchemaVersion (nametupled): (subject, schema_id, schema, version)
```

#### Get schema by `id`:

```python
get_by_id(schema_id)
    Args:
        schema_id (int): Schema Id

    Returns:
        avro.schema.RecordSchema: Avro Record schema
```

Register a Schema:

```python
register(subject, avro_schema)
    Args:
        subject (str): subject name
        avro_schema (avro.schema.RecordSchema): Avro schema to be registered

    Returns:
        int: schema_id
```

#### Delete Schema

```python
delete_subject(subject)
    Args:
        subject (str): subject name

    Returns:
        int: version of the schema deleted under this subject
```

Check if a schema has already been registered under the specified subject:

```python
check_version(subject, avro_schema)
    Args:
        subject (str): subject name
        avro_schema (avro.schema.RecordSchema): Avro schema

    Returns:
        int: Schema version
        None: If schema not found.
```

#### Test Compatibility:

```python
test_compatibility(subject, avro_schema, version="latest")
    By default the latest version is checked against.

    Args:
        subject (str): subject name
        avro_schema (avro.schema.RecordSchema): Avro schema parsed

    Returns:
        bool: True if compatible, False if not compatible
```

#### Get Compatibility:

```python
get_compatibility(subject=None)
    Get the current compatibility level for a subject.  Result will be one of:

    Args:
        subject (str): subject name

    Returns:
        str: one of 'NONE','FULL','FORWARD', or 'BACKWARD'

    Raises:
        ClientError: if the request was unsuccessful or an invalid
        compatibility level was returned
```

#### Update Compatibility:

```python
update_compatibility(level, subjec)
    Update the compatibility level for a subject.

    Args:
        level (str): ex: 'NONE','FULL','FORWARD', or 'BACKWARD'

    Returns:
        None
```
