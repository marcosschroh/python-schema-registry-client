# Client

The `Schema Registry Client` consumes the API exposed by the `schema-registry` to operate resources that are `avro` schemas.

You probably won't use this but is good to know that exists. The `MessageSerialzer` is whom interact with the `SchemaRegistryClient`


SchemaRegistryClient:
---------------------

```python
SchemaRegistryClient
    A client that talks to a Schema Registry over HTTP

    def __init__(self, url, ca_location=None, cert_location=None, key_location=None, extra_headers=None)

    Args:
        url (str|dict) url: Url to schema registry or dictionary containing client configuration.
        ca_location (str): File or directory path to CA certificate(s) for verifying the Schema Registry key.
        cert_location (str): Path to public key used for authentication.
        key_location (str): Path to private key used for authentication.
        extra_headers (dict): Extra headers to add on every requests.
```

Methods:
--------

#### Get Schema

Get Schema for a given version. If version is `None`, try to resolve the latest schema

```python
def get_schema(subject, version="latest", headers=None):
    """
    Args:
        subject (str): subject name
        version (int, optional): version id. If is None, the latest schema is returned
        headers (dict): Extra headers to add on the requests

    Returns:
            SchemaVersion (nametupled): (subject, schema_id, schema, version)
    """
```

#### Get schema by `id`:

```python
def get_by_id(schema_id, headers=None):
    """
    Args:
        schema_id (int): Schema Id
        headers (dict): Extra headers to add on the requests

    Returns:
        avro.schema.RecordSchema: Avro Record schema
    """
```

Register a Schema:

```python
def register(subject, avro_schema, headers=None):
    """
    Args:
        subject (str): subject name
        avro_schema (avro.schema.RecordSchema): Avro schema to be registered
        headers (dict): Extra headers to add on the requests

    Returns:
        int: schema_id
    """
```

#### Delete Schema

```python
def delete_subject(subject, headers=None):
    """
    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests

    Returns:
        int: version of the schema deleted under this subject
    """
```

Check if a schema has already been registered under the specified subject:

```python
def check_version(subject, avro_schema, headers=None):
    """
    Args:
        subject (str): subject name
        avro_schema (avro.schema.RecordSchema): Avro schema
        headers (dict): Extra headers to add on the requests

    Returns:
        int: Schema version
        None: If schema not found.
    """
```

#### Test Compatibility:

```python
def test_compatibility(subject, avro_schema, version="latest", headers=None):
    """
    By default the latest version is checked against.

    Args:
        subject (str): subject name
        avro_schema (avro.schema.RecordSchema): Avro schema parsed
        headers (dict): Extra headers to add on the requests

    Returns:
        bool: True if compatible, False if not compatible
    """
```

#### Get Compatibility:

```python
def get_compatibility(subject, headers=None):
    """
    Get the current compatibility level for a subject.  Result will be one of:

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests

    Returns:
        str: one of 'NONE','FULL','FORWARD', or 'BACKWARD'

    Raises:
        ClientError: if the request was unsuccessful or an invalid
        compatibility level was returned
    """
```

#### Update Compatibility:

```python
def update_compatibility(level, subject, headers=None):
    """
    Update the compatibility level for a subject.

    Args:
        level (str): ex: 'NONE','FULL','FORWARD', or 'BACKWARD'
        headers (dict): Extra headers to add on the requests

    Returns:
        None
    """
```
