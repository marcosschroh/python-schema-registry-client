# Client

The `Schema Registry Client` consumes the API exposed by the `schema-registry` to operate resources that are `avro` and `json` schemas.

You probably won't use this but is good to know that exists. The `MessageSerializer` is whom interact with the `SchemaRegistryClient`

## SchemaRegistryClient

```python
SchemaRegistryClient
    A client that talks to a Schema Registry over HTTP

    def __init__(
        self, url, ca_location=None, cert_location=None, key_location=None, key_password=None, extra_headers=None,
        timeout=httpx._config.DEFAULT_TIMEOUT_CONFIG, pool_limits=httpx._config.DEFAULT_POOL_LIMITS

    Args:
        url (str|dict) url: Url to schema registry or dictionary containing client configuration.
        ca_location (str): File or directory path to CA certificate(s) for verifying the Schema Registry key.
        cert_location (str): Path to public key used for authentication.
        key_location (str): Path to private key used for authentication.
        key_password (str): Key password
        extra_headers (dict): Extra headers to add on every requests.
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests.
        pool_limits (httpx.Limits): The connection pool configuration to use when
            determining the maximum number of concurrently open HTTP connections.
```

## Methods

### Get Schema

Get Schema for a given version. If version is `None`, try to resolve the latest schema

```python
def get_schema(subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> utils.SchemaVersion:
    """
    Args:
        subject (str): subject name
        version (int, optional): version id. If is None, the latest schema is returned
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        utils.SchemaVersion (nametupled): (subject, schema_id, schema, version)

        None: If server returns a not success response:
            404: Schema not found
            422: Unprocessable entity
            ~ (200 - 299): Not success
    """
```

### Get schema by `id`

```python
def get_by_id(schema_id: int, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]:
    """
    Args:
        schema_id (int): Schema Id
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]: Avro or JSON Record schema
    """
```

### Register a Schema

```python
def register(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET, schema_type: typing.Union["AVRO", "JSON"]) -> int:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        int: schema_id
    """
```

### Get Subjects

```python
def get_subjects(self, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> list:
    """
    GET /subjects/(string: subject)
    Get list of all registered subjects in your Schema Registry.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        list [str]: list of registered subjects.
    """
```

### Delete Schema

```python
def delete_subject(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> list:
    """
    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        list (int): versions of the schema deleted under this subject
    """
```

### Check if a schema has already been registered under the specified subject

```python
def check_version(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET, schema_type: typing.Union["AVRO", "JSON"]) -> dict:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        dict:
            subject (string) -- Name of the subject that this schema is registered under
            id (int) -- Globally unique identifier of the schema
            version (int) -- Version of the returned schema
            schema (dict) -- The Avro or JSON schema

        None: If schema not found.
    """
```

### Get schema version under a specific subject

```python
def get_versions(self, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> list:
    """
    GET subjects/{subject}/versions
    Get a list of versions registered under the specified subject.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        list (str): version of the schema registered under this subject
    """
```

### Deletes a specific version of the schema registered under a subject

```python
def delete_version(self, subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET):
    """
    DELETE /subjects/(string: subject)/versions/(versionId: version)

    Deletes a specific version of the schema registered under this subject.
    This only deletes the version and the schema ID remains intact making
    it still possible to decode data using the schema ID.
    This API is recommended to be used only in development environments or
    under extreme circumstances where-in, its required to delete a previously
    registered schema for compatibility purposes or re-register previously registered schema.

    Args:
        subject (str): subject name
        version (str): Version of the schema to be deleted.
            Valid values for versionId are between [1,2^31-1] or the string "latest".
            "latest" deletes the last registered schema under the specified subject.
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        int: version of the schema deleted
        None: If the subject or version does not exist.
    """
```

### Test Compatibility

```python
def test_compatibility(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET, schema_type: typing.Union["AVRO", "JSON"]):
    """
    By default the latest version is checked against.

    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        bool: True if schema given compatible, False otherwise
    """
```

### Get Compatibility

```python
def get_compatibility(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> str:
    """
    Get the current compatibility level for a subject.  Result will be one of:

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        str: one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
            FULL, FULL_TRANSITIVE, NONE

    Raises:
        ClientError: if the request was unsuccessful or an invalid
        compatibility level was returned
    """
```

### Update Compatibility

```python
def update_compatibility(level: str, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> bool:
    """
    Update the compatibility level for a subject.
    If subject is None, the compatibility level is global.

    Args:
        level (str): one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
            FULL, FULL_TRANSITIVE, NONE
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        bool: True if compatibility was updated

    Raises:
        ClientError: if the request was unsuccessful or an invalid
    """
```

## AsyncSchemaRegistryClient

```python
AsyncSchemaRegistryClient
    A client that talks to a Schema Registry over HTTP

    def __init__(self, url, ca_location=None, cert_location=None, key_location=None, extra_headers=None)

    Args:
        url (str|dict) url: Url to schema registry or dictionary containing client configuration.
        ca_location (str): File or directory path to CA certificate(s) for verifying the Schema Registry key.
        cert_location (str): Path to public key used for authentication.
        key_location (str): Path to private key used for authentication.
        extra_headers (dict): Extra headers to add on every requests.
```

## Methods

### Get Schema

Get Schema for a given version. If version is `None`, try to resolve the latest schema

```python
async def get_schema(subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> utils.SchemaVersion:
    """
    Args:
        subject (str): subject name
        version (int, optional): version id. If is None, the latest schema is returned
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        utils.SchemaVersion (nametupled): (subject, schema_id, schema, version)

        None: If server returns a not success response:
            404: Schema not found
            422: Unprocessable entity
            ~ (200 - 299): Not success
    """
```

### Get schema by `id`

```python
async def get_by_id(schema_id: int, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]:
    """
    Args:
        schema_id (int): Schema Id
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]: Avro or JSON Record schema
    """
```

### Register a Schema

```python
async def register(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema] headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET, schema_type: typing.Union["AVRO", "JSON"]) -> int:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        int: schema_id
    """
```

### Get Subjects

```python
async def get_subjects(self, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> list:
    """
    GET /subjects/(string: subject)
    Get list of all registered subjects in your Schema Registry.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        list [str]: list of registered subjects.
    """
```

### Delete Schema

```python
async def delete_subject(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> list:
    """
    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        list (int): versions of the schema deleted under this subject
    """
```

### Check if a schema has already been registered under the specified subject

```python
async def check_version(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET, schema_type: typing.Union["AVRO", "JSON"]) -> dict:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        dict:
            subject (string) -- Name of the subject that this schema is registered under
            id (int) -- Globally unique identifier of the schema
            version (int) -- Version of the returned schema
            schema (dict) -- The Avro or JSON schema

        None: If schema not found.
    """
```

### Get schema version under a specific subject

```python
async def get_versions(self, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> list:
    """
    GET subjects/{subject}/versions
    Get a list of versions registered under the specified subject.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        list (str): version of the schema registered under this subject
    """
```

### Deletes a specific version of the schema registered under a subject

```python
async def delete_version(self, subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET):
    """
    DELETE /subjects/(string: subject)/versions/(versionId: version)

    Deletes a specific version of the schema registered under this subject.
    This only deletes the version and the schema ID remains intact making
    it still possible to decode data using the schema ID.
    This API is recommended to be used only in development environments or
    under extreme circumstances where-in, its required to delete a previously
    registered schema for compatibility purposes or re-register previously registered schema.

    Args:
        subject (str): subject name
        version (str): Version of the schema to be deleted.
            Valid values for versionId are between [1,2^31-1] or the string "latest".
            "latest" deletes the last registered schema under the specified subject.
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        int: version of the schema deleted
        None: If the subject or version does not exist.
    """
```

### Test Compatibility

```python
async def test_compatibility(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET, schema_type: typing.Union["AVRO", "JSON"]):
    """
    By default the latest version is checked against.

    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        bool: True if schema given compatible, False otherwise
    """
```

### Get Compatibility

```python
async def get_compatibility(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> str:
    """
    Get the current compatibility level for a subject.  Result will be one of:

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        str: one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
            FULL, FULL_TRANSITIVE, NONE

    Raises:
        ClientError: if the request was unsuccessful or an invalid
        compatibility level was returned
    """
```

### Update Compatibility

```python
async def update_compatibility(level: str, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET) -> bool:
    """
    Update the compatibility level for a subject.
    If subject is None, the compatibility level is global.

    Args:
        level (str): one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
            FULL, FULL_TRANSITIVE, NONE
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default UNSET

    Returns:
        bool: True if compatibility was updated

    Raises:
        ClientError: if the request was unsuccessful or an invalid
    """
```
