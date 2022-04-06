# Client

The `Schema Registry Client` consumes the API exposed by the `schema-registry` to operate resources that are `avro` and `json` schemas.

You probably won't use this but is good to know that exists. The `MessageSerializer` is whom interact with the `SchemaRegistryClient`

## SchemaRegistryClient

::: schema_registry.client.SchemaRegistryClient
    :docstring:

## Methods

### Get Schema

Get Schema for a given version. If version is `None`, try to resolve the latest schema

```python
def get_schema(subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> utils.SchemaVersion:
    """
    Args:
        subject (str): subject name
        version (int, optional): version id. If is None, the latest schema is returned
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

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
def get_by_id(schema_id: int, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]:
    """
    Args:
        schema_id (int): Schema Id
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]: Avro or JSON Record schema
    """
```

### Register a Schema

```python
def register(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT, schema_type: typing.Union["AVRO", "JSON"]) -> int:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        int: schema_id
    """
```

### Get Subjects

```python
def get_subjects(self, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> list:
    """
    GET /subjects/(string: subject)
    Get list of all registered subjects in your Schema Registry.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        list [str]: list of registered subjects.
    """
```

### Delete Schema

```python
def delete_subject(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> list:
    """
    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        list (int): versions of the schema deleted under this subject
    """
```

### Check if a schema has already been registered under the specified subject

```python
def check_version(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT, schema_type: typing.Union["AVRO", "JSON"]) -> dict:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
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
def get_versions(self, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> list:
    """
    GET subjects/{subject}/versions
    Get a list of versions registered under the specified subject.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        list (str): version of the schema registered under this subject
    """
```

### Deletes a specific version of the schema registered under a subject

```python
def delete_version(self, subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT):
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
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        int: version of the schema deleted
        None: If the subject or version does not exist.
    """
```

### Test Compatibility

```python
def test_compatibility(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT, schema_type: typing.Union["AVRO", "JSON"]):
    """
    By default the latest version is checked against.

    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        bool: True if schema given compatible, False otherwise
    """
```

### Get Compatibility

```python
def get_compatibility(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> str:
    """
    Get the current compatibility level for a subject.  Result will be one of:

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

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
def update_compatibility(level: str, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> bool:
    """
    Update the compatibility level for a subject.
    If subject is None, the compatibility level is global.

    Args:
        level (str): one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
            FULL, FULL_TRANSITIVE, NONE
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        bool: True if compatibility was updated

    Raises:
        ClientError: if the request was unsuccessful or an invalid
    """
```

## AsyncSchemaRegistryClient

::: schema_registry.client.AsyncSchemaRegistryClient
    :docstring:

## Methods

### Get Schema

Get Schema for a given version. If version is `None`, try to resolve the latest schema

```python
async def get_schema(subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> utils.SchemaVersion:
    """
    Args:
        subject (str): subject name
        version (int, optional): version id. If is None, the latest schema is returned
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

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
async def get_by_id(schema_id: int, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]:
    """
    Args:
        schema_id (int): Schema Id
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        typing.Union[client.schema.AvroSchema, client.schema.JsonSchema]: Avro or JSON Record schema
    """
```

### Register a Schema

```python
async def register(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema] headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT, schema_type: typing.Union["AVRO", "JSON"]) -> int:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        int: schema_id
    """
```

### Get Subjects

```python
async def get_subjects(self, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> list:
    """
    GET /subjects/(string: subject)
    Get list of all registered subjects in your Schema Registry.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        list [str]: list of registered subjects.
    """
```

### Delete Schema

```python
async def delete_subject(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> list:
    """
    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        list (int): versions of the schema deleted under this subject
    """
```

### Check if a schema has already been registered under the specified subject

```python
async def check_version(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT, schema_type: typing.Union["AVRO", "JSON"]) -> dict:
    """
    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
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
async def get_versions(self, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> list:
    """
    GET subjects/{subject}/versions
    Get a list of versions registered under the specified subject.

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        list (str): version of the schema registered under this subject
    """
```

### Deletes a specific version of the schema registered under a subject

```python
async def delete_version(self, subject: str, version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT):
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
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        int: version of the schema deleted
        None: If the subject or version does not exist.
    """
```

### Test Compatibility

```python
async def test_compatibility(subject: str, schema: typing.Union[client.schema.AvroSchema, client.schema.JsonSchema], version="latest", headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT, schema_type: typing.Union["AVRO", "JSON"]):
    """
    By default the latest version is checked against.

    Args:
        subject (str): subject name
        schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]: Avro or JSON schema to be registered
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
        schema_type typing.Union["AVRO", "JSON"]: The type of schema to parse if `schema` is a string. Default "AVRO"

    Returns:
        bool: True if schema given compatible, False otherwise
    """
```

### Get Compatibility

```python
async def get_compatibility(subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> str:
    """
    Get the current compatibility level for a subject.  Result will be one of:

    Args:
        subject (str): subject name
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

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
async def update_compatibility(level: str, subject: str, headers: dict = None, timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT) -> bool:
    """
    Update the compatibility level for a subject.
    If subject is None, the compatibility level is global.

    Args:
        level (str): one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
            FULL, FULL_TRANSITIVE, NONE
        headers (dict): Extra headers to add on the requests
        timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT

    Returns:
        bool: True if compatibility was updated

    Raises:
        ClientError: if the request was unsuccessful or an invalid
    """
```
