# Schema Registry Client

The `Schema Registry Client` consumes the API exposed by the `schema-registry` to operate resources that are `avro` and `json` schemas.

You probably won't use this but is good to know that exists. The `MessageSerializer` is whom interact with the `SchemaRegistryClient`

::: schema_registry.client.SchemaRegistryClient
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false

::: schema_registry.client.AsyncSchemaRegistryClient
    options:
        show_root_heading: true
        docstring_section_style: table
        show_signature_annotations: false

## Auth

Credentials can be supplied in `two` different ways: using the `url` or the `schema_registry.client.Auth`.

```python title="Credentials using Auth"
from schema_registry.client import SchemaRegistryClient, Auth

SchemaRegistryClient(
    url="https://user_url:secret_url@127.0.0.1:65534",
    auth=Auth(username="secret-user", password="secret"),
)
```

```python title="Credentials using the url"
from schema_registry.client import SchemaRegistryClient

username="secret-username"
password="secret"

SchemaRegistryClient({"url": f"https://{username}:{password}@127.0.0.1:65534"})
```

!!! note
    This auth methods are the same for `AsyncSchemaRegistryClient`
