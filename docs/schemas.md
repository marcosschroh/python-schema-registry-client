# Schemas

## BaseSchema

`BaseSchema` an abstract base class from which `AvroSchema` and `JsonSchema` inherit.
Requires concrete classes implement the following methods.

```python
@abstractmethod
def parse_schema(self, schema: typing.Dict) -> typing.Dict:
    pass

@staticmethod
@abstractmethod
def load(fp: str) -> BaseSchema:
    """Parse a schema from a file path"""
    pass

@staticmethod
@abstractmethod
async def async_load(fp: str) -> BaseSchema:
    """Parse a schema from a file path"""
    pass

@property
@abstractmethod
def name(self) -> typing.Optional[str]:
    pass

@property
@abstractmethod
def schema_type(self) -> str:
    pass
```

## AvroSchema

`AvroSchema` parses strings into avro schemas to assure validation. Properties:

`raw_schema`: The input string that will be parsed

`schema`: Result of parsing the raw_schema with `fastavro`

`flat_schema`: Parsed schema without `__fastavro_parsed` flag

`expanded_schema`: Parsed schema where all named types are expanded to their real schema

## JsonSchema

`JsonSchema` parses strings into json schemas to assure validation. Properties:

`raw_schema`: The input string that will be parsed

`schema`: Result of parsing the raw_schema with `jsonschema.Draft7Validator.check_schema`

## SchemaVersion

`SchemaVersion` is a `namedtuple` that contains the `subject`, `schema_id`, `version` and either `AvroSchema` or `JsonSchema`.

The `SchemaVersion` is returned by `get_schema` and `check_version` client methods
