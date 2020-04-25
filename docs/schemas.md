# Schemas

## AvroSchema

`AvroSchema` parses strings into avro schemas to assure validation. Properties:

`raw_schema`: The input string that will be parsed

`schema`: Result of parsing the raw_schema with `fastavro`

`flat_schena`: Parsed schema without `__fastavro_parsed` flag

`expanded_schema`: Parsed schema where all named types are expanded to their real schema

## SchemaVersion

`SchemaVersion` is a `namedtuple` that contains the `subject`, `schema_id`, `version` and `AvroSchema`.

The `SchemaVersion` is returned by `get_schema` and `check_version` client methods
