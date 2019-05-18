from avro import schema

from .errors import ClientError


def loads(schema_str):
    """ Parse a schema given a schema string """
    try:
        return schema.Parse(schema_str)
    except schema.SchemaParseException as e:
        raise ClientError(f"Schema parse failed: {e}")


def load(fp):
    """ Parse a schema from a file path """
    with open(fp, mode="r") as f:
        content = f.read()
        return loads(content)
