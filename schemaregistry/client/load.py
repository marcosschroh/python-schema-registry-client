import sys

from .errors import ClientError


def loads(schema_str):
    """ Parse a schema given a schema string """
    try:
        if sys.version_info[0] < 3:
            return schema.parse(schema_str)
        else:
            return schema.Parse(schema_str)
    except schema.SchemaParseException as e:
        raise ClientError("Schema parse failed: %s" % (str(e)))


def load(fp):
    """ Parse a schema from a file path """
    with open(fp) as f:
        return loads(f.read())


# avro.schema.RecordSchema and avro.schema.PrimitiveSchema classes are not hashable. Hence defining them explicitly as
# a quick fix
def _hash_func(self):
    return hash(str(self))


try:
    from avro import schema

    schema.RecordSchema.__hash__ = _hash_func
    schema.PrimitiveSchema.__hash__ = _hash_func
    schema.UnionSchema.__hash__ = _hash_func

except ImportError:
    schema = None
