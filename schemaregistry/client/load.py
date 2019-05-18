import aiofiles
from avro import schema

from .errors import ClientError


def loads(schema_str):
    """ Parse a schema given a schema string """
    try:
        return schema.Parse(schema_str)
    except schema.SchemaParseException as e:
        raise ClientError("Schema parse failed: %s" % (str(e)))


async def load(fp):
    """ Parse a schema from a file path """
    async with aiofiles.open(fp, mode='r') as f:
        content = await f.read()
        return loads(content)


# def _hash_func(self):
#     return hash(str(self))


# try:
#     from avro import schema

#     schema.RecordSchema.__hash__ = _hash_func
#     schema.PrimitiveSchema.__hash__ = _hash_func
#     schema.UnionSchema.__hash__ = _hash_func

# except ImportError:
#     schema = None
