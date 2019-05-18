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
