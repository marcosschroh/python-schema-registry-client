import json
import fastavro


class AvroSchema:
    def __init__(self, schema):
        if isinstance(schema, str):
            schema = json.loads(schema)
        self.schema = fastavro.parse_schema(schema)
        self.generate_hash()

    def generate_hash(self):
        self._hash = hash(json.dumps(self.schema))

    @property
    def name(self):
        return self.schema.get("name")

    def __hash__(self):
        return self._hash

    def __str__(self):
        return str(self.schema)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


def load(fp):
    """Parse a schema from a file path"""
    with open(fp, mode="r") as f:
        content = f.read()
        return AvroSchema(content)
