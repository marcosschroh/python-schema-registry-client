import json
import typing

import fastavro


class AvroSchema:
    def __init__(self, schema: str) -> None:
        if isinstance(schema, str):
            schema = json.loads(schema)
        self.raw_schema = schema
        self.schema = fastavro.parse_schema(schema, _force=True)
        self.generate_hash()

        self._flat_schema: typing.Optional[str] = None
        self._expanded_schema: typing.Optional[str] = None

    def generate_hash(self) -> None:
        self._hash = hash(json.dumps(self.schema))

    @property
    def name(self) -> str:
        return self.schema.get("name")

    @property
    def expanded_schema(self) -> str:
        """
        Returns a schema where all named types are expanded to their real schema

        Returns:
            expanced_schema (str): Schema parsed expanded
        """
        if self._expanded_schema is None:
            self._expanded_schema = fastavro.schema.expand_schema(self.raw_schema)
        return self._expanded_schema

    @property
    def flat_schema(self) -> str:
        """
        Parse the schema removing the fastavro write_hint flag __fastavro_parsed

        Returns:
            flat_schema (str): Schema parsed without the write hint
        """
        if self._flat_schema is None:
            self._flat_schema = fastavro.parse_schema(self.raw_schema, _write_hint=False, _force=True)

        return self._flat_schema

    def __hash__(self) -> int:
        return self._hash

    def __str__(self) -> str:
        return str(self.schema)

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, AvroSchema):
            return NotImplemented
        return self.__hash__() == other.__hash__()


def load(fp: str) -> AvroSchema:
    """Parse a schema from a file path"""
    with open(fp, mode="r") as f:
        content = f.read()
        return AvroSchema(content)
