from __future__ import annotations
from abc import ABC, abstractmethod

import json
import typing

import aiofiles
import fastavro
import jsonschema


class BaseSchema(ABC):
    def __init__(self, schema: typing.Union[str, typing.Dict]) -> None:
        if isinstance(schema, str):
            schema = json.loads(schema)
        self.raw_schema = schema
        self.schema = self.parse_schema(typing.cast(typing.Dict, schema))
        self.generate_hash()

        self._flat_schema: typing.Optional[str] = None
        self._expanded_schema: typing.Optional[str] = None

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

    def generate_hash(self) -> None:
        self._hash = hash(json.dumps(self.schema))

    def __hash__(self) -> int:
        return self._hash

    def __str__(self) -> str:
        return str(self.schema)

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, BaseSchema):
            return NotImplemented
        return self.__hash__() == other.__hash__()


class AvroSchema(BaseSchema):
    @property
    def name(self) -> typing.Optional[str]:
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

    def parse_schema(self, schema: typing.Dict) -> typing.Dict:
        return fastavro.parse_schema(schema, _force=True)

    @staticmethod
    def load(fp: str) -> AvroSchema:
        """Parse an avro schema from a file path"""
        with open(fp, mode="r") as f:
            content = f.read()
            return AvroSchema(content)

    @staticmethod
    async def async_load(fp: str) -> AvroSchema:
        """Parse an avro schema from a file path"""
        async with aiofiles.open(fp, mode="r") as f:
            content = await f.read()
            return AvroSchema(content)


class JsonSchema(BaseSchema):
    def parse_schema(self, schema: typing.Dict) -> typing.Dict:
        jsonschema.Draft7Validator.check_schema(schema)
        return schema

    @staticmethod
    def load(fp: str) -> BaseSchema:
        """Parse a json schema from a file path"""
        with open(fp, mode="r") as f:
            content = f.read()
            return JsonSchema(content)

    @staticmethod
    async def async_load(fp: str) -> BaseSchema:
        """Parse a json schema from a file path"""
        async with aiofiles.open(fp, mode="r") as f:
            content = await f.read()
            return JsonSchema(content)
