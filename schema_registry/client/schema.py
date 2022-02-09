from __future__ import annotations

import json
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass

import aiofiles
import fastavro
import jsonschema

from schema_registry.client.utils import AVRO_SCHEMA_TYPE, JSON_SCHEMA_TYPE


class BaseSchema(ABC):
    def __init__(self, schema: typing.Union[str, typing.Dict]) -> None:
        if isinstance(schema, str):
            schema = json.loads(schema)
        self.raw_schema = schema
        self.schema = self.parse_schema(typing.cast(typing.Dict, schema))
        self.generate_hash()

    @abstractmethod
    def parse_schema(self, schema: typing.Dict) -> typing.Dict:
        pass

    @staticmethod
    @abstractmethod
    def load(fp: str) -> BaseSchema:
        """Parse a schema from a file path"""

    @staticmethod
    @abstractmethod
    async def async_load(fp: str) -> BaseSchema:
        """Parse a schema from a file path"""

    @property
    @abstractmethod
    def name(self) -> typing.Optional[str]:
        pass

    @property
    @abstractmethod
    def schema_type(self) -> str:
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
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        self._expanded_schema: typing.Optional[str] = None
        self._flat_schema: typing.Optional[str] = None

        super().__init__(*args, **kwargs)

    @property
    def name(self) -> typing.Optional[str]:
        return self.schema.get("name")

    @property
    def schema_type(self) -> str:
        return AVRO_SCHEMA_TYPE

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
    @property
    def name(self) -> typing.Optional[str]:
        return self.schema.get("title", self.schema.get("$id", self.schema.get("$ref")))

    @property
    def schema_type(self) -> str:
        return JSON_SCHEMA_TYPE

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


class SchemaFactory:
    @staticmethod
    def create_schema(schema: str, schema_type: str) -> typing.Union[JsonSchema, AvroSchema]:
        if schema_type == JSON_SCHEMA_TYPE:
            return JsonSchema(schema)
        elif schema_type == AVRO_SCHEMA_TYPE:
            return AvroSchema(schema)
        else:
            raise ValueError(f"Unsupported schema type '{schema_type}'. Supported schemas are 'AVRO' and 'JSON'.")


@dataclass
class SubjectVersion(object):
    subject: str
    version: int
