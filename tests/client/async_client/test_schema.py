import fastavro
import jsonschema
import pytest

from schema_registry.client import schema
from tests import data_gen


def test_avro_schema_from_string():
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)

    assert isinstance(parsed, schema.AvroSchema)


@pytest.mark.asyncio
async def test_avro_schema_from_file():
    parsed = await schema.AvroSchema.async_load(data_gen.get_schema_path("adv_schema.avsc"))
    assert isinstance(parsed, schema.AvroSchema)


@pytest.mark.asyncio
async def test_avro_schema_load_parse_error():
    with pytest.raises(fastavro.schema.UnknownType):
        await schema.AvroSchema.async_load(data_gen.get_schema_path("invalid_schema.avsc"))


def test_json_schema_from_string():
    parsed = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)

    assert isinstance(parsed, schema.JsonSchema)


@pytest.mark.asyncio
async def test_json_schema_from_file():
    parsed = await schema.JsonSchema.async_load(data_gen.get_schema_path("adv_schema.json"))
    assert isinstance(parsed, schema.JsonSchema)


@pytest.mark.asyncio
async def test_json_schema_load_parse_error():
    with pytest.raises(jsonschema.exceptions.SchemaError):
        await schema.JsonSchema.async_load(data_gen.get_schema_path("invalid_schema.json"))
