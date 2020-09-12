import fastavro
import pytest

from schema_registry.client import schema
from tests import data_gen


def test_schema_from_string():
    parsed = schema.AvroSchema(data_gen.BASIC_SCHEMA)

    assert isinstance(parsed, schema.AvroSchema)


@pytest.mark.asyncio
async def test_schema_from_file():
    parsed = await schema.async_load(data_gen.get_schema_path("adv_schema.avsc"))
    assert isinstance(parsed, schema.AvroSchema)


@pytest.mark.asyncio
async def test_schema_load_parse_error():
    with pytest.raises(fastavro.schema.UnknownType):
        await schema.async_load(data_gen.get_schema_path("invalid_schema.avsc"))
