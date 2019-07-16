import pytest

from schema_registry.client import schema, errors

from tests import data_gen


def test_schema_from_string():
    parsed = schema.load_schema(data_gen.BASIC_SCHEMA)

    assert isinstance(parsed, schema.AvroSchema)


def test_schema_from_file():
    parsed = schema.load(data_gen.get_schema_path("adv_schema.avsc"))
    assert isinstance(parsed, schema.AvroSchema)


def test_schema_load_parse_error():
    with pytest.raises(errors.ClientError) as excinfo:
        schema.load(data_gen.get_schema_path("invalid_scema.avsc"))
    assert "Schema parse failed:" in str(excinfo.value)
