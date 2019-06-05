import pytest
import avro

from schema_registry.client import load, errors

from tests import data_gen


def test_schema_from_string():
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    assert isinstance(parsed, avro.schema.Schema)


def test_schema_from_file():
    parsed = load.load(data_gen.get_schema_path("adv_schema.avsc"))
    assert isinstance(parsed, avro.schema.Schema)


def test_schema_load_parse_error():
    with pytest.raises(errors.ClientError) as excinfo:
        load.load(data_gen.get_schema_path("invalid_scema.avsc"))
    assert "Schema parse failed:" in str(excinfo.value)
