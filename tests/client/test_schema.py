import fastavro
import pytest

from schema_registry.client import schema
from tests import data_gen


def test_schema_from_string():
    parsed = schema.AvroSchema(data_gen.BASIC_SCHEMA)

    assert isinstance(parsed, schema.AvroSchema)


def test_schema_from_file():
    parsed = schema.load(data_gen.get_schema_path("adv_schema.avsc"))
    assert isinstance(parsed, schema.AvroSchema)


def test_schema_load_parse_error():
    with pytest.raises(fastavro.schema.UnknownType):
        schema.load(data_gen.get_schema_path("invalid_schema.avsc"))


def test_expended_schema(client):
    advance_schema = schema.AvroSchema(data_gen.ADVANCED_SCHEMA)
    expanded = {
        "type": "record",
        "doc": "advanced schema for tests",
        "name": "python.test.advanced.advanced",
        "fields": [
            {"name": "number", "doc": "age", "type": ["long", "null"]},
            {"name": "name", "doc": "a name", "type": ["string"]},
            {
                "doc": "friends",
                "name": "friends",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "python.test.advanced.basicPerson",
                        "fields": [
                            {"doc": "friend age", "name": "number", "type": ["long", "null"]},
                            {"doc": "friend name", "name": "name", "type": ["string"]},
                        ],
                    },
                },
            },
            {
                "name": "family",
                "doc": "family",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "python.test.advanced.basicPerson",
                        "fields": [
                            {"doc": "friend age", "name": "number", "type": ["long", "null"]},
                            {"doc": "friend name", "name": "name", "type": ["string"]},
                        ],
                    },
                },
            },
        ],
    }

    assert advance_schema.expanded_schema == expanded


def test_flat_schema(client):
    advance_schema = schema.AvroSchema(data_gen.ADVANCED_SCHEMA)
    subject = "test-advance-schema"
    client.register(subject, advance_schema)

    schema_version = client.get_schema(subject)
    parsed_schema = schema_version.schema
    parsed_schema.schema.pop("__fastavro_parsed")
    parsed_schema.schema.pop("__named_schemas")

    assert schema_version.schema.flat_schema == parsed_schema.schema
