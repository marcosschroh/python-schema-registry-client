from schema_registry.client import schema as schema_loader

from tests import data_gen


def test_getters(client):
    parsed = schema_loader.AvroSchema(data_gen.BASIC_SCHEMA)
    subject = "subject-does-not-exist"

    parsed_basic = schema_loader.AvroSchema(data_gen.BASIC_SCHEMA)
    client.register("test-basic-schema", parsed_basic)
    schema = client.get_by_id(1)
    assert schema is not None

    latest = client.get_schema(subject)
    assert latest is None

    # register schema
    schema_id = client.register(subject, parsed)
    latest = client.get_schema(subject)
    fetched = client.get_by_id(schema_id)

    assert fetched == parsed
