from schema_registry.client import load

from tests import data_gen


def test_getters(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    subject = "subject-does-not-exist"
    version = client.check_version(subject, parsed)
    assert version is None

    parsed_basic = load.loads(data_gen.BASIC_SCHEMA)
    client.register("test-basic-schema", parsed_basic)
    schema = client.get_by_id(1)
    assert schema is not None

    latest = client.get_schema(subject)
    assert latest == (None, None, None, None)

    # register
    schema_id = client.register(subject, parsed)
    schema = client.get_schema(subject, schema_id)
    assert schema is not None

    latest = client.get_schema(subject)
    version = client.check_version(subject, parsed)
    fetched = client.get_by_id(schema_id)

    assert fetched == parsed
