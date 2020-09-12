from schema_registry.client import schema as schema_loader
from tests import data_gen


def test_getters(client):
    subject = "test-basic-schema"
    parsed_basic = schema_loader.AvroSchema(data_gen.BASIC_SCHEMA)
    client.register(subject, parsed_basic)
    schema = client.get_by_id(1)
    assert schema is not None

    subject = "subject-does-not-exist"
    latest = client.get_schema(subject)
    assert latest is None

    schema_id = client.register(subject, parsed_basic)
    latest = client.get_schema(subject)
    fetched = client.get_by_id(schema_id)

    assert fetched == parsed_basic


def test_get_subjects(client, user_schema_v3, country_schema):
    subject_user = "test-user-schema"
    subject_country = "test-country"

    client.register("test-user-schema", user_schema_v3)
    client.register("test-country", country_schema)

    subjects = client.get_subjects()

    assert subject_user in subjects
    assert subject_country in subjects
