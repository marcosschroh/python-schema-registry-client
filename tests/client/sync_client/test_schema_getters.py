from schema_registry.client import schema as schema_loader
from tests import data_gen


def test_avro_getters(client):
    subject = "test-avro-basic-schema"
    parsed_basic = schema_loader.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
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


def test_avro_get_subjects(client, avro_user_schema_v3, avro_country_schema):
    subject_user = "test-avro-user-schema"
    subject_country = "test-avro-country"

    client.register("test-avro-user-schema", avro_user_schema_v3)
    client.register("test-avro-country", avro_country_schema)

    subjects = client.get_subjects()

    assert subject_user in subjects
    assert subject_country in subjects


def test_json_getters(client):
    subject = "test-json-basic-schema"
    parsed_basic = schema_loader.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
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


def test_json_get_subjects(client, json_user_schema_v3, json_country_schema):
    subject_user = "test-json-user-schema"
    subject_country = "test-json-country"

    client.register("test-json-user-schema", json_user_schema_v3)
    client.register("test-json-country", json_country_schema)

    subjects = client.get_subjects()

    assert subject_user in subjects
    assert subject_country in subjects
