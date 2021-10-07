from schema_registry.client import schema
from tests import data_gen


def test_avro_delete_subject(client, avro_user_schema_v3):
    subject = "avro-subject-to-delete"
    versions = [schema.AvroSchema(data_gen.AVRO_USER_V1), schema.AvroSchema(data_gen.AVRO_USER_V2)]

    for version in versions:
        client.register(subject, version)

    assert len(client.delete_subject(subject)) == len(versions)


def test_json_delete_subject(client, json_user_schema_v3):
    subject = "json-subject-to-delete"
    versions = [schema.JsonSchema(data_gen.JSON_USER_V2), json_user_schema_v3]

    for version in versions:
        client.register(subject, version)

    assert len(client.delete_subject(subject)) == len(versions)


def test_delete_subject_does_not_exist(client):
    assert not client.delete_subject("a-random-subject")
