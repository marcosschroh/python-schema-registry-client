from schema_registry.client import utils


def test_avro_version_does_not_exists(client, avro_country_schema):
    assert client.check_version("test-avro-schema-version", avro_country_schema) is None


def test_avro_get_versions(client, avro_country_schema):
    subject = "test-avro-schema-version"
    client.register(subject, avro_country_schema)
    versions = client.get_versions(subject)

    assert versions


def test_avro_get_versions_does_not_exist(client):
    assert not client.get_versions("random-subject")


def test_avro_check_version(client, avro_country_schema):
    subject = "test-avro-schema-version"
    schema_id = client.register(subject, avro_country_schema)
    result = client.check_version(subject, avro_country_schema)

    assert subject == result.subject
    assert schema_id == result.schema_id


def test_avro_check_version_dataclasses_avroschema(client, dataclass_avro_schema):
    subject = "dataclasses-avroschema-subject"
    schema_id = client.register(subject, dataclass_avro_schema.avro_schema())
    result = client.check_version(subject, dataclass_avro_schema.avro_schema())

    assert subject == result.subject
    assert schema_id == result.schema_id


def test_avro_delete_version(client, avro_country_schema):
    subject = "test-avro-schema-version"
    client.register(subject, avro_country_schema)
    versions = client.get_versions(subject)
    latest_version = versions[-1]

    assert latest_version == client.delete_version(subject, latest_version)


def test_avro_delete_version_does_not_exist(client, avro_country_schema):
    subject = "test-avro-schema-version"
    client.register(subject, avro_country_schema)

    assert not client.delete_version("random-subject")
    assert not client.delete_version(subject, "random-version")


def test_json_version_does_not_exists(client, json_country_schema):
    assert client.check_version("test-json-schema-version", json_country_schema) is None


def test_json_get_versions(client, json_country_schema):
    subject = "test-json-schema-version"
    client.register(subject, json_country_schema)
    versions = client.get_versions(subject)

    assert versions


def test_json_get_versions_does_not_exist(client):
    assert not client.get_versions("random-subject")


def test_json_check_version(client, json_country_schema):
    subject = "test-json-schema-version"
    schema_id = client.register(subject, json_country_schema)
    result = client.check_version(subject, json_country_schema)

    assert subject == result.subject
    assert schema_id == result.schema_id


def test_json_check_version_dataclasses_jsonschema(client, dataclass_json_schema):
    subject = "dataclasses-jsonschema-subject"
    schema_id = client.register(subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE)
    result = client.check_version(subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE)

    assert subject == result.subject
    assert schema_id == result.schema_id


def test_json_delete_version(client, json_country_schema):
    subject = "test-json-schema-version"
    client.register(subject, json_country_schema)
    versions = client.get_versions(subject)
    latest_version = versions[-1]

    assert latest_version == client.delete_version(subject, latest_version)


def test_json_delete_version_does_not_exist(client, json_country_schema):
    subject = "test-json-schema-version"
    client.register(subject, json_country_schema)

    assert not client.delete_version("random-subject")
    assert not client.delete_version(subject, "random-version")
