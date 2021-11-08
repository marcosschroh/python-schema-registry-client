from schema_registry.client import schema, utils
from tests import data_gen
from tests.conftest import RequestLoggingSchemaRegistryClient


def assertLatest(self, meta_tuple, sid, schema, version):
    self.assertNotEqual(sid, -1)
    self.assertNotEqual(version, -1)
    self.assertEqual(meta_tuple[0], sid)
    self.assertEqual(meta_tuple[1], schema)
    self.assertEqual(meta_tuple[2], version)


def test_avro_register(client):
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    schema_id = client.register("test-avro-basic-schema", parsed)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1

    schema_versions = client.get_schema_subject_versions(schema_id)
    assert len(schema_versions) == 1
    assert schema_versions[0].subject == "test-avro-basic-schema"


def test_avro_register_json_data(client, avro_deployment_schema):
    schema_id = client.register("test-avro-deployment", avro_deployment_schema)
    assert schema_id > 0


def test_avro_register_with_custom_headers(client, avro_country_schema):
    headers = {"custom-serialization": "application/x-avro-json"}
    schema_id = client.register("test-avro-country", avro_country_schema, headers=headers)
    assert schema_id > 0


def test_avro_register_with_logical_types(client):
    parsed = schema.AvroSchema(data_gen.AVRO_LOGICAL_TYPES_SCHEMA)
    schema_id = client.register("test-logical-types-schema", parsed)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1


def test_avro_multi_subject_register(client: RequestLoggingSchemaRegistryClient):
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    schema_id = client.register("test-avro-basic-schema", parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = client.register("test-avro-basic-schema-backup", parsed)
    assert schema_id == dupe_id
    assert len(client.id_to_schema) == 1

    schema_versions = client.get_schema_subject_versions(schema_id)
    assert len(schema_versions) == 2
    schema_versions.sort(key=lambda x: x.subject)
    assert schema_versions[0].subject == "test-avro-basic-schema"
    assert schema_versions[1].subject == "test-avro-basic-schema-backup"
    # The schema version we get here has a tendency to vary with the
    # number of times the schema has been soft-deleted, so only verifying
    # it's an int and > 0
    assert type(schema_versions[1].version) == int
    assert schema_versions[1].version > 0


def test_avro_dupe_register(client):
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    subject = "test-avro-basic-schema"
    schema_id = client.register(subject, parsed)

    # Verify we had a check version call
    client.assert_url_suffix(0, "/subjects/%s" % subject)
    client.assert_method(0, "POST")
    # Verify that we had a register call
    client.assert_url_suffix(1, "/subjects/%s/versions" % subject)
    client.assert_method(1, "POST")
    assert len(client.request_calls) == 2

    assert schema_id > 0
    latest = client.get_schema(subject)

    client.assert_url_suffix(2, "/subjects/%s/versions/latest" % subject)
    client.assert_method(2, "GET")
    assert len(client.request_calls) == 3

    # register again under same subject
    dupe_id = client.register(subject, parsed)
    assert schema_id == dupe_id

    # Served from cache
    assert len(client.request_calls) == 3

    dupe_latest = client.get_schema(subject)
    assert latest == dupe_latest


def test_avro_multi_register(client):
    """
    Register two different schemas under the same subject
    with backwards compatibility
    """
    version_1 = schema.AvroSchema(data_gen.AVRO_USER_V1)
    version_2 = schema.AvroSchema(data_gen.AVRO_USER_V2)
    subject = "test-avro-user-schema"

    id1 = client.register(subject, version_1)
    latest_schema_1 = client.get_schema(subject)
    client.check_version(subject, version_1)

    id2 = client.register(subject, version_2)
    latest_schema_2 = client.get_schema(subject)
    client.check_version(subject, version_2)

    assert id1 != id2
    assert latest_schema_1 != latest_schema_2
    # ensure version is higher
    assert latest_schema_1.version < latest_schema_2.version

    client.register(subject, version_1)
    latest_schema_3 = client.get_schema(subject)

    assert latest_schema_2 == latest_schema_3


def test_register_dataclass_avro_schema(client, dataclass_avro_schema):
    subject = "dataclasses-avroschema-subject"
    schema_id = client.register(subject, dataclass_avro_schema.avro_schema())

    assert schema_id > 0
    assert len(client.id_to_schema) == 1

    subjects = client.get_subjects()

    assert subject in subjects


def test_json_register(client):
    parsed = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    schema_id = client.register("test-json-basic-schema", parsed)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1


def test_json_register_json_data(client, json_deployment_schema):
    schema_id = client.register("test-json-deployment", json_deployment_schema)
    assert schema_id > 0


def test_json_register_with_custom_headers(client, json_country_schema):
    headers = {"custom-serialization": "application/x-avro-json"}
    schema_id = client.register("test-json-country", json_country_schema, headers=headers)
    assert schema_id > 0


def test_json_multi_subject_register(client):
    parsed = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    schema_id = client.register("test-json-basic-schema", parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = client.register("test-json-basic-schema-backup", parsed)
    assert schema_id == dupe_id
    assert len(client.id_to_schema) == 1


def test_json_dupe_register(client):
    parsed = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    subject = "test-json-basic-schema"
    schema_id = client.register(subject, parsed)

    # Verify we had a check version call
    client.assert_url_suffix(0, "/subjects/%s" % subject)
    client.assert_method(0, "POST")
    # Verify that we had a register call
    client.assert_url_suffix(1, "/subjects/%s/versions" % subject)
    client.assert_method(1, "POST")
    assert len(client.request_calls) == 2

    assert schema_id > 0
    latest = client.get_schema(subject)

    client.assert_url_suffix(2, "/subjects/%s/versions/latest" % subject)
    client.assert_method(2, "GET")
    assert len(client.request_calls) == 3

    # register again under same subject
    dupe_id = client.register(subject, parsed)
    assert schema_id == dupe_id

    # Served from cache
    assert len(client.request_calls) == 3

    dupe_latest = client.get_schema(subject)
    assert latest == dupe_latest


def test_json_multi_register(client, json_user_schema_v3):
    """
    Register two different schemas under the same subject
    with backwards compatibility
    """
    version_1 = schema.JsonSchema(data_gen.JSON_USER_V2)
    version_2 = json_user_schema_v3
    subject = "test-json-user-schema"

    id1 = client.register(subject, version_1)
    latest_schema_1 = client.get_schema(subject)
    client.check_version(subject, version_1)

    id2 = client.register(subject, version_2)
    latest_schema_2 = client.get_schema(subject)
    client.check_version(subject, version_2)

    assert id1 != id2
    assert latest_schema_1 != latest_schema_2
    # ensure version is higher
    assert latest_schema_1.version < latest_schema_2.version

    client.register(subject, version_1)
    latest_schema_3 = client.get_schema(subject)

    assert latest_schema_2 == latest_schema_3


def test_register_dataclass_json_schema(client, dataclass_json_schema):
    subject = "dataclasses-jsonschema-subject"
    schema_id = client.register(subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1

    subjects = client.get_subjects()

    assert subject in subjects
