from schema_registry.client import schema
from tests import data_gen


def assertLatest(self, meta_tuple, sid, schema, version):
    self.assertNotEqual(sid, -1)
    self.assertNotEqual(version, -1)
    self.assertEqual(meta_tuple[0], sid)
    self.assertEqual(meta_tuple[1], schema)
    self.assertEqual(meta_tuple[2], version)


def test_register(client):
    parsed = schema.AvroSchema(data_gen.BASIC_SCHEMA)
    schema_id = client.register("test-basic-schema", parsed)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1


def test_register_json_data(client, deployment_schema):
    schema_id = client.register("test-deployment", deployment_schema)
    assert schema_id > 0


def test_register_with_custom_headers(client, country_schema):
    headers = {"custom-serialization": "application/x-avro-json"}
    schema_id = client.register("test-country", country_schema, headers=headers)
    assert schema_id > 0


def test_register_with_logical_types(client):
    parsed = schema.AvroSchema(data_gen.LOGICAL_TYPES_SCHEMA)
    schema_id = client.register("test-logical-types-schema", parsed)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1


def test_multi_subject_register(client):
    parsed = schema.AvroSchema(data_gen.BASIC_SCHEMA)
    schema_id = client.register("test-basic-schema", parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = client.register("test-basic-schema-backup", parsed)
    assert schema_id == dupe_id
    assert len(client.id_to_schema) == 1


def test_dupe_register(client):
    parsed = schema.AvroSchema(data_gen.BASIC_SCHEMA)
    subject = "test-basic-schema"
    schema_id = client.register(subject, parsed)

    assert schema_id > 0
    latest = client.get_schema(subject)

    # register again under same subject
    dupe_id = client.register(subject, parsed)
    assert schema_id == dupe_id

    dupe_latest = client.get_schema(subject)
    assert latest == dupe_latest


def test_multi_register(client):
    """
    Register two different schemas under the same subject
    with backwards compatibility
    """
    version_1 = schema.AvroSchema(data_gen.USER_V1)
    version_2 = schema.AvroSchema(data_gen.USER_V2)
    subject = "test-user-schema"

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
