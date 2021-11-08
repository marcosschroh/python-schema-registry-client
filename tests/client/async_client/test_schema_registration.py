import pytest

from schema_registry.client import schema, utils
from tests import data_gen


def assertLatest(self, meta_tuple, sid, schema, version):
    self.assertNotEqual(sid, -1)
    self.assertNotEqual(version, -1)
    self.assertEqual(meta_tuple[0], sid)
    self.assertEqual(meta_tuple[1], schema)
    self.assertEqual(meta_tuple[2], version)


@pytest.mark.asyncio
async def test_avro_register(async_client):
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    schema_id = await async_client.register("test-avro-basic-schema", parsed)

    assert schema_id > 0
    assert len(async_client.id_to_schema) == 1

    schema_versions = await async_client.get_schema_subject_versions(schema_id)
    assert len(schema_versions) == 1
    assert schema_versions[0].subject == "test-avro-basic-schema"


@pytest.mark.asyncio
async def test_avro_register_json_data(async_client, avro_deployment_schema):
    schema_id = await async_client.register("test-avro-deployment", avro_deployment_schema)
    assert schema_id > 0


@pytest.mark.asyncio
async def test_avro_register_with_custom_headers(async_client, avro_country_schema):
    headers = {"custom-serialization": "application/x-avro-json"}
    schema_id = await async_client.register("test-avro-country", avro_country_schema, headers=headers)
    assert schema_id > 0


@pytest.mark.asyncio
async def test_avro_register_with_logical_types(async_client):
    parsed = schema.AvroSchema(data_gen.AVRO_LOGICAL_TYPES_SCHEMA)
    schema_id = await async_client.register("test-logical-types-schema", parsed)

    assert schema_id > 0
    assert len(async_client.id_to_schema) == 1


@pytest.mark.asyncio
async def test_avro_multi_subject_register(async_client):
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    schema_id = await async_client.register("test-avro-basic-schema", parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = await async_client.register("test-avro-basic-schema-backup", parsed)
    assert schema_id == dupe_id
    assert len(async_client.id_to_schema) == 1

    schema_versions = await async_client.get_schema_subject_versions(schema_id)
    assert len(schema_versions) == 2
    schema_versions.sort(key=lambda x: x.subject)
    assert schema_versions[0].subject == "test-avro-basic-schema"
    assert schema_versions[1].subject == "test-avro-basic-schema-backup"
    # The schema version we get here has a tendency to vary with the
    # number of times the schema has been soft-deleted, so only verifying
    # it's an int and > 0
    assert type(schema_versions[1].version) == int
    assert schema_versions[1].version > 0


@pytest.mark.asyncio
async def test_avro_dupe_register(async_client):
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    subject = "test-avro-basic-schema"
    schema_id = await async_client.register(subject, parsed)

    # Verify we had a check version call
    async_client.assert_url_suffix(0, "/subjects/%s" % subject)
    async_client.assert_method(0, "POST")
    # Verify that we had a register call
    async_client.assert_url_suffix(1, "/subjects/%s/versions" % subject)
    async_client.assert_method(1, "POST")
    assert len(async_client.request_calls) == 2

    assert schema_id > 0
    latest = await async_client.get_schema(subject)

    async_client.assert_url_suffix(2, "/subjects/%s/versions/latest" % subject)
    async_client.assert_method(2, "GET")
    assert len(async_client.request_calls) == 3

    # register again under same subject
    dupe_id = await async_client.register(subject, parsed)
    assert schema_id == dupe_id

    # Served from cache
    assert len(async_client.request_calls) == 3

    dupe_latest = await async_client.get_schema(subject)
    assert latest == dupe_latest


@pytest.mark.asyncio
async def test_avro_multi_register(async_client):
    """
    Register two different schemas under the same subject
    with backwards compatibility
    """
    version_1 = schema.AvroSchema(data_gen.AVRO_USER_V1)
    version_2 = schema.AvroSchema(data_gen.AVRO_USER_V2)
    subject = "test-avro-user-schema"

    id1 = await async_client.register(subject, version_1)
    latest_schema_1 = await async_client.get_schema(subject)
    await async_client.check_version(subject, version_1)

    id2 = await async_client.register(subject, version_2)
    latest_schema_2 = await async_client.get_schema(subject)
    await async_client.check_version(subject, version_2)

    assert id1 != id2
    assert latest_schema_1 != latest_schema_2
    # ensure version is higher
    assert latest_schema_1.version < latest_schema_2.version

    await async_client.register(subject, version_1)
    latest_schema_3 = await async_client.get_schema(subject)

    assert latest_schema_2 == latest_schema_3


@pytest.mark.asyncio
async def test_register_dataclass_avro_schema(async_client, dataclass_avro_schema):
    subject = "dataclasses-avroschema-subject"
    schema_id = await async_client.register(subject, dataclass_avro_schema.avro_schema())

    assert schema_id > 0
    assert len(async_client.id_to_schema) == 1

    subjects = await async_client.get_subjects()

    assert subject in subjects


@pytest.mark.asyncio
async def test_json_register(async_client):
    parsed = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    schema_id = await async_client.register("test-json-basic-schema", parsed)

    assert schema_id > 0
    assert len(async_client.id_to_schema) == 1


@pytest.mark.asyncio
async def test_json_register_json_data(async_client, json_deployment_schema):
    schema_id = await async_client.register("test-json-deployment", json_deployment_schema)
    assert schema_id > 0


@pytest.mark.asyncio
async def test_json_register_with_custom_headers(async_client, json_country_schema):
    headers = {"custom-serialization": "application/x-avro-json"}
    schema_id = await async_client.register("test-json-country", json_country_schema, headers=headers)
    assert schema_id > 0


@pytest.mark.asyncio
async def test_json_multi_subject_register(async_client):
    parsed = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    schema_id = await async_client.register("test-json-basic-schema", parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = await async_client.register("test-json-basic-schema-backup", parsed)
    assert schema_id == dupe_id
    assert len(async_client.id_to_schema) == 1


@pytest.mark.asyncio
async def test_json_dupe_register(async_client):
    parsed = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    subject = "test-json-basic-schema"
    schema_id = await async_client.register(subject, parsed)

    # Verify we had a check version call
    async_client.assert_url_suffix(0, "/subjects/%s" % subject)
    async_client.assert_method(0, "POST")
    # Verify that we had a register call
    async_client.assert_url_suffix(1, "/subjects/%s/versions" % subject)
    async_client.assert_method(1, "POST")
    assert len(async_client.request_calls) == 2

    assert schema_id > 0
    latest = await async_client.get_schema(subject)

    async_client.assert_url_suffix(2, "/subjects/%s/versions/latest" % subject)
    async_client.assert_method(2, "GET")
    assert len(async_client.request_calls) == 3

    # register again under same subject
    dupe_id = await async_client.register(subject, parsed)
    assert schema_id == dupe_id

    # Served from cache
    assert len(async_client.request_calls) == 3

    dupe_latest = await async_client.get_schema(subject)
    assert latest == dupe_latest


@pytest.mark.asyncio
async def test_json_multi_register(async_client, json_user_schema_v3):
    """
    Register two different schemas under the same subject
    with backwards compatibility
    """
    version_1 = schema.JsonSchema(data_gen.JSON_USER_V2)
    version_2 = json_user_schema_v3
    subject = "test-json-user-schema"

    id1 = await async_client.register(subject, version_1)
    latest_schema_1 = await async_client.get_schema(subject)
    await async_client.check_version(subject, version_1)

    id2 = await async_client.register(subject, version_2)
    latest_schema_2 = await async_client.get_schema(subject)
    await async_client.check_version(subject, version_2)

    assert id1 != id2
    assert latest_schema_1 != latest_schema_2
    # ensure version is higher
    assert latest_schema_1.version < latest_schema_2.version

    await async_client.register(subject, version_1)
    latest_schema_3 = await async_client.get_schema(subject)

    assert latest_schema_2 == latest_schema_3


@pytest.mark.asyncio
async def test_register_dataclass_json_schema(async_client, dataclass_json_schema):
    subject = "dataclasses-jsonschema-subject"
    schema_id = await async_client.register(
        subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE
    )

    assert schema_id > 0
    assert len(async_client.id_to_schema) == 1

    subjects = await async_client.get_subjects()

    assert subject in subjects
