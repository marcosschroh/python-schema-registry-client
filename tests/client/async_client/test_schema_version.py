import pytest

from schema_registry.client import utils


@pytest.mark.asyncio
async def test_avro_version_does_not_exists(async_client, avro_country_schema):
    assert await async_client.check_version("test-avro-schema-version", avro_country_schema) is None


@pytest.mark.asyncio
async def test_avro_get_versions(async_client, avro_country_schema):
    subject = "test-avro-schema-version"
    await async_client.register(subject, avro_country_schema)
    versions = await async_client.get_versions(subject)

    assert versions


@pytest.mark.asyncio
async def test_avro_get_versions_does_not_exist(async_client):
    assert not await async_client.get_versions("random-subject")


@pytest.mark.asyncio
async def test_avro_check_version(async_client, avro_country_schema):
    subject = "test-avro-schema-version"
    schema_id = await async_client.register(subject, avro_country_schema)
    result = await async_client.check_version(subject, avro_country_schema)

    assert subject == result.subject
    assert schema_id == result.schema_id


@pytest.mark.asyncio
async def test_avro_check_version_dataclasses_avroschema(async_client, dataclass_avro_schema):
    subject = "dataclasses-avroschema-subject"
    schema_id = await async_client.register(subject, dataclass_avro_schema.avro_schema())
    result = await async_client.check_version(subject, dataclass_avro_schema.avro_schema())

    assert subject == result.subject
    assert schema_id == result.schema_id


@pytest.mark.asyncio
async def test_avro_delete_version(async_client, avro_country_schema):
    subject = "test-avro-schema-version"
    await async_client.register(subject, avro_country_schema)
    versions = await async_client.get_versions(subject)
    latest_version = versions[-1]

    assert latest_version == await async_client.delete_version(subject, latest_version)


@pytest.mark.asyncio
async def test_avro_delete_version_does_not_exist(async_client, avro_country_schema):
    subject = "test-avro-schema-version"
    await async_client.register(subject, avro_country_schema)

    assert not await async_client.delete_version("random-subject")
    assert not await async_client.delete_version(subject, "random-version")


@pytest.mark.asyncio
async def test_json_version_does_not_exists(async_client, json_country_schema):
    assert await async_client.check_version("test-json-schema-version", json_country_schema) is None


@pytest.mark.asyncio
async def test_json_get_versions(async_client, json_country_schema):
    subject = "test-json-schema-version"
    await async_client.register(subject, json_country_schema)
    versions = await async_client.get_versions(subject)

    assert versions


@pytest.mark.asyncio
async def test_json_get_versions_does_not_exist(async_client):
    assert not await async_client.get_versions("random-subject")


@pytest.mark.asyncio
async def test_json_check_version(async_client, json_country_schema):
    subject = "test-json-schema-version"
    schema_id = await async_client.register(subject, json_country_schema)
    result = await async_client.check_version(subject, json_country_schema)

    assert subject == result.subject
    assert schema_id == result.schema_id


@pytest.mark.asyncio
async def test_json_check_version_dataclasses_avroschema(async_client, dataclass_json_schema):
    subject = "dataclasses-jsonschema-subject"
    schema_id = await async_client.register(
        subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE
    )
    result = await async_client.check_version(
        subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE
    )

    assert subject == result.subject
    assert schema_id == result.schema_id


@pytest.mark.asyncio
async def test_json_delete_version(async_client, json_country_schema):
    subject = "test-json-schema-version"
    await async_client.register(subject, json_country_schema)
    versions = await async_client.get_versions(subject)
    latest_version = versions[-1]

    assert latest_version == await async_client.delete_version(subject, latest_version)


@pytest.mark.asyncio
async def test_json_delete_version_does_not_exist(async_client, json_country_schema):
    subject = "test-json-schema-version"
    await async_client.register(subject, json_country_schema)

    assert not await async_client.delete_version("random-subject")
    assert not await async_client.delete_version(subject, "random-version")
