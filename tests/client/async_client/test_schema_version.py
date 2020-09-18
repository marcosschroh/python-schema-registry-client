import pytest


@pytest.mark.asyncio
async def test_version_does_not_exists(async_client, country_schema):
    assert await async_client.check_version("test-schema-version", country_schema) is None


@pytest.mark.asyncio
async def test_get_versions(async_client, country_schema):
    subject = "test-schema-version"
    await async_client.register(subject, country_schema)
    versions = await async_client.get_versions(subject)

    assert versions


@pytest.mark.asyncio
async def test_get_versions_does_not_exist(async_client):
    assert not await async_client.get_versions("random-subject")


@pytest.mark.asyncio
async def test_check_version(async_client, country_schema):
    subject = "test-schema-version"
    schema_id = await async_client.register(subject, country_schema)
    result = await async_client.check_version(subject, country_schema)

    assert subject == result.subject
    assert schema_id == result.schema_id


@pytest.mark.asyncio
async def test_check_version_dataclases_avroschema(async_client, dataclass_avro_schema):
    subject = "dataclasses-avroschema-subject"
    schema_id = await async_client.register(subject, dataclass_avro_schema.avro_schema())
    result = await async_client.check_version(subject, dataclass_avro_schema.avro_schema())

    assert subject == result.subject
    assert schema_id == result.schema_id


@pytest.mark.asyncio
async def test_delete_version(async_client, country_schema):
    subject = "test-schema-version"
    await async_client.register(subject, country_schema)
    versions = await async_client.get_versions(subject)
    latest_version = versions[-1]

    assert latest_version == await async_client.delete_version(subject, latest_version)


@pytest.mark.asyncio
async def test_delete_version_does_not_exist(async_client, country_schema):
    subject = "test-schema-version"
    await async_client.register(subject, country_schema)

    assert not await async_client.delete_version("random-subject")
    assert not await async_client.delete_version(subject, "random-version")
