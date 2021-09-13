import pytest

from schema_registry.client import schema
from tests import data_gen


@pytest.mark.asyncio
async def test_avro_delete_subject(async_client, avro_user_schema_v3):
    subject = "avro-subject-to-delete"
    versions = [schema.AvroSchema(data_gen.AVRO_USER_V1), schema.AvroSchema(data_gen.AVRO_USER_V2)]

    for version in versions:
        await async_client.register(subject, version)

    assert len(await async_client.delete_subject(subject)) == len(versions)


@pytest.mark.asyncio
async def test_json_delete_subject(async_client, json_user_schema_v3):
    subject = "json-subject-to-delete"
    versions = [schema.JsonSchema(data_gen.JSON_USER_V2), json_user_schema_v3]

    for version in versions:
        await async_client.register(subject, version)

    assert len(await async_client.delete_subject(subject)) == len(versions)


@pytest.mark.asyncio
async def test_delete_subject_does_not_exist(async_client):
    assert not await async_client.delete_subject("a-random-subject")
