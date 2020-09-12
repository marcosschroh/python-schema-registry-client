import pytest

from schema_registry.client import schema
from tests import data_gen


@pytest.mark.asyncio
async def test_delete_subject(async_client, user_schema_v3):
    subject = "subject-to-delete"
    versions = [schema.AvroSchema(data_gen.USER_V1), schema.AvroSchema(data_gen.USER_V2)]

    for version in versions:
        await async_client.register(subject, version)

    assert len(await async_client.delete_subject(subject)) == len(versions)


@pytest.mark.asyncio
async def test_delete_subject_does_not_exist(async_client):
    assert not await async_client.delete_subject("a-random-subject")
