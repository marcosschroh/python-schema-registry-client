import httpx
import pytest

from schema_registry.client import errors, schema
from tests import data_gen


@pytest.mark.asyncio
async def test_compatibility(async_client, user_schema_v3):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "test-user-schema"
    version_2 = schema.AvroSchema(data_gen.USER_V2)
    await async_client.register(subject, version_2)

    compatibility = await async_client.test_compatibility(subject, user_schema_v3)
    assert compatibility


@pytest.mark.asyncio
async def test_compatibility_dataclasses_avroschema(async_client, dataclass_avro_schema, dataclass_avro_schema_advance):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "dataclasses-avroschema-subject"
    await async_client.register(subject, dataclass_avro_schema.avro_schema())

    compatibility = await async_client.test_compatibility(subject, dataclass_avro_schema_advance.avro_schema())
    assert compatibility


@pytest.mark.asyncio
async def test_update_compatibility_for_subject(async_client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert await async_client.update_compatibility("FULL", "test-user-schema")


@pytest.mark.asyncio
async def test_update_global_compatibility(async_client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert await async_client.update_compatibility("FULL")


@pytest.mark.asyncio
async def test_update_compatibility_fail(async_client, response_klass, async_mock):
    http_code = 404
    mock = async_mock(httpx.AsyncClient, "request", returned_value=response_klass(http_code))

    with mock:
        with pytest.raises(errors.ClientError) as excinfo:
            await async_client.update_compatibility("FULL", "test-user-schema")

            assert excinfo.http_code == http_code


@pytest.mark.asyncio
async def test_get_compatibility_for_subject(async_client):
    """
    Test latest compatibility for test-user-schema subject
    """
    assert await async_client.get_compatibility("test-user-schema") == "FULL"


@pytest.mark.asyncio
async def test_get_global_compatibility(async_client):
    """
    Test latest compatibility for test-user-schema subject
    """
    assert await async_client.get_compatibility() is not None
