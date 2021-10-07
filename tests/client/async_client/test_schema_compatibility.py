import httpx
import pytest

from schema_registry.client import errors, schema, utils
from tests import data_gen


@pytest.mark.asyncio
async def test_avro_compatibility(async_client, avro_user_schema_v3):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "test-avro-user-schema"
    version_2 = schema.AvroSchema(data_gen.AVRO_USER_V2)
    await async_client.register(subject, version_2)

    compatibility = await async_client.test_compatibility(subject, avro_user_schema_v3)
    assert compatibility


@pytest.mark.asyncio
async def test_avro_compatibility_dataclasses_avroschema(
    async_client, dataclass_avro_schema, dataclass_avro_schema_advance
):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "dataclasses-avroschema-subject"
    await async_client.register(subject, dataclass_avro_schema.avro_schema())

    compatibility = await async_client.test_compatibility(subject, dataclass_avro_schema_advance.avro_schema())
    assert compatibility


@pytest.mark.asyncio
async def test_avro_update_compatibility_for_subject(async_client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert await async_client.update_compatibility("FULL", "test-avro-user-schema")


@pytest.mark.asyncio
async def test_avro_update_global_compatibility(async_client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert await async_client.update_compatibility("FULL")


@pytest.mark.asyncio
async def test_avro_update_compatibility_fail(async_client, response_klass, async_mock):
    http_code = 404
    mock = async_mock(httpx.AsyncClient, "request", returned_value=response_klass(http_code))

    with mock:
        with pytest.raises(errors.ClientError) as excinfo:
            await async_client.update_compatibility("FULL", "test-avro-user-schema")

            assert excinfo.http_code == http_code


@pytest.mark.asyncio
async def test_avro_get_compatibility_for_subject(async_client):
    """
    Test latest compatibility for test-avro-user-schema subject
    """
    assert await async_client.get_compatibility("test-avro-user-schema") == "FULL"


@pytest.mark.asyncio
async def test_avro_get_global_compatibility(async_client):
    """
    Test latest compatibility for test-avro-user-schema subject
    """
    assert await async_client.get_compatibility() is not None


@pytest.mark.asyncio
async def test_json_compatibility(async_client, json_user_schema_v3):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "test-json-user-schema"
    version_2 = schema.JsonSchema(data_gen.JSON_USER_V2)
    await async_client.register(subject, version_2)

    compatibility = await async_client.test_compatibility(subject, json_user_schema_v3)
    assert compatibility


@pytest.mark.asyncio
async def test_json_compatibility_dataclasses_jsonschema(
    async_client, dataclass_json_schema, dataclass_json_schema_advance
):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "dataclasses-jsonschema-subject"
    await async_client.register(subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE)

    compatibility = await async_client.test_compatibility(
        subject, dataclass_json_schema_advance.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE
    )

    assert compatibility


@pytest.mark.asyncio
async def test_json_update_compatibility_for_subject(async_client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert await async_client.update_compatibility("FULL", "test-json-user-schema")


@pytest.mark.asyncio
async def test_json_update_global_compatibility(async_client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert await async_client.update_compatibility("FULL")


@pytest.mark.asyncio
async def test_json_update_compatibility_fail(async_client, response_klass, async_mock):
    http_code = 404
    mock = async_mock(httpx.AsyncClient, "request", returned_value=response_klass(http_code))

    with mock:
        with pytest.raises(errors.ClientError) as excinfo:
            await async_client.update_compatibility("FULL", "test-json-user-schema")

            assert excinfo.http_code == http_code


@pytest.mark.asyncio
async def test_json_get_compatibility_for_subject(async_client):
    """
    Test latest compatibility for test-json-user-schema subject
    """
    assert await async_client.get_compatibility("test-json-user-schema") == "FULL"


@pytest.mark.asyncio
async def test_json_get_global_compatibility(async_client):
    """
    Test latest compatibility for test-json-user-schema subject
    """
    assert await async_client.get_compatibility() is not None
