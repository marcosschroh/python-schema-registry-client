import struct

import pytest

from schema_registry.client import schema
from tests import data_gen

pytestmark = pytest.mark.asyncio


async def assertMessageIsSame(message, expected, schema_id, async_message_serializer):
    assert message
    assert len(message) > 5

    magic, sid = struct.unpack(">bI", message[0:5])
    assert magic == 0
    assert sid == schema_id

    decoded = await async_message_serializer.decode_message(message)
    assert decoded
    assert decoded == expected


async def test_encode_with_schema_id(async_client, async_message_serializer):
    basic = schema.AvroSchema(data_gen.BASIC_SCHEMA)
    subject = "test-basic-schema"
    schema_id = await async_client.register(subject, basic)

    records = data_gen.BASIC_ITEMS
    for record in records:
        message = await async_message_serializer.encode_record_with_schema_id(schema_id, record)
        await assertMessageIsSame(message, record, schema_id, async_message_serializer)

    adv = schema.AvroSchema(data_gen.ADVANCED_SCHEMA)
    subject = "test-advance-schema"
    adv_schema_id = await async_client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.ADVANCED_ITEMS
    for record in records:
        message = await async_message_serializer.encode_record_with_schema_id(adv_schema_id, record)
        await assertMessageIsSame(message, record, adv_schema_id, async_message_serializer)


async def test_encode_logical_types(async_client, async_message_serializer):
    logical_types_schema = schema.AvroSchema(data_gen.LOGICAL_TYPES_SCHEMA)
    subject = "test-logical-types-schema"
    schema_id = await async_client.register(subject, logical_types_schema)

    record = data_gen.create_logical_item()
    message = await async_message_serializer.encode_record_with_schema_id(schema_id, record)

    decoded = await async_message_serializer.decode_message(message)

    decoded_datetime = decoded.get("metadata").get("timestamp")
    timestamp = record.get("metadata").get("timestamp")

    decoded_total = decoded.get("metadata").get("total")
    total = record.get("metadata").get("total")

    assert timestamp == decoded_datetime.replace(tzinfo=None)
    assert total == decoded_total


async def test_encode_decode_with_schema_from_json(async_message_serializer, deployment_schema):
    deployment_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": 1, "port": 8080}

    message_encoded = await async_message_serializer.encode_record_with_schema("deployment", deployment_schema, deployment_record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    # now decode the message
    message_decoded = await async_message_serializer.decode_message(message_encoded)
    assert message_decoded == deployment_record


async def test_fail_encode_with_schema(async_message_serializer, deployment_schema):
    bad_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": "1", "port": "8080"}

    with pytest.raises(TypeError):
        await async_message_serializer.encode_record_with_schema("deployment", deployment_schema, bad_record)


async def test_encode_record_with_schema(async_client, async_message_serializer):
    topic = "test"
    basic = schema.AvroSchema(data_gen.BASIC_SCHEMA)
    subject = "test-value"
    schema_id = await async_client.register(subject, basic)
    records = data_gen.BASIC_ITEMS

    for record in records:
        message = await async_message_serializer.encode_record_with_schema(topic, basic, record)
        await assertMessageIsSame(message, record, schema_id, async_message_serializer)


async def test_decode_none(async_message_serializer):
    """ "null/None messages should decode to None"""

    assert await async_message_serializer.decode_message(None) is None
