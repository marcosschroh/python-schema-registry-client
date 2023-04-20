import struct

import jsonschema
import pytest

from schema_registry.client import schema
from tests import data_gen

pytestmark = pytest.mark.asyncio


async def assertAvroMessageIsSame(message, expected, schema_id, async_avro_message_serializer):
    assert message
    assert len(message) > 5

    magic, sid = struct.unpack(">bI", message[0:5])
    assert magic == 0
    assert sid == schema_id

    deserialized = await async_avro_message_serializer.deserialize(message)
    assert deserialized
    assert deserialized == expected


async def test_avro_serialize_with_schema_id(async_client, async_avro_message_serializer):
    basic = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    subject = "test-avro-basic-schema"
    schema_id = await async_client.register(subject, basic)

    records = data_gen.AVRO_BASIC_ITEMS
    for record in records:
        message = await async_avro_message_serializer.serialize(schema_id, record)
        await assertAvroMessageIsSame(message, record, schema_id, async_avro_message_serializer)

    adv = schema.AvroSchema(data_gen.AVRO_ADVANCED_SCHEMA)
    subject = "test-avro-advance-schema"
    adv_schema_id = await async_client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.AVRO_ADVANCED_ITEMS
    for record in records:
        message = await async_avro_message_serializer.serialize(adv_schema_id, record)
        await assertAvroMessageIsSame(message, record, adv_schema_id, async_avro_message_serializer)

#TODO : Stabilize this test for all timezones.
@pytest.mark.xfail
async def test_avro_serialize_logical_types(async_client, async_avro_message_serializer):
    logical_types_schema = schema.AvroSchema(data_gen.AVRO_LOGICAL_TYPES_SCHEMA)
    subject = "test-logical-types-schema"
    schema_id = await async_client.register(subject, logical_types_schema)

    record = data_gen.create_logical_item()
    message = await async_avro_message_serializer.serialize(schema_id, record)

    deserialized = await async_avro_message_serializer.deserialize(message)

    deserialized_datetime = deserialized.get("metadata").get("timestamp")
    timestamp = record.get("metadata").get("timestamp")

    deserialized_total = deserialized.get("metadata").get("total")
    total = record.get("metadata").get("total")

    assert timestamp == deserialized_datetime.replace(tzinfo=None)
    assert total == deserialized_total


async def test_avro_serialize_deserialize_with_schema_from_json(async_avro_message_serializer, avro_deployment_schema):
    deployment_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": 1, "port": 8080}

    message_serialized = await async_avro_message_serializer.serialize(
        avro_deployment_schema, deployment_record, "avro-deployment"
    )

    assert message_serialized
    assert len(message_serialized) > 5
    assert isinstance(message_serialized, bytes)

    # now deserialize the message
    message_deserialized = await async_avro_message_serializer.deserialize(message_serialized)
    assert message_deserialized == deployment_record


# async def test_serialize_with_schema_string(async_avro_message_serializer):
#     deployment_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": 1, "port": 8080}
#     schema = """{
#         "type": "record",
#         "namespace": "com.kubertenes.v2",
#         "name": "AvroDeploymentV2",
#         "fields": [
#             {"name": "image", "type": "string"},
#             {"name": "replicas", "type": "int"},
#             {"name": "host", "type": "string", "default": "localhost"},
#             {"name": "port", "type": "int"}
#         ]
#     }"""

#     message_serialized = async_avro_message_serializer.serialize_record_with_schema(
#         "avro-deployment", schema, deployment_record
#     )

#     assert message_serialized
#     assert len(message_serialized) > 5
#     assert isinstance(message_serialized, bytes)

#     # now deserialize the message
#     message_deserialized = async_avro_message_serializer.deserialize()(message_serialized)
#     assert message_deserialized == deployment_record


async def test_avro_fail_serialize_with_schema(async_avro_message_serializer, avro_deployment_schema):
    bad_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": "1", "port": "8080"}

    with pytest.raises(TypeError):
        await async_avro_message_serializer.serialize(
            avro_deployment_schema, bad_record, "avro-deployment"
        )


async def test_avro_serialize_record_with_schema(async_client, async_avro_message_serializer):
    topic = "test"
    basic = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    subject = "test-avro-value"
    schema_id = await async_client.register(subject, basic)
    records = data_gen.AVRO_BASIC_ITEMS

    for record in records:
        message = await async_avro_message_serializer.serialize( basic, record, topic)
        await assertAvroMessageIsSame(message, record, schema_id, async_avro_message_serializer)


async def test_avro_deserialize_none(async_avro_message_serializer):
    """ "null/None messages should deserialize to None"""

    assert await async_avro_message_serializer.deserialize(None) is None


async def assertJsonMessageIsSame(message, expected, schema_id, async_json_message_serializer):
    assert message
    assert len(message) > 5

    magic, sid = struct.unpack(">bI", message[0:5])
    assert magic == 0
    assert sid == schema_id

    deserialized = await async_json_message_serializer.deserialize(message)
    assert deserialized
    assert deserialized == expected


async def test_json_serialize_with_schema_id(async_client, async_json_message_serializer):
    basic = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    subject = "test-json-basic-schema"
    schema_id = await async_client.register(subject, basic)

    records = data_gen.JSON_BASIC_ITEMS
    for record in records:
        message = await async_json_message_serializer.serialize(schema_id, record)
        await assertJsonMessageIsSame(message, record, schema_id, async_json_message_serializer)

    adv = schema.JsonSchema(data_gen.JSON_ADVANCED_SCHEMA)
    subject = "test-json-advance-schema"
    adv_schema_id = await async_client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.JSON_ADVANCED_ITEMS
    for record in records:
        message = await async_json_message_serializer.serialize(adv_schema_id, record)
        await assertJsonMessageIsSame(message, record, adv_schema_id, async_json_message_serializer)


async def test_json_serialize_deserialize_with_schema_from_json(async_json_message_serializer, json_deployment_schema):
    deployment_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": 1, "port": 8080}

    message_serialized = await async_json_message_serializer.serialize(
        json_deployment_schema, deployment_record, "json-deployment",
    )

    assert message_serialized
    assert len(message_serialized) > 5
    assert isinstance(message_serialized, bytes)

    # now deserialize the message
    message_deserialized = await async_json_message_serializer.deserialize(message_serialized)
    assert message_deserialized == deployment_record


async def test_json_fail_serialize_with_schema(async_json_message_serializer, json_deployment_schema):
    bad_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": "1", "port": "8080"}

    with pytest.raises(jsonschema.exceptions.ValidationError):
        await async_json_message_serializer.serialize(
            json_deployment_schema, bad_record, "json-deployment"
        )


async def test_json_serialize_record_with_schema(async_client, async_json_message_serializer):
    topic = "test"
    basic = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    subject = "test-json-value"
    schema_id = await async_client.register(subject, basic)
    records = data_gen.JSON_BASIC_ITEMS

    for record in records:
        message = await async_json_message_serializer.serialize(basic, record, topic)
        await assertJsonMessageIsSame(message, record, schema_id, async_json_message_serializer)


async def test_json_deserialize_none(async_json_message_serializer):
    """ "null/None messages should deserialize to None"""

    assert await async_json_message_serializer.deserialize(None) is None
