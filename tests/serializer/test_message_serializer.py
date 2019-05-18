import pytest
import struct

from confluent_kafka import avro

from schemaregistry.serializer.message_serializer import MessageSerializer
from schemaregistry.client import CachedSchemaRegistryClient

from tests.server import mock_registry
from tests.client import data_gen


@pytest.fixture
def client():
    server = mock_registry.ServerThread(0)
    server.start()
    yield CachedSchemaRegistryClient(f"http://127.0.0.1:{server.server.server_port}")
    server.shutdown()
    server.join()


@pytest.fixture
def message_serializer(client):
    return MessageSerializer(client)


async def assertMessageIsSame(message, expected, schema_id, message_serializer):
    assert message
    assert len(message) > 5

    magic, sid = struct.unpack('>bI', message[0:5])
    assert magic == 0
    assert sid == schema_id

    decoded = await message_serializer.decode_message(message)
    assert decoded
    assert decoded == expected


def hash_func(self):
    return hash(str(self))


@pytest.mark.asyncio
async def test_encode_with_schema_id(client, message_serializer):
    adv = avro.loads(data_gen.ADVANCED_SCHEMA)
    basic = avro.loads(data_gen.BASIC_SCHEMA)
    subject = 'test'
    schema_id = await client.register(subject, basic)

    records = data_gen.BASIC_ITEMS
    for record in records:
        message = await message_serializer.encode_record_with_schema_id(schema_id, record)
        await assertMessageIsSame(message, record, schema_id, message_serializer)

    subject = 'test_adv'
    adv_schema_id = await client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.ADVANCED_ITEMS
    for record in records:
        message = await message_serializer.encode_record_with_schema_id(adv_schema_id, record)
        await assertMessageIsSame(message, record, adv_schema_id, message_serializer)


@pytest.mark.asyncio
async def test_encode_record_with_schema(client, message_serializer):
    topic = 'test'
    basic = avro.loads(data_gen.BASIC_SCHEMA)
    subject = 'test-value'
    schema_id = await client.register(subject, basic)
    records = data_gen.BASIC_ITEMS

    for record in records:
        message = await message_serializer.encode_record_with_schema(topic, basic, record)
        await assertMessageIsSame(message, record, schema_id, message_serializer)


@pytest.mark.asyncio
async def test_decode_none(message_serializer):
    """"null/None messages should decode to None"""

    assert await message_serializer.decode_message(None) is None
