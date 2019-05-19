import pytest
import struct

from schema_registry.serializer.message_serializer import MessageSerializer
from schema_registry.client import SchemaRegistryClient, load

from tests.server import mock_registry
from tests.client import data_gen


@pytest.fixture
def client():
    server = mock_registry.ServerThread(0)
    server.start()
    yield SchemaRegistryClient(f"http://127.0.0.1:{server.server.server_port}")
    server.shutdown()
    server.join()


@pytest.fixture
def message_serializer(client):
    return MessageSerializer(client)


def assertMessageIsSame(message, expected, schema_id, message_serializer):
    assert message
    assert len(message) > 5

    magic, sid = struct.unpack(">bI", message[0:5])
    assert magic == 0
    assert sid == schema_id

    decoded = message_serializer.decode_message(message)
    assert decoded
    assert decoded == expected


def hash_func(self):
    return hash(str(self))


def test_encode_with_schema_id(client, message_serializer):
    adv = load.loads(data_gen.ADVANCED_SCHEMA)
    basic = load.loads(data_gen.BASIC_SCHEMA)
    subject = "test"
    schema_id = client.register(subject, basic)

    records = data_gen.BASIC_ITEMS
    for record in records:
        message = message_serializer.encode_record_with_schema_id(schema_id, record)
        assertMessageIsSame(message, record, schema_id, message_serializer)

    subject = "test_adv"
    adv_schema_id = client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.ADVANCED_ITEMS
    for record in records:
        message = message_serializer.encode_record_with_schema_id(adv_schema_id, record)
        assertMessageIsSame(message, record, adv_schema_id, message_serializer)


def test_encode_record_with_schema(client, message_serializer):
    topic = "test"
    basic = load.loads(data_gen.BASIC_SCHEMA)
    subject = "test-value"
    schema_id = client.register(subject, basic)
    records = data_gen.BASIC_ITEMS

    for record in records:
        message = message_serializer.encode_record_with_schema(topic, basic, record)
        assertMessageIsSame(message, record, schema_id, message_serializer)


def test_decode_none(message_serializer):
    """"null/None messages should decode to None"""

    assert message_serializer.decode_message(None) is None
