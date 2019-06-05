import pytest
import struct

from schema_registry.client import load

from tests import data_gen


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
    subject = "test-basic-schema"
    schema_id = client.register(subject, basic)

    records = data_gen.BASIC_ITEMS
    for record in records:
        message = message_serializer.encode_record_with_schema_id(schema_id, record)
        assertMessageIsSame(message, record, schema_id, message_serializer)

    subject = "test-advance-schema"
    adv_schema_id = client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.ADVANCED_ITEMS
    for record in records:
        message = message_serializer.encode_record_with_schema_id(adv_schema_id, record)
        assertMessageIsSame(message, record, adv_schema_id, message_serializer)


def test_encode_decode_with_schema_from_json(message_serializer, deployment_schema):
    deployment_record = {
        "image": "registry.gitlab.com/my-project:1.0.0",
        "replicas": 1,
        "port": 8080,
    }

    message_encoded = message_serializer.encode_record_with_schema(
        "deployment", deployment_schema, deployment_record
    )

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    # now decode the message
    message_decoded = message_serializer.decode_message(message_encoded)
    assert message_decoded == deployment_record


def test_fail_encode_with_schema(message_serializer, deployment_schema):
    bad_record = {
        "image": "registry.gitlab.com/my-project:1.0.0",
        "replicas": "1",
        "port": "8080",
    }

    with pytest.raises(TypeError):
        message_serializer.encode_record_with_schema(
            "deployment", deployment_schema, bad_record
        )


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
