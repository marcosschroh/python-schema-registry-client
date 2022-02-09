import struct

import jsonschema
import pytest

from schema_registry.client import schema
from tests import data_gen


def assertAvroMessageIsSame(message, expected, schema_id, avro_message_serializer):
    assert message
    assert len(message) > 5

    magic, sid = struct.unpack(">bI", message[0:5])
    assert magic == 0
    assert sid == schema_id

    decoded = avro_message_serializer.decode_message(message)
    assert decoded
    assert decoded == expected


def test_avro_encode_with_schema_id(client, avro_message_serializer):
    basic = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    subject = "test-avro-basic-schema"
    schema_id = client.register(subject, basic)

    records = data_gen.AVRO_BASIC_ITEMS
    for record in records:
        message = avro_message_serializer.encode_record_with_schema_id(schema_id, record)
        assertAvroMessageIsSame(message, record, schema_id, avro_message_serializer)

    adv = schema.AvroSchema(data_gen.AVRO_ADVANCED_SCHEMA)
    subject = "test-avro-advance-schema"
    adv_schema_id = client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.AVRO_ADVANCED_ITEMS
    for record in records:
        message = avro_message_serializer.encode_record_with_schema_id(adv_schema_id, record)
        assertAvroMessageIsSame(message, record, adv_schema_id, avro_message_serializer)


def test_avro_encode_logical_types(client, avro_message_serializer):
    logical_types_schema = schema.AvroSchema(data_gen.AVRO_LOGICAL_TYPES_SCHEMA)
    subject = "test-logical-types-schema"
    schema_id = client.register(subject, logical_types_schema)

    record = data_gen.create_logical_item()
    message = avro_message_serializer.encode_record_with_schema_id(schema_id, record)

    decoded = avro_message_serializer.decode_message(message)

    decoded_datetime = decoded.get("metadata").get("timestamp")
    timestamp = record.get("metadata").get("timestamp")

    decoded_total = decoded.get("metadata").get("total")
    total = record.get("metadata").get("total")

    assert timestamp == decoded_datetime.replace(tzinfo=None)
    assert total == decoded_total


def test_avro_encode_decode_with_schema_from_json(avro_message_serializer, avro_deployment_schema):
    deployment_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": 1, "port": 8080}

    message_encoded = avro_message_serializer.encode_record_with_schema(
        "avro-deployment", avro_deployment_schema, deployment_record
    )

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    # now decode the message
    message_decoded = avro_message_serializer.decode_message(message_encoded)
    assert message_decoded == deployment_record


# def test_encode_with_schema_string(avro_message_serializer):
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

#     message_encoded = avro_message_serializer.encode_record_with_schema(
#         "avro-deployment", schema, deployment_record
#     )

#     assert message_encoded
#     assert len(message_encoded) > 5
#     assert isinstance(message_encoded, bytes)

#     # now decode the message
#     message_decoded = avro_message_serializer.decode_message(message_encoded)
#     assert message_decoded == deployment_record


def test_avro_fail_encode_with_schema(avro_message_serializer, avro_deployment_schema):
    bad_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": "1", "port": "8080"}

    with pytest.raises(TypeError):
        avro_message_serializer.encode_record_with_schema("avro-deployment", avro_deployment_schema, bad_record)


def test_avro_encode_record_with_schema(client, avro_message_serializer):
    topic = "test"
    basic = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    subject = "test-avro-value"
    schema_id = client.register(subject, basic)
    records = data_gen.AVRO_BASIC_ITEMS

    for record in records:
        message = avro_message_serializer.encode_record_with_schema(topic, basic, record)
        assertAvroMessageIsSame(message, record, schema_id, avro_message_serializer)


def test_avro_decode_none(avro_message_serializer):
    """ "null/None messages should decode to None"""

    assert avro_message_serializer.decode_message(None) is None


def assertJsonMessageIsSame(message, expected, schema_id, json_message_serializer):
    assert message
    assert len(message) > 5

    magic, sid = struct.unpack(">bI", message[0:5])
    assert magic == 0
    assert sid == schema_id

    decoded = json_message_serializer.decode_message(message)
    assert decoded
    assert decoded == expected


def test_json_encode_with_schema_id(client, json_message_serializer):
    basic = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    subject = "test-json-basic-schema"
    schema_id = client.register(subject, basic)

    records = data_gen.JSON_BASIC_ITEMS
    for record in records:
        message = json_message_serializer.encode_record_with_schema_id(schema_id, record)
        assertJsonMessageIsSame(message, record, schema_id, json_message_serializer)

    adv = schema.JsonSchema(data_gen.JSON_ADVANCED_SCHEMA)
    subject = "test-json-advance-schema"
    adv_schema_id = client.register(subject, adv)

    assert adv_schema_id != schema_id

    records = data_gen.JSON_ADVANCED_ITEMS
    for record in records:
        message = json_message_serializer.encode_record_with_schema_id(adv_schema_id, record)
        assertJsonMessageIsSame(message, record, adv_schema_id, json_message_serializer)


def test_json_encode_decode_with_schema_from_json(json_message_serializer, json_deployment_schema):
    deployment_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": 1, "port": 8080}

    message_encoded = json_message_serializer.encode_record_with_schema(
        "json-deployment", json_deployment_schema, deployment_record
    )

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    # now decode the message
    message_decoded = json_message_serializer.decode_message(message_encoded)
    assert message_decoded == deployment_record


def test_json_fail_encode_with_schema(json_message_serializer, json_deployment_schema):
    bad_record = {"image": "registry.gitlab.com/my-project:1.0.0", "replicas": "1", "port": "8080"}

    with pytest.raises(jsonschema.exceptions.ValidationError):
        json_message_serializer.encode_record_with_schema("json-deployment", json_deployment_schema, bad_record)


def test_json_encode_record_with_schema(client, json_message_serializer):
    topic = "test"
    basic = schema.JsonSchema(data_gen.JSON_BASIC_SCHEMA)
    subject = "test-json-value"
    schema_id = client.register(subject, basic)
    records = data_gen.JSON_BASIC_ITEMS

    for record in records:
        message = json_message_serializer.encode_record_with_schema(topic, basic, record)
        assertJsonMessageIsSame(message, record, schema_id, json_message_serializer)


def test_json_decode_none(json_message_serializer):
    """ "null/None messages should decode to None"""

    assert json_message_serializer.decode_message(None) is None
