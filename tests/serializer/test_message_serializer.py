#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#

import struct

import unittest

from confluent_kafka import avro
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from tests.client import data_gen
from tests.client.mock_schema_registry_client import MockSchemaRegistryClient


class TestMessageSerializer(unittest.TestCase):
    def setUp(self):
        # need to set up the serializer
        self.client = MockSchemaRegistryClient()
        self.ms = MessageSerializer(self.client)

    def assertMessageIsSame(self, message, expected, schema_id):
        self.assertTrue(message)
        self.assertTrue(len(message) > 5)
        magic, sid = struct.unpack('>bI', message[0:5])
        self.assertEqual(magic, 0)
        self.assertEqual(sid, schema_id)
        decoded = self.ms.decode_message(message)
        self.assertTrue(decoded)
        self.assertEqual(decoded, expected)

    def test_encode_with_schema_id(self):
        adv = avro.loads(data_gen.ADVANCED_SCHEMA)
        basic = avro.loads(data_gen.BASIC_SCHEMA)
        subject = 'test'
        schema_id = self.client.register(subject, basic)

        records = data_gen.BASIC_ITEMS
        for record in records:
            message = self.ms.encode_record_with_schema_id(schema_id, record)
            self.assertMessageIsSame(message, record, schema_id)

        subject = 'test_adv'
        adv_schema_id = self.client.register(subject, adv)
        self.assertNotEqual(adv_schema_id, schema_id)
        records = data_gen.ADVANCED_ITEMS
        for record in records:
            message = self.ms.encode_record_with_schema_id(adv_schema_id, record)
            self.assertMessageIsSame(message, record, adv_schema_id)

    def test_encode_record_with_schema(self):
        topic = 'test'
        basic = avro.loads(data_gen.BASIC_SCHEMA)
        subject = 'test-value'
        schema_id = self.client.register(subject, basic)
        records = data_gen.BASIC_ITEMS
        for record in records:
            message = self.ms.encode_record_with_schema(topic, basic, record)
            self.assertMessageIsSame(message, record, schema_id)

    def test_decode_none(self):
        """"null/None messages should decode to None"""

        self.assertIsNone(self.ms.decode_message(None))

    def hash_func(self):
        return hash(str(self))
