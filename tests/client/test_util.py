import unittest
import pytest

from avro import schema
from confluent_kafka import avro

from tests.client import data_gen


class TestUtil(unittest.TestCase):
    def test_schema_from_string(self):
        parsed = avro.loads(data_gen.BASIC_SCHEMA)
        self.assertTrue(isinstance(parsed, schema.Schema))

    def test_schema_from_file(self):
        parsed = avro.load(data_gen.get_schema_path('adv_schema.avsc'))
        self.assertTrue(isinstance(parsed, schema.Schema))

    def test_schema_load_parse_error(self):
        with pytest.raises(avro.ClientError) as excinfo:
            avro.load(data_gen.get_schema_path("invalid_scema.avsc"))
        assert 'Schema parse failed:' in str(excinfo.value)
