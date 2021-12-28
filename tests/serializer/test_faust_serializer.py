import typing

import faust
import pydantic
from dataclasses_avroschema import AvroModel

from schema_registry.client import schema
from schema_registry.serializers import AvroMessageSerializer, JsonMessageSerializer, faust as serializer
from tests import data_gen


def test_create_avro_faust_serializer(client, avro_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, avro_country_schema)

    assert type(faust_serializer.message_serializer) == AvroMessageSerializer
    assert faust_serializer.schema_subject == schema_subject
    assert faust_serializer.schema == avro_country_schema
    assert faust_serializer.message_serializer.schemaregistry_client == client


def test_avro_dumps_load_message(client, avro_country_schema):
    faust_serializer = serializer.FaustSerializer(client, "test-avro-country", avro_country_schema)

    record = {"country": "Argentina"}
    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_avro_nested_schema(client):
    nested_schema = schema.AvroSchema(data_gen.AVRO_NESTED_SCHENA)
    faust_serializer = serializer.FaustSerializer(client, "test-avro-nested-schema", nested_schema)

    record = data_gen.create_nested_schema()
    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_avro_dumps_load_with_register_codec(client, avro_country_schema):
    payload = {"country": "Argentina"}
    country_serializer = serializer.FaustSerializer(client, "test-avro-country", avro_country_schema)

    faust.serializers.codecs.register("country_avro_serializer", country_serializer)

    class CountryRecord(faust.Record, serializer="country_avro_serializer"):
        country: str

    country_record = CountryRecord(**payload)
    message_encoded = country_record.dumps()

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = CountryRecord.loads(message_encoded)

    assert message_decoded == country_record


def test_avro_nested_schema_with_register_codec(client):
    nested_schema = schema.AvroSchema(data_gen.AVRO_NESTED_SCHENA)
    order_schema = schema.AvroSchema(data_gen.AVRO_ORDER_SCHENA)

    customer_serializer = serializer.FaustSerializer(client, "test-avro-nested-schema", nested_schema)
    order_serializer = serializer.FaustSerializer(client, "test-avro-order-schema", order_schema)

    faust.serializers.codecs.register("customer_avro_serializer", customer_serializer)
    faust.serializers.codecs.register("order_avro_serializer", order_serializer)

    class Order(faust.Record, serializer="order_avro_serializer"):
        uid: int

    class Customer(faust.Record, serializer="customer_avro_serializer"):
        name: str
        uid: int
        order: Order

    payload = data_gen.create_nested_schema()

    customer = Customer(**payload)

    message_encoded = customer.dumps()

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = Customer.loads(message_encoded)
    assert message_decoded == customer


def test_avro_dumps_load_message_dataclasses_avro_schema(client):
    class AdvanceUserModel(faust.Record, AvroModel):
        first_name: str
        last_name: str
        age: int

    faust_serializer = serializer.FaustSerializer(client, "test-dataclasses-avroschema", AdvanceUserModel.avro_schema())

    record = {
        "first_name": "Juan",
        "last_name": "Perez",
        "age": 20,
    }

    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_avro_dumps_load_message_union_avro_schema(client):
    class FirstMemberRecord(faust.Record, AvroModel):
        name: str = ""

    class SecondMemberRecord(faust.Record, AvroModel):
        name: str = ""

    class UnionFieldAvroModel(faust.Record, AvroModel):
        a_name: typing.Union[FirstMemberRecord, SecondMemberRecord, None]

    avro_name = "test-union-field-avroschema"
    avro_schema = UnionFieldAvroModel.avro_schema()

    faust_serializer = serializer.FaustSerializer(client, avro_name, avro_schema, return_record_name=True)

    record = {"a_name": ("FirstMemberRecord", {"name": "jj"})}

    message_encoded = faust_serializer._dumps(record)

    assert message_encoded

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_create_json_faust_serializer(client, json_country_schema):
    schema_subject = "test-json-country"
    faust_serializer = serializer.FaustJsonSerializer(client, schema_subject, json_country_schema)

    assert type(faust_serializer.message_serializer) == JsonMessageSerializer
    assert faust_serializer.schema_subject == schema_subject
    assert faust_serializer.schema == json_country_schema
    assert faust_serializer.message_serializer.schemaregistry_client == client


def test_json_dumps_load_message(client, json_country_schema):
    faust_serializer = serializer.FaustJsonSerializer(client, "test-json-country", json_country_schema)

    record = {"country": "Argentina"}
    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_json_nested_schema(client):
    nested_schema = schema.JsonSchema(data_gen.JSON_NESTED_SCHEMA)
    faust_serializer = serializer.FaustJsonSerializer(client, "test-json-nested-schema", nested_schema)

    record = data_gen.create_nested_schema()
    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_json_dumps_load_with_register_codec(client, json_country_schema):
    payload = {"country": "Argentina"}
    country_serializer = serializer.FaustJsonSerializer(client, "test-json-country", json_country_schema)

    faust.serializers.codecs.register("country_json_serializer", country_serializer)

    class CountryRecord(faust.Record, serializer="country_json_serializer"):
        country: str

    country_record = CountryRecord(**payload)
    message_encoded = country_record.dumps()

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = CountryRecord.loads(message_encoded)

    assert message_decoded == country_record


def test_json_nested_schema_with_register_codec(client):
    nested_schema = schema.JsonSchema(data_gen.JSON_NESTED_SCHEMA)
    order_schema = schema.JsonSchema(data_gen.JSON_ORDER_SCHEMA)

    customer_serializer = serializer.FaustJsonSerializer(client, "test-json-nested-schema", nested_schema)
    order_serializer = serializer.FaustJsonSerializer(client, "test-json-order-schema", order_schema)

    faust.serializers.codecs.register("customer_json_serializer", customer_serializer)
    faust.serializers.codecs.register("order_json_serializer", order_serializer)

    class Order(faust.Record, serializer="order_json_serializer"):
        uid: int

    class Customer(faust.Record, serializer="customer_json_serializer"):
        name: str
        uid: int
        order: Order

    payload = data_gen.create_nested_schema()

    customer = Customer(**payload)

    message_encoded = customer.dumps()

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = Customer.loads(message_encoded)
    assert message_decoded == customer


def test_json_dumps_load_message_dataclasses_json_schema(client):
    class AdvanceUserModel(faust.Record, pydantic.BaseModel):
        first_name: str
        last_name: str
        age: int

    faust_serializer = serializer.FaustJsonSerializer(
        client, "test-dataclasses-jsonschema", AdvanceUserModel.schema_json()
    )

    record = {
        "first_name": "Juan",
        "last_name": "Perez",
        "age": 20,
    }

    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_json_dumps_load_message_union_json_schema(client):
    class FirstMemberRecord(pydantic.BaseModel):
        name: str = ""

    class SecondMemberRecord(pydantic.BaseModel):
        name: str = ""

    class UnionFieldJsonModel(pydantic.BaseModel):
        a_name: typing.Union[FirstMemberRecord, SecondMemberRecord, None]

    json_name = "test-union-field-jsonschema"
    json_schema = UnionFieldJsonModel.schema_json()

    faust_serializer = serializer.FaustJsonSerializer(client, json_name, json_schema, return_record_name=True)

    record = {"a_name": {"name": "jj"}}

    message_encoded = faust_serializer._dumps(record)

    assert message_encoded

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record
