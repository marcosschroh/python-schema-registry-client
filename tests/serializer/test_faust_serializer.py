import faust

from schema_registry.client import schema
from schema_registry.serializers import faust_serializer as serializer
from tests import data_gen


def test_create_faust_serializer(client, country_schema):
    schema_subject = "test-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, country_schema)

    assert faust_serializer.schema_registry_client == client
    assert faust_serializer.schema_subject == schema_subject
    assert faust_serializer.schema == country_schema


def test_dumps_load_message(client, country_schema):
    faust_serializer = serializer.FaustSerializer(client, "test-country", country_schema)

    record = {"country": "Argentina"}
    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_nested_schema(client):
    nested_schema = schema.AvroSchema(data_gen.NESTED_SCHENA)
    faust_serializer = serializer.FaustSerializer(client, "test-nested-schema", nested_schema)

    record = data_gen.create_nested_schema()
    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record


def test_dumps_load_with_register_codec(client, country_schema):
    payload = {"country": "Argenntina"}
    country_serializer = serializer.FaustSerializer(client, "test-country", country_schema)

    faust.serializers.codecs.register("country_serializer", country_serializer)

    class CountryRecord(faust.Record, serializer="country_serializer"):
        country: str

    country_record = CountryRecord(**payload)
    message_encoded = country_record.dumps()

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = CountryRecord.loads(message_encoded)

    assert message_decoded == country_record


def test_nested_schema_with_register_codec(client):
    nested_schema = schema.AvroSchema(data_gen.NESTED_SCHENA)
    order_schema = schema.AvroSchema(data_gen.ORDER_SCHENA)

    customer_serializer = serializer.FaustSerializer(client, "test-nested-schema", nested_schema)
    order_serializer = serializer.FaustSerializer(client, "test-order-schema", order_schema)

    faust.serializers.codecs.register("customer_serializer", customer_serializer)
    faust.serializers.codecs.register("order_serializer", order_serializer)

    class Order(faust.Record, serializer="order_serializer"):
        uid: int

    class Customer(faust.Record, serializer="customer_serializer"):
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
