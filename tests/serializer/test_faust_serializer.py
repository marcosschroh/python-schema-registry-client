from schema_registry.serializers import faust_serializer as serializer


def test_create_faust_serializer(client, country_schema):
    schema_subject = "test-country"
    faust_serializer = serializer.FaustSerializer(
        client, schema_subject, country_schema
    )

    assert faust_serializer.schema_registry_client == client
    assert faust_serializer.schema_subject == schema_subject
    assert faust_serializer.schema == country_schema
    assert faust_serializer.is_key is False


def test_dumps_load_message(client, country_schema):
    schema_subject = "test-country"
    faust_serializer = serializer.FaustSerializer(
        client, schema_subject, country_schema
    )

    record = {"country": "Argentina"}
    message_encoded = faust_serializer._dumps(record)

    assert message_encoded
    assert len(message_encoded) > 5
    assert isinstance(message_encoded, bytes)

    message_decoded = faust_serializer._loads(message_encoded)
    assert message_decoded == record
