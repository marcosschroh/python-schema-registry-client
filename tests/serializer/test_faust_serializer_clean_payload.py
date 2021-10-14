import typing

from faust import Record

from schema_registry.serializers import faust as serializer


class DummyRecord(Record):
    item: typing.Any


def test_avro_simple_record(client, avro_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, avro_country_schema)

    result = {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"}

    dummy = DummyRecord("test")
    assert result == faust_serializer.clean_payload(dummy)


def test_avro_nested_record(client, avro_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, avro_country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
    }

    dummy = DummyRecord(DummyRecord("test"))
    assert result == faust_serializer.clean_payload(dummy)


def test_avro_list_of_records(client, avro_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, avro_country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": [
            {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
            {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
        ],
    }

    dummy = DummyRecord([DummyRecord("test"), DummyRecord("test")])
    assert result == faust_serializer.clean_payload(dummy)


def test_avro_map_of_records(client, avro_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, avro_country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": {
            "key1": {
                "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
                "item": "test",
            },
            "key2": {
                "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
                "item": "test",
            },
        },
    }

    dummy = DummyRecord({"key1": DummyRecord("test"), "key2": DummyRecord("test")})
    assert result == faust_serializer.clean_payload(dummy)


def test_json_simple_record(client, json_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustJsonSerializer(client, schema_subject, json_country_schema)

    result = {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"}

    dummy = DummyRecord("test")
    assert result == faust_serializer.clean_payload(dummy)


def test_json_nested_record(client, json_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustJsonSerializer(client, schema_subject, json_country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
    }

    dummy = DummyRecord(DummyRecord("test"))
    assert result == faust_serializer.clean_payload(dummy)


def test_json_list_of_records(client, json_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustJsonSerializer(client, schema_subject, json_country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": [
            {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
            {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
        ],
    }

    dummy = DummyRecord([DummyRecord("test"), DummyRecord("test")])
    assert result == faust_serializer.clean_payload(dummy)


def test_json_map_of_records(client, json_country_schema):
    schema_subject = "test-avro-country"
    faust_serializer = serializer.FaustJsonSerializer(client, schema_subject, json_country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": {
            "key1": {
                "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
                "item": "test",
            },
            "key2": {
                "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
                "item": "test",
            },
        },
    }

    dummy = DummyRecord({"key1": DummyRecord("test"), "key2": DummyRecord("test")})
    assert result == faust_serializer.clean_payload(dummy)
