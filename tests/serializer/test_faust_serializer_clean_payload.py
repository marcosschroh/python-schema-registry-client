import typing

from faust import Record

from schema_registry.serializers import faust_serializer as serializer


class DummyRecord(Record):
    item: typing.Any


def test_simple_record(client, country_schema):
    schema_subject = "test-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, country_schema)

    result = {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"}

    dummy = DummyRecord("test")
    assert result == faust_serializer.clean_payload(dummy)


def test_nested_record(client, country_schema):
    schema_subject = "test-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
    }

    dummy = DummyRecord(DummyRecord("test"))
    assert result == faust_serializer.clean_payload(dummy)


def test_list_of_records(client, country_schema):
    schema_subject = "test-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, country_schema)

    result = {
        "__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"},
        "item": [
            {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
            {"__faust": {"ns": "tests.serializer.test_faust_serializer_clean_payload.DummyRecord"}, "item": "test"},
        ],
    }

    dummy = DummyRecord([DummyRecord("test"), DummyRecord("test")])
    assert result == faust_serializer.clean_payload(dummy)


def test_map_of_records(client, country_schema):
    schema_subject = "test-country"
    faust_serializer = serializer.FaustSerializer(client, schema_subject, country_schema)

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
