import httpx
import pytest

from schema_registry.client import errors, schema, utils
from tests import data_gen


def test_avro_compatibility(client, avro_user_schema_v3):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "test-avro-user-schema"
    version_2 = schema.AvroSchema(data_gen.AVRO_USER_V2)
    client.register(subject, version_2)

    compatibility = client.test_compatibility(subject, avro_user_schema_v3)
    assert compatibility


def test_avro_compatibility_dataclasses_avroschema(client, dataclass_avro_schema, dataclass_avro_schema_advance):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "dataclasses-avroschema-subject"
    client.register(subject, dataclass_avro_schema.avro_schema())

    compatibility = client.test_compatibility(subject, dataclass_avro_schema_advance.avro_schema())
    assert compatibility


def test_avro_update_compatibility_for_subject(client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert client.update_compatibility("FULL", "test-avro-user-schema")


def test_avro_update_global_compatibility(client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert client.update_compatibility("FULL")


def test_avro_update_compatibility_fail(client, response_klass, mocker):
    http_code = 404
    mocker.patch.object(httpx.Client, "request", return_value=response_klass(http_code))

    with pytest.raises(errors.ClientError) as excinfo:
        client.update_compatibility("FULL", "test-avro-user-schema")

        assert excinfo.http_code == http_code


def test_avro_get_compatibility_for_subject(client):
    """
    Test latest compatibility for test-avro-user-schema subject
    """
    assert client.get_compatibility("test-avro-user-schema") == "FULL"


def test_avro_get_global_compatibility(client):
    """
    Test latest compatibility for test-avro-user-schema subject
    """
    assert client.get_compatibility() is not None


def test_json_compatibility(client, json_user_schema_v3):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "test-json-user-schema"
    version_2 = schema.JsonSchema(data_gen.JSON_USER_V2)
    client.register(subject, version_2)

    compatibility = client.test_compatibility(subject, json_user_schema_v3)

    assert compatibility


def test_json_compatibility_dataclasses_jsonschema(client, dataclass_json_schema, dataclass_json_schema_advance):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "dataclasses-jsonschema-subject"
    client.register(subject, dataclass_json_schema.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE)

    compatibility = client.test_compatibility(
        subject, dataclass_json_schema_advance.schema_json(), schema_type=utils.JSON_SCHEMA_TYPE
    )

    assert compatibility


def test_json_update_compatibility_for_subject(client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert client.update_compatibility("FULL", "test-json-user-schema")


def test_json_update_global_compatibility(client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert client.update_compatibility("FULL")


def test_json_update_compatibility_fail(client, response_klass, mocker):
    http_code = 404
    mocker.patch.object(httpx.Client, "request", return_value=response_klass(http_code))

    with pytest.raises(errors.ClientError) as excinfo:
        client.update_compatibility("FULL", "test-json-user-schema")

        assert excinfo.http_code == http_code


def test_json_get_compatibility_for_subject(client):
    """
    Test latest compatibility for test-json-user-schema subject
    """
    assert client.get_compatibility("test-json-user-schema") == "FULL"


def test_json_get_global_compatibility(client):
    """
    Test latest compatibility for test-json-user-schema subject
    """
    assert client.get_compatibility() is not None
