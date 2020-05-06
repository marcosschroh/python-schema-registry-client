import httpx
import pytest

from schema_registry.client import errors, schema
from tests import data_gen


def test_compatibility(client, user_schema_v3):
    """
    Test the compatibility of a new User Schema against the User schema version 2.
    """
    subject = "test-user-schema"
    version_2 = schema.AvroSchema(data_gen.USER_V2)
    client.register(subject, version_2)

    compatibility = client.test_compatibility(subject, user_schema_v3)
    assert compatibility


def test_update_compatibility_for_subject(client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert client.update_compatibility("FULL", "test-user-schema")


def test_update_global_compatibility(client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert client.update_compatibility("FULL")


def test_update_compatibility_fail(client, response_klass, mocker):
    http_code = 404
    mocker.patch.object(httpx.Client, "request", return_value=response_klass(http_code))

    with pytest.raises(errors.ClientError) as excinfo:
        client.update_compatibility("FULL", "test-user-schema")

        assert excinfo.http_code == http_code


def test_get_compatibility_for_subject(client):
    """
    Test latest compatibility for test-user-schema subject
    """
    assert client.get_compatibility("test-user-schema") == "FULL"


def test_get_global_compatibility(client):
    """
    Test latest compatibility for test-user-schema subject
    """
    assert client.get_compatibility() is not None
