import pytest
import requests

from schema_registry.client import SchemaRegistryClient, load

from tests.client import data_gen


def assertLatest(self, meta_tuple, sid, schema, version):
    self.assertNotEqual(sid, -1)
    self.assertNotEqual(version, -1)
    self.assertEqual(meta_tuple[0], sid)
    self.assertEqual(meta_tuple[1], schema)
    self.assertEqual(meta_tuple[2], version)


def test_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    schema_id = client.register("test-basic-schema", parsed)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1


def test_register_json_data(client, deployment_schema):
    schema_id = client.register("test-deployment", deployment_schema)
    assert schema_id > 0


def test_register_with_custom_headers(client, country_schema):
    headers = {"custom-serialization": "application/x-avro-json"}
    schema_id = client.register("test-country", country_schema, headers=headers)
    assert schema_id > 0


def test_multi_subject_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    schema_id = client.register("test-basic-schema", parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = client.register("test-basic-schema-backup", parsed)
    assert schema_id == dupe_id
    assert len(client.id_to_schema) == 1


def test_dupe_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    subject = "test-basic-schema"
    schema_id = client.register(subject, parsed)

    assert schema_id > 0
    latest = client.get_schema(subject)

    # register again under same subject
    dupe_id = client.register(subject, parsed)
    assert schema_id == dupe_id

    dupe_latest = client.get_schema(subject)
    assert latest == dupe_latest


def test_getters(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    subject = "subject-does-not-exist"
    version = client.check_version(subject, parsed)
    assert version is None

    # There is already a registered schema with id 1
    schema = client.get_by_id(1)
    assert schema is not None

    latest = client.get_schema(subject)
    assert latest == (None, None, None, None)

    # register
    schema_id = client.register(subject, parsed)
    schema = client.get_schema(subject, schema_id)
    assert schema is not None

    latest = client.get_schema(subject)
    version = client.check_version(subject, parsed)
    fetched = client.get_by_id(schema_id)

    assert fetched == parsed


def test_multi_register(client):
    """
    Register two different schemas under the same subject
    with backwards compatibility
    """
    version_1 = load.loads(data_gen.USER_V1)
    version_2 = load.loads(data_gen.USER_V2)
    subject = "test-user-schema"

    id1 = client.register(subject, version_1)
    latest_schema_1 = client.get_schema(subject)
    client.check_version(subject, version_1)

    id2 = client.register(subject, version_2)
    latest_schema_2 = client.get_schema(subject)
    client.check_version(subject, version_2)

    assert id1 != id2
    assert latest_schema_1 != latest_schema_2
    # ensure version is higher
    assert latest_schema_1.version < latest_schema_2.version

    client.register(subject, version_1)
    latest_schema_3 = client.get_schema(subject)
    # latest should not change with a re-reg
    assert latest_schema_2 == latest_schema_3


def test_compatibility(client, user_schema_v3):
    """
    Test the compatibility of a new User Schema against the latest one.
    The last user schema is the V2.
    """
    compatibility = client.test_compatibility("test-user-schema", user_schema_v3)
    assert compatibility


def test_update_compatibility(client):
    """
    The latest User V2 schema is  BACKWARD and FORWARDFULL compatibility (FULL).
    So, we can ipdate compatibility level for the specified subject.
    """
    assert client.update_compatibility("FULL", "test-user-schema") == "FULL"


def test_get_compatibility(client):
    """
    Test latest compatibility for test-user-schema subject
    """
    assert client.get_compatibility("test-user-schema") == "FULL"


def test_delete_subject(client, user_schema_v3):
    subject = "subject-to-delete"
    versions = [load.loads(data_gen.USER_V1), load.loads(data_gen.USER_V2)]

    for version in versions:
        client.register(subject, version)

    assert client.delete_subject(subject) == [1, 2]


def test_context(client):
    with client as c:
        parsed = load.loads(data_gen.BASIC_SCHEMA)
        schema_id = c.register("test-basic-schema", parsed)
        assert schema_id > 0
        assert len(c.id_to_schema) == 1


def test_cert_no_key():
    with pytest.raises(ValueError):
        SchemaRegistryClient(
            url="https://127.0.0.1:65534", cert_location="/path/to/cert"
        )


def test_cert_with_key():
    client = SchemaRegistryClient(
        url="https://127.0.0.1:65534",
        cert_location="/path/to/cert",
        key_location="/path/to/key",
    )

    assert ("/path/to/cert", "/path/to/key") == client.cert


def test_custom_headers():
    extra_headers = {"custom-serialization": "application/x-avro-json"}

    client = SchemaRegistryClient(
        url="https://127.0.0.1:65534", extra_headers=extra_headers
    )
    assert extra_headers == client.extra_headers


def test_override_headers(client, deployment_schema, mocker):
    extra_headers = {"custom-serialization": "application/x-avro-json"}
    client = SchemaRegistryClient(
        "https://127.0.0.1:65534", extra_headers=extra_headers
    )

    assert (
        client.prepare_headers().get("custom-serialization")
        == "application/x-avro-json"
    )

    class Response:
        def __init__(self, status_code, content=None):
            self.status_code = status_code

            if content is None:
                content = {}

            self.content = content

        def json(self):
            return self.content

    subject = "test"
    override_header = {"custom-serialization": "application/avro"}

    request_patch = mocker.patch.object(
        requests.sessions.Session,
        "request",
        return_value=Response(200, content={"id": 1}),
    )
    client.register(subject, deployment_schema, headers=override_header)

    prepare_headers = client.prepare_headers(body="1")
    prepare_headers["custom-serialization"] = "application/avro"

    request_patch.assert_called_once_with(
        "POST", mocker.ANY, headers=prepare_headers, json=mocker.ANY
    )


def test_cert_path():
    client = SchemaRegistryClient(
        url="https://127.0.0.1:65534", ca_location="/path/to/ca"
    )

    assert "/path/to/ca" == client.verify


def test_init_with_dict():
    client = SchemaRegistryClient(
        {
            "url": "https://127.0.0.1:65534",
            "ssl.certificate.location": "/path/to/cert",
            "ssl.key.location": "/path/to/key",
        }
    )
    assert "https://127.0.0.1:65534" == client.url


def test_empty_url():
    with pytest.raises(ValueError):
        SchemaRegistryClient({"url": ""})


def test_invalid_type_url():
    with pytest.raises(TypeError):
        SchemaRegistryClient(url=1)


def test_invalid_type_url_dict():
    with pytest.raises(TypeError):
        SchemaRegistryClient({"url": 1})


def test_invalid_url():
    with pytest.raises(ValueError):
        SchemaRegistryClient({"url": "example.com:65534"})


def test_basic_auth_url():
    client = SchemaRegistryClient(
        {"url": "https://user_url:secret_url@127.0.0.1:65534"}
    )

    assert ("user_url", "secret_url") == client.auth


def test_basic_auth_userinfo():
    client = SchemaRegistryClient(
        {
            "url": "https://user_url:secret_url@127.0.0.1:65534",
            "basic.auth.credentials.source": "user_info",
            "basic.auth.user.info": "user_userinfo:secret_userinfo",
        }
    )
    assert ("user_userinfo", "secret_userinfo") == client.auth


def test_basic_auth_sasl_inherit():
    client = SchemaRegistryClient(
        {
            "url": "https://user_url:secret_url@127.0.0.1:65534",
            "basic.auth.credentials.source": "SASL_INHERIT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "user_sasl",
            "sasl.password": "secret_sasl",
        }
    )
    assert ("user_sasl", "secret_sasl") == client.auth


def test_basic_auth_invalid():
    with pytest.raises(ValueError):
        SchemaRegistryClient(
            {
                "url": "https://user_url:secret_url@127.0.0.1:65534",
                "basic.auth.credentials.source": "VAULT",
            }
        )


def test_invalid_conf():
    with pytest.raises(ValueError):
        SchemaRegistryClient(
            {
                "url": "https://user_url:secret_url@127.0.0.1:65534",
                "basic.auth.credentials.source": "SASL_INHERIT",
                "sasl.username": "user_sasl",
                "sasl.password": "secret_sasl",
                "invalid.conf": 1,
                "invalid.conf2": 2,
            }
        )
