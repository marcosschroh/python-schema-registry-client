import pickle
from base64 import b64encode

import httpx
import pytest
from httpx import USE_CLIENT_DEFAULT

from schema_registry.client import Auth, SchemaRegistryClient, schema, utils
from tests import data_gen


def test_invalid_cert():
    client = SchemaRegistryClient(url="https://127.0.0.1:65534", cert_location="/path/to/cert")
    with pytest.raises(FileNotFoundError):
        client.request("https://example.com")


def test_cert_with_key(certificates):
    client = SchemaRegistryClient(
        url="https://127.0.0.1:65534",
        cert_location=certificates["certificate"],
        key_location=certificates["key"],
        key_password=certificates["password"],
    )

    assert client.conf[utils.SSL_CERTIFICATE_LOCATION] == certificates["certificate"]
    assert client.conf[utils.SSL_KEY_LOCATION] == certificates["key"]
    assert client.conf[utils.SSL_KEY_PASSWORD] == certificates["password"]


def test_pickelable(client):
    unpickled_client = pickle.loads(pickle.dumps(client))

    assert client == unpickled_client

    # make sure that is possible to do client operations with unpickled_client
    subject = "test-avro-basic-schema"
    parsed = schema.AvroSchema(data_gen.AVRO_BASIC_SCHEMA)
    unpickled_client.get_subjects()
    schema_id = unpickled_client.register(subject, parsed)

    assert schema_id > 0
    assert unpickled_client.delete_subject(subject)


def test_custom_headers():
    extra_headers = {"custom-serialization": utils.HEADER_AVRO_JSON}

    client = SchemaRegistryClient(url="https://127.0.0.1:65534", extra_headers=extra_headers)
    assert extra_headers == client.extra_headers


def test_custom_httpx_config():
    """
    Test the SchemaRegistryClient creation with custom httpx config
    """
    timeout = httpx.Timeout(10.0, connect=60.0)
    pool_limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)

    client = SchemaRegistryClient(
        url="https://127.0.0.1:65534",
        timeout=timeout,
        pool_limits=pool_limits,
    )

    assert client.timeout == timeout
    assert client.pool_limits == pool_limits


def test_override_headers(client, avro_deployment_schema, mocker, response_klass):
    extra_headers = {"custom-serialization": utils.HEADER_AVRO_JSON}
    client = SchemaRegistryClient("https://127.0.0.1:65534", extra_headers=extra_headers)

    response = client.request("https://example.com")
    assert response.request.headers.get("custom-serialization") == utils.HEADER_AVRO_JSON

    subject = "test"
    override_header = {"custom-serialization": utils.HEADER_AVRO}

    request_patch = mocker.patch.object(httpx.Client, "request", return_value=response_klass(200, content={"id": 1}))
    client.register(subject, avro_deployment_schema, headers=override_header)

    prepare_headers = client.prepare_headers(body="1")
    prepare_headers["custom-serialization"] = utils.HEADER_AVRO

    request_patch.assert_called_once_with(
        "POST", mocker.ANY, headers=prepare_headers, json=mocker.ANY, timeout=USE_CLIENT_DEFAULT
    )


def test_cert_path():
    client = SchemaRegistryClient(url="https://127.0.0.1:65534", ca_location=True)

    assert client.conf[utils.SSL_CA_LOCATION]


def test_init_with_dict(certificates):
    client = SchemaRegistryClient(
        {
            "url": "https://127.0.0.1:65534",
            "ssl.certificate.location": certificates["certificate"],
            "ssl.key.location": certificates["key"],
            "ssl.key.password": certificates["password"],
        }
    )
    assert "https://127.0.0.1:65534/" == client.url_manager.url


def test_empty_url():
    with pytest.raises(AssertionError):
        SchemaRegistryClient({"url": ""})


def test_invalid_type_url():
    with pytest.raises(AttributeError):
        SchemaRegistryClient(url=1)


def test_invalid_type_url_dict():
    with pytest.raises(AttributeError):
        SchemaRegistryClient({"url": 1})


def test_invalid_url():
    with pytest.raises(AssertionError):
        SchemaRegistryClient({"url": "example.com:65534"})


def test_basic_auth_url():
    username = "secret-user"
    password = "secret"
    client = SchemaRegistryClient({"url": f"https://{username}:{password}@127.0.0.1:65534"})
    userpass = b":".join((httpx._utils.to_bytes(username), httpx._utils.to_bytes(password)))
    token = b64encode(userpass).decode()
    response = client.request("https://example.com")
    assert response.request.headers.get("Authorization") == f"Basic {token}"


def test_basic_auth_user_info():
    username = "secret-user"
    password = "secret"
    client = SchemaRegistryClient(
        {
            "url": "https://user_url:secret_url@127.0.0.1:65534",
            "basic.auth.credentials.source": "user_info",
            "basic.auth.user.info": f"{username}:{password}",
        }
    )

    userpass = b":".join((httpx._utils.to_bytes(username), httpx._utils.to_bytes(password)))
    token = b64encode(userpass).decode()
    response = client.request("https://example.com")
    assert response.request.headers.get("Authorization") == f"Basic {token}"


def test_auth():
    username = "secret-user"
    password = "secret"
    client = SchemaRegistryClient(
        url="https://user_url:secret_url@127.0.0.1:65534",
        auth=Auth(username=username, password=password),
    )

    userpass = b":".join((httpx._utils.to_bytes(username), httpx._utils.to_bytes(password)))
    token = b64encode(userpass).decode()
    response = client.request("https://example.com")
    assert response.request.headers.get("Authorization") == f"Basic {token}"


def test_basic_auth_invalid():
    with pytest.raises(ValueError):
        SchemaRegistryClient(
            {"url": "https://user_url:secret_url@127.0.0.1:65534", "basic.auth.credentials.source": "VAULT"}
        )
