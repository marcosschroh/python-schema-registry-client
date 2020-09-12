# import pickle
import os
from base64 import b64encode

import httpx
import pytest

from schema_registry.client import AsyncSchemaRegistryClient, utils


def test_invalid_cert():
    with pytest.raises(FileNotFoundError):
        AsyncSchemaRegistryClient(url="https://127.0.0.1:65534", cert_location="/path/to/cert")


def test_cert_with_key(certificates):
    client = AsyncSchemaRegistryClient(
        url="https://127.0.0.1:65534",
        cert_location=certificates["certificate"],
        key_location=certificates["key"],
        key_password=certificates["password"],
    )

    assert client.conf[utils.SSL_CERTIFICATE_LOCATION] == certificates["certificate"]
    assert client.conf[utils.SSL_KEY_LOCATION] == certificates["key"]
    assert client.conf[utils.SSL_KEY_PASSWORD] == certificates["password"]


def test_custom_headers():
    extra_headers = {"custom-serialization": utils.HEADER_AVRO_JSON}

    client = AsyncSchemaRegistryClient(url="https://127.0.0.1:65534", extra_headers=extra_headers)
    assert extra_headers == client.extra_headers


@pytest.mark.asyncio
async def test_override_headers(deployment_schema, response_klass, async_mock):
    extra_headers = {"custom-serialization": utils.HEADER_AVRO_JSON}
    async_client = AsyncSchemaRegistryClient(url=os.getenv("SCHEMA_REGISTRY_URL"), extra_headers=extra_headers)

    assert async_client.session.headers.get("custom-serialization") == utils.HEADER_AVRO_JSON

    subject = "test"
    override_header = {"custom-serialization": utils.HEADER_AVRO}

    mock = async_mock(httpx.AsyncClient, "request", returned_value=response_klass(200, content={"id": 1}))

    with mock:
        await async_client.register(subject, deployment_schema, headers=override_header)

        prepare_headers = async_client.prepare_headers(body="1")
        prepare_headers["custom-serialization"] = utils.HEADER_AVRO

        mock.assert_called_with(headers=prepare_headers)


def test_cert_path():
    client = AsyncSchemaRegistryClient(url="https://127.0.0.1:65534", ca_location=True)

    assert client.conf[utils.SSL_CA_LOCATION]


def test_init_with_dict(certificates):
    client = AsyncSchemaRegistryClient(
        {
            "url": "https://127.0.0.1:65534",
            "ssl.certificate.location": certificates["certificate"],
            "ssl.key.location": certificates["key"],
            "ssl.key.password": "test",
        }
    )
    assert "https://127.0.0.1:65534/" == client.url_manager.url


def test_empty_url():
    with pytest.raises(AssertionError):
        AsyncSchemaRegistryClient({"url": ""})


def test_invalid_type_url():
    with pytest.raises(AttributeError):
        AsyncSchemaRegistryClient(url=1)


def test_invalid_type_url_dict():
    with pytest.raises(AttributeError):
        AsyncSchemaRegistryClient({"url": 1})


def test_basic_auth_url():
    username = "secret-user"
    password = "secret"
    client = AsyncSchemaRegistryClient({"url": f"https://{username}:{password}@127.0.0.1:65534"})
    userpass = b":".join((httpx._utils.to_bytes(username), httpx._utils.to_bytes(password)))
    token = b64encode(userpass).decode()

    assert client.session.auth._auth_header == f"Basic {token}"


def test_basic_auth_user_info():
    username = "secret-user"
    password = "secret"
    client = AsyncSchemaRegistryClient(
        {
            "url": "https://user_url:secret_url@127.0.0.1:65534",
            "basic.auth.credentials.source": "user_info",
            "basic.auth.user.info": f"{username}:{password}",
        }
    )

    userpass = b":".join((httpx._utils.to_bytes(username), httpx._utils.to_bytes(password)))
    token = b64encode(userpass).decode()

    assert client.session.auth._auth_header == f"Basic {token}"


def test_basic_auth_sasl_inherit():
    username = "secret-user-sasl"
    password = "secret-sasl"
    client = AsyncSchemaRegistryClient(
        {
            "url": "https://user_url:secret_url@127.0.0.1:65534",
            "basic.auth.credentials.source": "SASL_INHERIT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
        }
    )

    userpass = b":".join((httpx._utils.to_bytes(username), httpx._utils.to_bytes(password)))
    token = b64encode(userpass).decode()

    assert client.session.auth._auth_header == f"Basic {token}"


def test_basic_auth_invalid():
    with pytest.raises(ValueError):
        AsyncSchemaRegistryClient(
            {"url": "https://user_url:secret_url@127.0.0.1:65534", "basic.auth.credentials.source": "VAULT"}
        )
