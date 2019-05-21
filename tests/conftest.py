import pytest

from tests.server import mock_registry

from avro.schema import SchemaFromJSONData

from schema_registry.client import SchemaRegistryClient
from schema_registry.serializer.message_serializer import MessageSerializer


@pytest.fixture
def client():
    server = mock_registry.ServerThread(0)
    server.start()
    yield SchemaRegistryClient(f"http://127.0.0.1:{server.server.server_port}")
    server.shutdown()
    server.join()


@pytest.fixture
def user_schema():
    return SchemaFromJSONData(
        {
            "type": "record",
            "namespace": "com.example",
            "name": "AvroUsers",
            "fields": [
                {"name": "first_name", "type": "string"},
                {"name": "last_name", "type": "string"},
                {"name": "age", "type": "int"},
            ],
        }
    )


@pytest.fixture
def country_schema():
    return SchemaFromJSONData(
        {
            "type": "record",
            "namespace": "com.example",
            "name": "AvroSomeSchema",
            "fields": [{"name": "country", "type": "string"}],
        }
    )


@pytest.fixture
def message_serializer(client):
    return MessageSerializer(client)
