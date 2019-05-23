import os
import pytest

from avro.schema import SchemaFromJSONData

from schema_registry.client import SchemaRegistryClient
from schema_registry.serializer.message_serializer import MessageSerializer


@pytest.fixture
def client():
    url = os.getenv("SCHEMA_REGISTRY_URL")
    yield SchemaRegistryClient(url)


@pytest.fixture
def deployment_schema():
    return SchemaFromJSONData(
        {
            "type": "record",
            "namespace": "com.kubertenes",
            "name": "AvroDeployment",
            "fields": [
                {"name": "image", "type": "string"},
                {"name": "replicas", "type": "int"},
                {"name": "port", "type": "int"},
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
