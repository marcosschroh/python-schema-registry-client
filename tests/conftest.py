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
def user_schema_v3():
    """
    The user V2 is:
    {
        "type": "record",
        "name": "User",
        "aliases": ["UserKey"],
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number",  "type": ["int", "null"], "default": 42},
            {"name": "favorite_color", "type": ["string", "null"], "default": "purple"}
        ]
    }
    """
    return SchemaFromJSONData(
        {
            "type": "record",
            "name": "User",
            "aliases": ["UserKey"],
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number", "type": ["int", "null"], "default": 42},
                {
                    "name": "favorite_color",
                    "type": ["string", "null"],
                    "default": "purple",
                },
                {"name": "country", "type": ["null", "string"], "default": None},
            ],
        }
    )


@pytest.fixture
def message_serializer(client):
    return MessageSerializer(client)
