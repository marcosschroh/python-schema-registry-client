import os
import pytest
import logging

from schema_registry.client import SchemaRegistryClient, errors, schema
from schema_registry.serializers import MessageSerializer

logger = logging.getLogger(__name__)


@pytest.fixture
def client():
    url = os.getenv("SCHEMA_REGISTRY_URL")
    client = SchemaRegistryClient(url)
    yield client

    subjects = {
        "test-basic-schema",
        "test-deployment",
        "test-country",
        "test-basic-schema-backup",
        "test-user-schema",
        "subject-does-not-exist",
        "test-logical-types-schema",
    }

    # Executing the clean up. Delete all the subjects between tests.
    for subject in subjects:
        try:
            client.delete_subject(subject)
        except errors.ClientError as exc:
            logger.info(exc.message)


@pytest.fixture
def deployment_schema():
    return schema.AvroSchema(
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
    return schema.AvroSchema(
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
    return schema.AvroSchema(
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
