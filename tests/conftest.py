import os
import pytest
import logging

from schema_registry.client import SchemaRegistryClient, errors, schema
from schema_registry.serializers import MessageSerializer

logger = logging.getLogger(__name__)

flat_schemas = {
    "deployment_schema": {
        "type": "record",
        "namespace": "com.kubertenes",
        "name": "AvroDeployment",
        "fields": [
            {"name": "image", "type": "string"},
            {"name": "replicas", "type": "int"},
            {"name": "port", "type": "int"},
        ],
    },
    "country_schema": {
        "type": "record",
        "namespace": "com.example",
        "name": "AvroSomeSchema",
        "fields": [{"name": "country", "type": "string"}],
    },
    "user_schema_v3": {
        "type": "record",
        "name": "User",
        "aliases": ["UserKey"],
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": ["int", "null"], "default": 42},
            {"name": "favorite_color", "type": ["string", "null"], "default": "purple"},
            {"name": "country", "type": ["null", "string"], "default": None},
        ],
    },
}


class Response:
    def __init__(self, status_code, content=None):
        self.status_code = status_code

        if content is None:
            content = {}

        self.content = content

    def json(self):
        return self.content


@pytest.fixture
def response_klass():
    return Response


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
        "test-advance-schema",
        "test-user-schema",
        "subject-does-not-exist",
        "test-logical-types-schema",
        "test-schema-version",
    }

    # Executing the clean up. Delete all the subjects between tests.
    for subject in subjects:
        try:
            client.delete_subject(subject)
        except errors.ClientError as exc:
            logger.info(exc.message)


@pytest.fixture
def schemas():
    return flat_schemas


@pytest.fixture
def deployment_schema():
    return schema.AvroSchema(flat_schemas.get("deployment_schema"))


@pytest.fixture
def country_schema():
    return schema.AvroSchema(flat_schemas.get("country_schema"))


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
    return schema.AvroSchema(flat_schemas.get("user_schema_v3"))


@pytest.fixture
def message_serializer(client):
    return MessageSerializer(client)
