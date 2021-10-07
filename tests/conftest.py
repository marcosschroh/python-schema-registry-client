import dataclasses
import logging
import os
import typing
from collections import namedtuple

import pytest
import pydantic
from dataclasses_avroschema import AvroModel, types
from httpx._client import UNSET, TimeoutTypes, UnsetType

from schema_registry.client import AsyncSchemaRegistryClient, SchemaRegistryClient, errors, schema, utils
from schema_registry.serializers import AvroMessageSerializer, JsonMessageSerializer

logger = logging.getLogger(__name__)

CERTIFICATES_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "certificates")

flat_schemas = {
    "avro_deployment_schema": {
        "type": "record",
        "namespace": "com.kubertenes",
        "name": "AvroDeployment",
        "fields": [
            {"name": "image", "type": "string"},
            {"name": "replicas", "type": "int"},
            {"name": "port", "type": "int"},
        ],
    },
    "avro_country_schema": {
        "type": "record",
        "namespace": "com.example",
        "name": "AvroSomeSchema",
        "fields": [{"name": "country", "type": "string"}],
    },
    "avro_user_schema_v3": {
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
    "json_deployment_schema": {
        "definitions": {
            "record:com.kubertenes.JsonDeployment": {
                "type": "object",
                "required": ["image", "replicas", "port"],
                "additionalProperties": True,
                "properties": {
                    "image": {"type": "string"},
                    "replicas": {"type": "integer"},
                    "port": {"type": "integer"},
                },
            }
        },
        "$ref": "#/definitions/record:com.kubertenes.JsonDeployment",
    },
    "json_country_schema": {
        "definitions": {
            "record:com.example.JsonSomeSchema": {
                "type": "object",
                "required": ["country"],
                "additionalProperties": True,
                "properties": {"country": {"type": "string"}},
            }
        },
        "$ref": "#/definitions/record:com.example.JsonSomeSchema",
    },
    "json_user_schema_v3": {
        "definitions": {
            "record:User": {
                "type": "object",
                "required": ["name", "favorite_number", "favorite_color", "country"],
                "additionalProperties": {"default": "null", "oneOf": [{"type": "null"}, {"type": "string"}]},
                "properties": {
                    "name": {"type": "string"},
                    "favorite_number": {"default": 42, "oneOf": [{"type": "integer"}, {"type": "null"}]},
                    "favorite_color": {"default": "purple", "oneOf": [{"type": "string"}, {"type": "null"}]},
                    "country": {
                        "default": None,
                        "oneOf": [{"type": "null"}, {"type": "string"}],
                    },
                },
            }
        },
        "$ref": "#/definitions/record:User",
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


RequestArgs = namedtuple("RequestArgs", ["url", "method", "body", "headers", "timeout"])


class RequestLoggingAssertMixin(object):
    def assert_url_suffix(self, call_no: int, url_suffix: str) -> None:
        args = self.request_calls[call_no]
        assert args.url.startswith(self.conf[utils.URL])
        assert args.url[len(self.conf[utils.URL]) :] == url_suffix

    def assert_method(self, call_no: int, method: str) -> None:
        assert self.request_calls[call_no].method == method


class RequestLoggingSchemaRegistryClient(SchemaRegistryClient, RequestLoggingAssertMixin):
    def __init__(self, url, *args, **kwargs):
        self.request_calls = []
        super(SchemaRegistryClient, self).__init__(url, *args, **kwargs)

    def request(
        self,
        url: str,
        method: str = "GET",
        body: dict = None,
        headers: dict = None,
        timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET,
    ) -> tuple:
        self.request_calls.append(RequestArgs(url, method, body, headers, timeout))
        return super().request(url, method, body, headers=headers, timeout=timeout)


@pytest.fixture
def client():
    url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    client = RequestLoggingSchemaRegistryClient(url)
    yield client

    subjects = {
        "test-avro-basic-schema",
        "test-json-basic-schema",
        "test-avro-deployment",
        "test-json-deployment",
        "test-avro-country",
        "test-json-country",
        "test-avro-basic-schema-backup",
        "test-json-basic-schema-backup",
        "test-avro-advance-schema",
        "test-json-advance-schema",
        "test-avro-user-schema",
        "test-json-user-schema",
        "subject-does-not-exist",
        "test-logical-types-schema",
        "test-avro-schema-version",
        "test-json-schema-version",
        "test-avro-nested-schema",
        "test-json-nested-schema",
        "test-dataclasses-avroschema",
        "test-dataclasses-jsonschema",
        "test-union-field-avroschema",
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
def avro_deployment_schema():
    return schema.AvroSchema(flat_schemas.get("avro_deployment_schema"))


@pytest.fixture
def avro_country_schema():
    return schema.AvroSchema(flat_schemas.get("avro_country_schema"))


@pytest.fixture
def avro_user_schema_v3():
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
    return schema.AvroSchema(flat_schemas.get("avro_user_schema_v3"))


@pytest.fixture
def json_deployment_schema():
    return schema.JsonSchema(flat_schemas.get("json_deployment_schema"))


@pytest.fixture
def json_country_schema():
    return schema.JsonSchema(flat_schemas.get("json_country_schema"))


@pytest.fixture
def json_user_schema_v3():
    return schema.JsonSchema(flat_schemas.get("json_user_schema_v3"))


@pytest.fixture
def avro_message_serializer(client):
    return AvroMessageSerializer(client)


@pytest.fixture
def json_message_serializer(client):
    return JsonMessageSerializer(client)


@pytest.fixture
def certificates():
    return {
        "certificate": os.path.join(CERTIFICATES_DIR, "cert.pem"),
        "key": os.path.join(CERTIFICATES_DIR, "key.pem"),
        "password": "test",
    }


class AsyncMock:
    def __init__(self, module, func, returned_value=None):
        self.module = module
        self.func = func
        self.returned_value = returned_value
        self.original_object = getattr(module, func)
        self.args_called_with = None
        self.kwargs_called_with = None

    def __enter__(self):
        setattr(self.module, self.func, self.mock)

    def __exit__(self, exception_type, exception_value, traceback):
        setattr(self.module, self.func, self.original_object)

    def assert_called_with(self, **kwargs):
        for key, value in kwargs.items():
            assert self.kwargs_called_with[key] == value

    async def mock(self, *args, **kwargs):
        self.args_called_with = args
        self.kwargs_called_with = kwargs

        return self.returned_value


@pytest.fixture
def async_mock():
    return AsyncMock


class RequestLoggingAsyncSchemaRegistryClient(AsyncSchemaRegistryClient, RequestLoggingAssertMixin):
    def __init__(self, url, *args, **kwargs):
        self.request_calls = []
        super(AsyncSchemaRegistryClient, self).__init__(url, *args, **kwargs)

    async def request(
        self,
        url: str,
        method: str = "GET",
        body: dict = None,
        headers: dict = None,
        timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET,
    ) -> tuple:
        self.request_calls.append(RequestArgs(url, method, body, headers, timeout))
        return await super().request(url, method, body, headers=headers, timeout=timeout)


@pytest.fixture
async def async_client():
    url = os.getenv("SCHEMA_REGISTRY_URL")
    client = RequestLoggingAsyncSchemaRegistryClient(url)
    yield client

    subjects = {
        "test-avro-basic-schema",
        "test-json-basic-schema",
        "test-avro-deployment",
        "test-json-deployment",
        "test-avro-country",
        "test-json-country",
        "test-avro-basic-schema-backup",
        "test-json-basic-schema-backup",
        "test-avro-advance-schema",
        "test-json-advance-schema",
        "test-avro-user-schema",
        "test-json-user-schema",
        "subject-does-not-exist",
        "test-logical-types-schema",
        "test-avro-schema-version",
        "test-json-schema-version",
        "test-avro-nested-schema",
        "test-json-nested-schema",
        "test-dataclasses-avroschema",
        "test-dataclasses-jsonschema",
        "test-union-field-avroschema",
        "test-union-field-jsonschema",
    }

    # Executing the clean up. Delete all the subjects between tests.
    for subject in subjects:
        try:
            await client.delete_subject(subject)
        except errors.ClientError as exc:
            logger.info(exc.message)


@pytest.fixture
def dataclass_avro_schema():
    @dataclasses.dataclass
    class UserAdvance(AvroModel):
        name: str
        age: int
        pets: typing.List[str] = dataclasses.field(default_factory=lambda: ["dog", "cat"])
        accounts: typing.Dict[str, int] = dataclasses.field(default_factory=lambda: {"key": 1})
        has_car: bool = False

    return UserAdvance


@pytest.fixture
def dataclass_avro_schema_advance():
    @dataclasses.dataclass
    class UserAdvance(AvroModel):
        name: str
        age: int
        pets: typing.List[str] = dataclasses.field(default_factory=lambda: ["dog", "cat"])
        accounts: typing.Dict[str, int] = dataclasses.field(default_factory=lambda: {"key": 1})
        has_car: bool = False
        favorite_colors: types.Enum = types.Enum(["BLUE", "YELLOW", "GREEN"], default="BLUE")
        address: str = None

    return UserAdvance


@pytest.fixture
def dataclass_json_schema():
    class UserAdvance(pydantic.BaseModel):
        name: str
        age: int
        pets: typing.List[str] = pydantic.Field(default_factory=lambda: ["dog", "cat"])
        accounts: typing.Dict[str, int] = pydantic.Field(default_factory=lambda: {"key": 1})
        has_car: bool = False

        class Config:
            schema_extra = {"additionalProperties": {"type": "string"}}

    return UserAdvance


@pytest.fixture
def dataclass_json_schema_advance():
    class UserAdvance(pydantic.BaseModel):
        name: str
        age: int
        pets: typing.List[str] = pydantic.Field(default_factory=lambda: ["dog", "cat"])
        accounts: typing.Dict[str, int] = pydantic.Field(default_factory=lambda: {"key": 1})
        has_car: bool = False
        address: str = None

        class Config:
            schema_extra = {"additionalProperties": {"type": "string"}}

    return UserAdvance
