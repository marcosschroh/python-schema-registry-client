import io
import json
import logging
import struct
import sys
import traceback
import typing
from abc import ABC, abstractmethod

from fastavro import schemaless_reader, schemaless_writer
from jsonschema import validate

from schema_registry.client import AsyncSchemaRegistryClient, SchemaRegistryClient, schema, utils
from schema_registry.client.errors import ClientError
from schema_registry.client.schema import BaseSchema

from .errors import SerializerError

log = logging.getLogger(__name__)

MAGIC_BYTE = 0


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self) -> "ContextStringIO":
        return self

    def __exit__(self, *args: typing.Any) -> None:
        self.close()


class MessageSerializer(ABC):
    """
    A helper class that can serialize and deserialize messages asynchronously

    Attributes:
        schemaregistry_client schema_registry.client.AsyncSchemaRegistryClient: Http Client
        reader_schema schema_registry.schema.AvroSchema: Specify a schema to decode the message
        return_record_name bool: If the record name should be returned
    """

    def __init__(
        self,
        schemaregistry_client: SchemaRegistryClient,
        reader_schema: typing.Optional[schema.AvroSchema] = None,
        return_record_name: bool = False,
    ):
        self.schemaregistry_client = schemaregistry_client
        self.id_to_decoder_func = {}  # type: typing.Dict
        self.id_to_writers = {}  # type: typing.Dict
        self.reader_schema = reader_schema
        self.return_record_name = return_record_name

    @property
    @abstractmethod
    def _serializer_schema_type(self) -> str:
        ...

    @abstractmethod
    def _get_encoder_func(self, schema: BaseSchema) -> typing.Callable:
        ...

    @abstractmethod
    def _get_decoder_func(self, payload: ContextStringIO, writer_schema: BaseSchema) -> typing.Callable:
        ...

    def encode_record_with_schema(self, subject: str, schema: typing.Union[BaseSchema], record: dict) -> bytes:
        """
        Given a parsed avro schema, encode a record for the given subject.
        The record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'

        Attributes:
            subject str: Subject name
            schema avro.schema.RecordSchema: Avro Schema
            record typing.Dict: An object to serialize

        Returns:
            Encoded record with schema ID as bytes
        """
        # Try to register the schema
        schema_id = self.schemaregistry_client.register(subject, schema, schema_type=self._serializer_schema_type)

        # cache writer
        if not self.id_to_writers.get(schema_id):
            self.id_to_writers[schema_id] = self._get_encoder_func(schema)

        return self.encode_record_with_schema_id(schema_id, record)

    def encode_record_with_schema_id(self, schema_id: int, record: dict) -> bytes:
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.

        Attributes:
            schema_id int: integer ID
            record typing.Dict: An object to serialize

        Returns:
            Decoder function
        """
        # use slow avro
        if schema_id not in self.id_to_writers:
            try:
                schema = self.schemaregistry_client.get_by_id(schema_id)
                if not schema:
                    raise SerializerError("Schema does not exist")
                self.id_to_writers[schema_id] = self._get_encoder_func(schema)
            except ClientError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise SerializerError(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        writer = self.id_to_writers[schema_id]
        with ContextStringIO() as outf:
            # Write the magic byte and schema ID in network byte order (big endian)
            outf.write(struct.pack(">bI", MAGIC_BYTE, schema_id))

            # write the record to the rest of the buffer
            writer(record, outf)

            return outf.getvalue()

    def decode_message(self, message: typing.Optional[bytes]) -> typing.Optional[dict]:
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        Attributes:
            message bytes: message key or value to be decoded

        Returns:
            dict: Decoded message contents.
        """

        if message is None:
            return None

        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(message) as payload:
            magic, schema_id = struct.unpack(">bI", payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")

            if schema_id in self.id_to_decoder_func:
                return self.id_to_decoder_func[schema_id](payload)

            try:
                writer_schema = self.schemaregistry_client.get_by_id(schema_id)
            except ClientError as e:
                raise SerializerError(f"unable to fetch schema with id {schema_id}: {e}")

            if writer_schema is None:
                raise SerializerError(f"unable to fetch schema with id {schema_id}")

            decoder_func = self._get_decoder_func(payload, writer_schema)
            self.id_to_decoder_func[schema_id] = decoder_func

            return decoder_func(payload)


class AvroMessageSerializer(MessageSerializer):
    """
    AvroMessageSerializer to serialize and deserialize messages

    !!! Example
        ```python
        from schema_registry.client import SchemaRegistryClient, schema
        from schema_registry.serializers import AvroMessageSerializer


        client = SchemaRegistryClient("http://127.0.0.1:8081")
        avro_message_serializer = AvroMessageSerializer(client)

        avro_user_schema = schema.AvroSchema({
            "type": "record",
            "namespace": "com.example",
            "name": "AvroUsers",
            "fields": [
                {"name": "first_name", "type": "string"},
                {"name": "last_name", "type": "string"},
                {"name": "age", "type": "int"},

            ],
        })

        # We want to encode the user_record with avro_user_schema
        user_record = {
            "first_name": "my_first_name",
            "last_name": "my_last_name",
            "age": 20,
        }

        # Encode the record
        message_encoded = avro_message_serializer.encode_record_with_schema(
            "user", avro_user_schema, user_record)

        # this is because the message encoded reserved 5 bytes for the schema_id
        assert len(message_encoded) > 5
        assert isinstance(message_encoded, bytes)

        # Decode the message
        message_decoded = avro_message_serializer.decode_message(message_encoded)
        assert message_decoded == user_record

        # Now if we send a bad record
        bad_record = {
            "first_name": "my_first_name",
            "last_name": "my_last_name",
            "age": "my_age"
        }

        avro_message_serializer.encode_record_with_schema(
            "user", avro_user_schema, bad_record)

        # >>> TypeError: an integer is required on field age
        ```

    Attributes:
        schemaregistry_client schema_registry.client.AsyncSchemaRegistryClient: Http Client
        reader_schema schema_registry.schema.AvroSchema: Specify a schema to decode the message
        return_record_name bool: If the record name should be returned
    """

    @property
    def _serializer_schema_type(self) -> str:
        return utils.AVRO_SCHEMA_TYPE

    def _get_encoder_func(self, schema: typing.Union[BaseSchema]) -> typing.Callable:
        return lambda record, fp: schemaless_writer(fp, schema.schema, record)  # type: ignore

    def _get_decoder_func(self, payload: ContextStringIO, writer_schema: BaseSchema) -> typing.Callable:
        return lambda payload: schemaless_reader(
            payload, writer_schema.schema, self.reader_schema, self.return_record_name
        )  # type: ignore


class JsonMessageSerializer(MessageSerializer):
    """
    JsonMessageSerializer to serialize and deserialize messages

    !!! Example

        ```python
        from schema_registry.client import SchemaRegistryClient, schema
        from schema_registry.serializers import JsonMessageSerializer


        client = SchemaRegistryClient("http://127.0.0.1:8081")
        json_message_serializer = JsonMessageSerializer(client)

        json_schema = schema.JsonSchema({
        "definitions" : {
            "record:python.test.basic.basic" : {
            "description" : "basic schema for tests",
            "type" : "object",
            "required" : [ "number", "name" ],
            "properties" : {
                "number" : {
                "oneOf" : [ {
                    "type" : "integer"
                }, {
                    "type" : "null"
                } ]
                },
                "name" : {
                "oneOf" : [ {
                    "type" : "string"
                } ]
                }
            }
            }
        },
        "$ref" : "#/definitions/record:python.test.basic.basic"
        })

        # Encode the record
        basic_record = {
            "number": 10,
            "name": "a_name",
        }

        message_encoded = json_message_serializer.encode_record_with_schema(
            "basic", json_schema, basic_record)

        # this is because the message encoded reserved 5 bytes for the schema_id
        assert len(message_encoded) > 5
        assert isinstance(message_encoded, bytes)

        # Decode the message
        message_decoded = json_message_serializer.decode_message(message_encoded)
        assert message_decoded == basic_record
        ```

    Attributes:
        schemaregistry_client schema_registry.client.AsyncSchemaRegistryClient: Http Client
        reader_schema schema_registry.schema.AvroSchema: Specify a schema to decode the message
        return_record_name bool: If the record name should be returned
    """

    @property
    def _serializer_schema_type(self) -> str:
        return utils.JSON_SCHEMA_TYPE

    def _get_encoder_func(self, schema: typing.Union[BaseSchema]) -> typing.Callable:
        def json_encoder_func(record: dict, fp: ContextStringIO) -> typing.Any:
            validate(record, schema.schema)
            fp.write(json.dumps(record).encode())

        return json_encoder_func

    def _get_decoder_func(self, payload: ContextStringIO, writer_schema: BaseSchema) -> typing.Callable:
        def json_decoder_func(payload: typing.Union[str, bytes]) -> typing.Any:
            obj = json.load(payload)  # type: ignore
            validate(obj, writer_schema.schema)  # type: ignore
            return obj

        return json_decoder_func


class AsyncMessageSerializer(ABC):
    """
    AsyncMessageSerializer to serialize and deserialize messages asynchronously

    Attributes:
        schemaregistry_client schema_registry.client.AsyncSchemaRegistryClient: Http Client
        reader_schema schema_registry.schema.AvroSchema: Specify a schema to decode the message
        return_record_name bool: If the record name should be returned
    """

    def __init__(
        self,
        schemaregistry_client: AsyncSchemaRegistryClient,
        reader_schema: typing.Optional[schema.AvroSchema] = None,
        return_record_name: bool = False,
    ):
        self.schemaregistry_client = schemaregistry_client
        self.id_to_decoder_func = {}  # type: typing.Dict
        self.id_to_writers = {}  # type: typing.Dict
        self.reader_schema = reader_schema
        self.return_record_name = return_record_name

    @property
    @abstractmethod
    def _serializer_schema_type(self) -> str:
        ...

    @abstractmethod
    def _get_encoder_func(self, schema: BaseSchema) -> typing.Callable:
        ...

    @abstractmethod
    def _get_decoder_func(self, payload: ContextStringIO, writer_schema: BaseSchema) -> typing.Callable:
        ...

    async def encode_record_with_schema(self, subject: str, schema: typing.Union[BaseSchema], record: dict) -> bytes:
        """
        Given a parsed avro schema, encode a record for the given subject.
        The record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'

        Attributes:
            subject str: Subject name
            schema avro.schema.RecordSchema: Avro Schema
            record dict: An object to serialize

        Returns:
            Encoded record with schema ID
        """
        # Try to register the schema
        schema_id = await self.schemaregistry_client.register(subject, schema, schema_type=self._serializer_schema_type)

        # cache writer
        if not self.id_to_writers.get(schema_id):
            self.id_to_writers[schema_id] = self._get_encoder_func(schema)

        return await self.encode_record_with_schema_id(schema_id, record)

    async def encode_record_with_schema_id(self, schema_id: int, record: dict) -> bytes:
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.

        Attributes:
            schema_id int: integer ID
            record typing.Dict: An object to serialize

        Returns:
            Decoder function
        """
        # use slow avro
        if schema_id not in self.id_to_writers:
            try:
                schema = await self.schemaregistry_client.get_by_id(schema_id)

                if not schema:
                    raise SerializerError("Schema does not exist")
                self.id_to_writers[schema_id] = self._get_encoder_func(schema)
            except ClientError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise SerializerError(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        writer = self.id_to_writers[schema_id]
        with ContextStringIO() as outf:
            # Write the magic byte and schema ID in network byte order (big endian)
            outf.write(struct.pack(">bI", MAGIC_BYTE, schema_id))

            # write the record to the rest of the buffer
            writer(record, outf)

            return outf.getvalue()

    async def decode_message(self, message: typing.Optional[bytes]) -> typing.Optional[dict]:
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        Attributes:
            message bytes: message key or value to be decoded

        Returns:
            Decoded message
        """

        if message is None:
            return None

        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(message) as payload:
            magic, schema_id = struct.unpack(">bI", payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")

            if schema_id in self.id_to_decoder_func:
                return self.id_to_decoder_func[schema_id](payload)

            try:
                writer_schema = await self.schemaregistry_client.get_by_id(schema_id)
            except ClientError as e:
                raise SerializerError(f"unable to fetch schema with id {schema_id}: {e}")

            if writer_schema is None:
                raise SerializerError(f"unable to fetch schema with id {schema_id}")

            decoder_func = self._get_decoder_func(payload, writer_schema)
            self.id_to_decoder_func[schema_id] = decoder_func

            return decoder_func(payload)


class AsyncAvroMessageSerializer(AsyncMessageSerializer):
    """
    AsyncAvroMessageSerializer to serialize and deserialize messages asynchronously

    Attributes:
        schemaregistry_client schema_registry.client.AsyncSchemaRegistryClient: Http Client
        reader_schema schema_registry.schema.AvroSchema: Specify a schema to decode the message
        return_record_name bool: If the record name should be returned
    """

    @property
    def _serializer_schema_type(self) -> str:
        return utils.AVRO_SCHEMA_TYPE

    def _get_encoder_func(self, schema: typing.Union[BaseSchema]) -> typing.Callable:
        return lambda record, fp: schemaless_writer(fp, schema.schema, record)  # type: ignore

    def _get_decoder_func(self, payload: ContextStringIO, writer_schema: BaseSchema) -> typing.Callable:
        return lambda payload: schemaless_reader(
            payload, writer_schema.schema, self.reader_schema, self.return_record_name
        )  # type: ignore


class AsyncJsonMessageSerializer(AsyncMessageSerializer):
    """
    AsyncJsonMessageSerializer to serialize and deserialize messages asynchronously

    Attributes:
        schemaregistry_client schema_registry.client.AsyncSchemaRegistryClient: Http Client
        reader_schema schema_registry.schema.AvroSchema: Specify a schema to decode the message
        return_record_name bool: If the record name should be returned
    """

    @property
    def _serializer_schema_type(self) -> str:
        return utils.JSON_SCHEMA_TYPE

    def _get_encoder_func(self, schema: typing.Union[BaseSchema]) -> typing.Callable:
        def json_encoder_func(record: dict, fp: ContextStringIO) -> typing.Any:
            validate(record, schema.schema)
            fp.write(json.dumps(record).encode())

        return json_encoder_func

    def _get_decoder_func(self, payload: ContextStringIO, writer_schema: BaseSchema) -> typing.Callable:
        def json_decoder_func(payload: typing.Union[str, bytes]) -> typing.Any:
            obj = json.load(payload)  # type: ignore
            validate(obj, writer_schema.schema)  # type: ignore
            return obj

        return json_decoder_func
