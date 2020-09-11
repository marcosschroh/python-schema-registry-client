import io
import logging
import struct
import sys
import traceback
import typing

from fastavro import schemaless_reader, schemaless_writer

from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.client.errors import ClientError

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


class MessageSerializer:
    """
    A helper class that can serialize and deserialize messages
    Args:
        schemaregistry_client (schema_registry.client.SchemaRegistryClient): Http Client
    """

    def __init__(
        self, schemaregistry_client: SchemaRegistryClient, reader_schema: typing.Optional[schema.AvroSchema] = None
    ):
        self.schemaregistry_client = schemaregistry_client
        self.id_to_decoder_func = {}  # type: typing.Dict
        self.id_to_writers = {}  # type: typing.Dict
        self.reader_schema = reader_schema
        self.schema_name_to_id = {}  # type: typing.Dict

    def _get_encoder_func(self, avro_schema: schema.AvroSchema) -> typing.Callable:
        return lambda record, fp: schemaless_writer(fp, avro_schema.schema, record)

    def encode_record_with_schema(self, subject: str, avro_schema: schema.AvroSchema, record: dict) -> bytes:
        """
        Given a parsed avro schema, encode a record for the given subject.
        The record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'
        Args:
            subject (str): Subject name
            schema (avro.schema.RecordSchema): Avro Schema
            record (dict): An object to serialize
        Returns:
            bytes: Encoded record with schema ID as bytes
        """
        schema_id = self.schema_name_to_id.get(avro_schema.name)

        if not schema_id:
            # Try to register the schema
            schema_id = self.schemaregistry_client.register(subject, avro_schema)

            if not schema_id:
                message = f"Unable to retrieve schema id for subject {subject}"
                raise SerializerError(message)

            # cache writer
            self.id_to_writers[schema_id] = self._get_encoder_func(avro_schema)

            # cache the schema_id using the schema name
            logging.info(f"Caching Schema {avro_schema.name} with ID: {schema_id}")
            self.schema_name_to_id[avro_schema.name] = schema_id

        return self.encode_record_with_schema_id(schema_id, record)

    def encode_record_with_schema_id(self, schema_id: int, record: dict) -> bytes:
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.
        Args:
            schema_id (int): integer ID
            record (dict): An object to serialize
        Returns:
            func: decoder function
        """
        # use slow avro
        if schema_id not in self.id_to_writers:
            try:
                avro_schema = self.schemaregistry_client.get_by_id(schema_id)
                if not avro_schema:
                    raise SerializerError("Schema does not exist")
                self.id_to_writers[schema_id] = self._get_encoder_func(avro_schema)
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

    def _get_decoder_func(self, schema_id: int, payload: ContextStringIO) -> typing.Callable:
        if schema_id in self.id_to_decoder_func:
            return self.id_to_decoder_func[schema_id]

        try:
            writer_schema = self.schemaregistry_client.get_by_id(schema_id)
        except ClientError as e:
            raise SerializerError(f"unable to fetch schema with id {schema_id}: {e}")

        if writer_schema is None:
            raise SerializerError(f"unable to fetch schema with id {schema_id}")

        self.id_to_decoder_func[schema_id] = lambda payload: schemaless_reader(
            payload, writer_schema.schema, self.reader_schema
        )

        return self.id_to_decoder_func[schema_id]

    def decode_message(self, message: typing.Union[None, bytes]) -> typing.Union[None, dict]:
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        Args:
            message (str|bytes or None): message key or value to be decoded
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
            decoder_func = self._get_decoder_func(schema_id, payload)

            return decoder_func(payload)
