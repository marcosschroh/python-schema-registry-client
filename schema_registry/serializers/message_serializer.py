import io
import logging
import struct
import sys
import traceback

import avro
import avro.io

from schema_registry.client.errors import ClientError
from .errors import SerializerError, KeySerializerError, ValueSerializerError

log = logging.getLogger(__name__)

MAGIC_BYTE = 0

HAS_FAST = False
try:
    from fastavro import schemaless_reader, schemaless_writer

    HAS_FAST = True
except ImportError:
    pass


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class MessageSerializer:
    """
    A helper class that can serialize and deserialize messages

    Args:
        schemaregistry_client (schema_registry.client.SchemaRegistryClient): Http Client
    """

    def __init__(
        self, schemaregistry_client, reader_key_schema=None, reader_value_schema=None
    ):
        self.schemaregistry_client = schemaregistry_client
        self.id_to_decoder_func = {}
        self.id_to_writers = {}
        self.reader_key_schema = reader_key_schema
        self.reader_value_schema = reader_value_schema
        self.schema_name_to_id = {}

    def _get_encoder_func(self, writer_schema):
        if HAS_FAST:
            schema = writer_schema.to_json()
            return lambda record, fp: schemaless_writer(fp, schema, record)
        writer = avro.io.DatumWriter(writer_schema)
        return lambda record, fp: writer.write(record, avro.io.BinaryEncoder(fp))

    def encode_record_with_schema(self, subject, schema, record, is_key=False):
        """
        Given a parsed avro schema, encode a record for the given subject.
        The record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'

        Args:
            subject (str): Subject name
            schema (avro.schema.RecordSchema): Avro Schema
            record (dict): An object to serialize
            is_key (bool): If the record is a key

        Returns:
            bytes: Encoded record with schema ID as bytes
        """
        schema_id = self.schema_name_to_id.get(schema.name)

        if not schema_id:
            serialize_err = KeySerializerError if is_key else ValueSerializerError

            subject_suffix = "-key" if is_key else "-value"
            # get the latest schema for the subject
            subject = f"{subject}{subject_suffix}"
            # register it
            schema_id = self.schemaregistry_client.register(subject, schema)

            if not schema_id:
                message = f"Unable to retrieve schema id for subject {subject}"
                raise serialize_err(message)

            # cache writer
            self.id_to_writers[schema_id] = self._get_encoder_func(schema)

            # cache the schema_id using the schema name
            logging.info(f"Caching Schema {schema.name} with ID: {schema_id}")
            self.schema_name_to_id[schema.name] = schema_id

        return self.encode_record_with_schema_id(schema_id, record, is_key=is_key)

    def encode_record_with_schema_id(self, schema_id, record, is_key=False):
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.

        Args:
            schema_id (int): integer ID
            record (dict): An object to serialize
            is_key (bool): If the record is a key

        Returns:
            func: decoder function
        """
        serialize_err = KeySerializerError if is_key else ValueSerializerError

        # use slow avro
        if schema_id not in self.id_to_writers:
            # get the writer + schema

            try:
                schema = self.schemaregistry_client.get_by_id(schema_id)
                if not schema:
                    raise serialize_err("Schema does not exist")
                self.id_to_writers[schema_id] = self._get_encoder_func(schema)
            except ClientError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise serialize_err(
                    repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
                )

        # get the writer
        writer = self.id_to_writers[schema_id]
        with ContextStringIO() as outf:
            # Write the magic byte and schema ID in network byte order (big endian)
            outf.write(struct.pack(">bI", MAGIC_BYTE, schema_id))

            # write the record to the rest of the buffer
            writer(record, outf)

            return outf.getvalue()

    def _get_decoder_func(self, schema_id, payload, is_key=False):
        if schema_id in self.id_to_decoder_func:
            return self.id_to_decoder_func[schema_id]

        try:
            writer_schema_obj = self.schemaregistry_client.get_by_id(schema_id)
        except ClientError as e:
            raise SerializerError(f"unable to fetch schema with id {schema_id}: {e}")

        if writer_schema_obj is None:
            raise SerializerError(f"unable to fetch schema with id {schema_id}")

        curr_pos = payload.tell()

        reader_schema_obj = (
            self.reader_key_schema if is_key else self.reader_value_schema
        )

        if HAS_FAST:
            try:
                writer_schema = writer_schema_obj.to_json()
                reader_schema = reader_schema_obj.to_json()
                schemaless_reader(payload, writer_schema)

                # If we reach this point, this means we have fastavro and it can
                # do this deserialization. Rewind since this method just determines
                # the reader function and we need to deserialize again along the
                # normal path.
                payload.seek(curr_pos)

                self.id_to_decoder_func[schema_id] = lambda p: schemaless_reader(
                    p, writer_schema, reader_schema
                )
                return self.id_to_decoder_func[schema_id]
            except Exception:
                # Fast avro failed, fall thru to standard avro below.
                pass

        # here means we should just delegate to slow avro
        # rewind
        payload.seek(curr_pos)
        # Avro DatumReader py2/py3 inconsistency, hence no param keywords
        # should be revisited later
        # https://github.com/apache/avro/blob/master/lang/py3/avro/io.py#L459
        # https://github.com/apache/avro/blob/master/lang/py/src/avro/io.py#L423
        # def __init__(self, writers_schema=None, readers_schema=None)
        # def __init__(self, writer_schema=None, reader_schema=None)
        avro_reader = avro.io.DatumReader(writer_schema_obj, reader_schema_obj)

        def decoder(p):
            bin_decoder = avro.io.BinaryDecoder(p)
            return avro_reader.read(bin_decoder)

        self.id_to_decoder_func[schema_id] = decoder
        return self.id_to_decoder_func[schema_id]

    def decode_message(self, message, is_key=False):
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
            decoder_func = self._get_decoder_func(schema_id, payload, is_key)
            return decoder_func(payload)
