import typing

from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import message_serializer

try:
    import faust
except ImportError:
    faust = None  # type: ignore


class Serializer(message_serializer.MessageSerializer, faust.Codec):
    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        schema_subject: str,
        schema: schema.AvroSchema,
        is_key: bool = False,
    ):
        self.schema_registry_client = schema_registry_client
        self.schema_subject = schema_subject
        self.schema = schema
        self.is_key = is_key

        message_serializer.MessageSerializer.__init__(self, schema_registry_client)
        faust.Codec.__init__(self)

    def _loads(self, s: bytes) -> typing.Optional[typing.Dict]:
        # method available on MessageSerializer
        return self.decode_message(s)

    def _dumps(self, obj: dict) -> bytes:
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.

        The schema is registered with the subject of 'topic-value'
        """

        # method available on MessageSerializer
        return self.encode_record_with_schema(
            self.schema_subject, self.schema, obj, is_key=self.is_key
        )


def serializer_factory(
    schema_registry_client: SchemaRegistryClient,
    schema_subject: str,
    schema: schema.AvroSchema,
    is_key: bool = False,
) -> Serializer:

    assert faust is not None, "faust must be installed in order to use FaustSerializer"

    return Serializer(schema_registry_client, schema_subject, schema, is_key=is_key)


FaustSerializer = serializer_factory
