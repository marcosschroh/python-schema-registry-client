import typing
from collections.abc import Mapping, Sequence

from schema_registry.client import SchemaRegistryClient
from schema_registry.client.schema import AvroSchema
from schema_registry.serializers import message_serializer

try:
    from faust import Codec, Record
except ImportError:
    Codec = None  # type: ignore


def serializer_factory(
    schema_registry_client: SchemaRegistryClient, schema_subject: str, schema: AvroSchema
) -> "Serializer":  # type: ignore # noqa: F821

    assert Codec is not None, "faust must be installed in order to use FaustSerializer"

    class Serializer(message_serializer.MessageSerializer, Codec):
        def __init__(
            self,
            schema_registry_client: SchemaRegistryClient,
            schema_subject: str,
            schema: AvroSchema,
        ):
            self.schema_registry_client = schema_registry_client
            self.schema_subject = schema_subject
            self.schema = schema

            message_serializer.MessageSerializer.__init__(self, schema_registry_client)
            Codec.__init__(self)

        def _loads(self, event: bytes) -> typing.Optional[typing.Dict]:
            # method available on MessageSerializer
            return self.decode_message(event)

        def _dumps(self, payload: typing.Dict[str, typing.Any]) -> bytes:
            """
            Given a parsed avro schema, encode a record for the given topic.  The
            record is expected to be a dictionary.
            The schema is registered with the subject of 'topic-value'
            """
            payload = self.clean_payload(payload)
            # method available on MessageSerializer
            return self.encode_record_with_schema(self.schema_subject, self.schema, payload)

        @staticmethod
        def _clean_item(item: typing.Any) -> typing.Any:
            if isinstance(item, Record):
                return Serializer._clean_item(item.to_representation())
            elif isinstance(item, str):
                # str is also a sequence, need to make sure we don't iterate over it.
                return item
            elif isinstance(item, Mapping):
                return type(item)({key: Serializer._clean_item(value) for key, value in item.items()})  # type: ignore
            elif isinstance(item, Sequence):
                return type(item)(Serializer._clean_item(value) for value in item)  # type: ignore
            return item

        @staticmethod
        def clean_payload(payload: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
            """
            Try to clean payload retrieve by faust.Record.to_representation.
            All values inside payload should be native types and not faust.Record
            On Faust versions <=1.9.0 Record.to_representation always returns a dict with native types
            as a values which are compatible with fastavro.
            On Faust 1.10.0 <= versions Record.to_representation always returns a dic but values
            can also be faust.Record, so fastavro is incapable of serialize them
            Args:
                payload (dict): Payload to clean
            Returns:
                dict that represents the clean payload
            """
            return Serializer._clean_item(payload)

    return Serializer(schema_registry_client, schema_subject, schema)


FaustSerializer = serializer_factory
