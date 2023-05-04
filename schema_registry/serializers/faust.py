import typing
from collections.abc import Mapping, Sequence

from schema_registry.client import SchemaRegistryClient
from schema_registry.client.schema import AvroSchema, BaseSchema, JsonSchema
from schema_registry.serializers import AvroMessageSerializer, JsonMessageSerializer, MessageSerializer

try:
    from faust import Codec, Record
except ImportError as ex:
    raise Exception("Cannot use Faust serializers Faust is not installed.") from ex


class Serializer(Codec):
    def __init__(
        self,
        schema_subject: str,
        schema: typing.Union[BaseSchema],
        message_serializer: MessageSerializer,
    ):
        self.schema_subject = schema_subject
        self.schema = schema
        self.message_serializer = message_serializer

        Codec.__init__(self)

    def _loads(self, event: bytes) -> typing.Optional[typing.Dict]:
        return self.message_serializer.decode_message(event)

    def _dumps(self, payload: typing.Dict[str, typing.Any]) -> bytes:
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'
        """
        payload = self.clean_payload(payload)

        return self.message_serializer.encode_record_with_schema(
            self.schema_subject, self.schema, payload
        )  # type: ignore

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


def avro_serializer_factory(
    schema_registry_client: SchemaRegistryClient,
    schema_subject: str,
    schema: AvroSchema,
    return_record_name: bool = False,
) -> "Serializer":  # type: ignore # noqa: F821
    if isinstance(schema, str):
        schema = AvroSchema(schema)

    return Serializer(
        schema_subject, schema, AvroMessageSerializer(schema_registry_client, return_record_name=return_record_name)
    )


def json_serializer_factory(
    schema_registry_client: SchemaRegistryClient,
    schema_subject: str,
    schema: JsonSchema,
    return_record_name: bool = False,
) -> "Serializer":  # type: ignore # noqa: F821
    if isinstance(schema, str):
        schema = JsonSchema(schema)

    return Serializer(
        schema_subject, schema, JsonMessageSerializer(schema_registry_client, return_record_name=return_record_name)
    )


FaustSerializer = avro_serializer_factory
FaustJsonSerializer = json_serializer_factory
