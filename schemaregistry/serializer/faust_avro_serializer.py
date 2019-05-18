from schemaregistry.serializer.message_serializer import MessageSerializer

from faust.serializers.codecs import Codec

from typing import Dict


class AvroSerializer(MessageSerializer, Codec):
    """
    Subclass of faust.serializers.codecs.Codec and
    datamountaineer.schemaregistry.serializers.MessageSerializer

    schama_registry_client: SchemaRegistryClient
        Client used to call the schema-registry service
    destination_topic: str
        Topic used to send the encoded message
    schema: str
        Parsed avro schema. Must be a parsed schema from the python avro library
    is_key: bool
    """

    def __init__(self, schema_registry_client, destination_topic,
                 schema, is_key=False):
        self.schema_registry_client = schema_registry_client
        self.destination_topic = destination_topic
        self.schema = schema
        self.is_key = is_key

        # Initialize parents
        MessageSerializer.__init__(self, schema_registry_client)
        Codec.__init__(self)

    def _loads(self, s: bytes) -> Dict:
        # method available on MessageSerializer
        return self.decode_message(s)

    def _dumps(self, obj: Dict) -> bytes:
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.

        The schema is registered with the subject of 'topic-value'
        """
        return self.encode_record_with_schema(
            topic=self.destination_topic,
            schema=self.schema,
            record=obj,
            is_key=self.is_key
        )
