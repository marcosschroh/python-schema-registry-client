# How to use it with Faust?

```
from schema_registry.serializer import MessageSerializer

from faust.serializers.codecs import Codec


class AvroSerializer(MessageSerializer, Codec):

    def __init__(self, schema_registry_client, destination_topic, schema, is_key=False):
        self.schema_registry_client = schema_registry_client
        self.destination_topic = destination_topic
        self.schema = schema
        self.is_key = is_key

        MessageSerializer.__init__(self, schema_registry_client)
        Codec.__init__(self)

    def _loads(self, s: bytes):
        # method available on MessageSerializer
        return self.decode_message(s)

    def _dumps(self, obj):
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.

        The schema is registered with the subject of 'topic-value'
        """

        # method available on MessageSerializer
        return self.encode_record_with_schema(
            topic=self.destination_topic,
            schema=self.schema,
            record=obj,
            is_key=self.is_key,
        )
```