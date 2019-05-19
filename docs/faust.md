# How to use it with Faust?

Avro Schemas, Custom Codecs and Serializers
-------------------------------------------

Because we want to be sure that the message that we encode are valid we use [Avro Schemas](https://docs.oracle.com/database/nosql-12.1.3.1/GettingStartedGuide/avroschemas.html).
Avro is used to define the data schema for a record's value. This schema describes the fields allowed in the value, along with their data types.

For our demostration in the `Users` application we are using the following schema:

```json
{
    "type": "record",
    "namespace": "com.example",
    "name": "AvroUsers",
    "fields": [
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"}
    ]
}
```

In order to use `avro schemas` with `Faust` we need to define a custom codec, a custom serializer and be able to talk with the `schema-registry`.
You can find the custom codec called `avro_users` registered using the [codec registation](https://faust.readthedocs.io/en/latest/userguide/models.html#codec-registry) approach described by faust.
The [AvroSerializer](https://github.com/marcosschroh/faust-docker-compose-example/blob/master/faust-project/example/helpers/avro/serializer/faust_avro_serializer.py#L8) is in charge to `encode` and `decode` messages using the [schema registry client](https://github.com/marcosschroh/faust-docker-compose-example/blob/master/faust-


Let's create a custom `codec`

```
codecs.py

from avro.schema import SchemaFromJSONData
from schema_registry.client import SchemaRegistryClient
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


# create an instance of the `SchemaRegistryClient`
client = SchemaRegistryClient(url=settings.SCHEMA_REGISTRY_URL)

avro_user_schema = SchemaFromJSONData({
     "type": "record",
     "namespace": "com.example",
     "name": "AvroUsers",
     "fields": [
       {"name": "first_name", "type": "string"},
       {"name": "last_name", "type": "string"}
     ]
})

avro_user_serializer = AvroSerializer(
        schema_registry_client=client,
        destination_topic="users",
        schema=avro_user_schema)


# function used to register the codec
def avro_user_codec():
    return avro_user_serializer
```

Now the final step is to integrate the faust model with the AvroSerializer.

```
# users.models

class UserModel(faust.Record, serializer='avro_users'):
    first_name: str
    last_name: str
```

Now our application is able to send and receive message using arvo schemas!!!! :-)