# Schema Registry Server

This section provides you just an introduction about the `Schema Server`.

Schema Registry provides a serving layer for your metadata. It provides a RESTful interface for storing and retrieving Avro schemas. It stores a versioned history of all schemas, provides multiple compatibility settings and allows evolution of schemas according to the configured compatibility settings and expanded Avro support. It provides serializers that plug into Apache Kafka® clients that handle schema storage and retrieval for Kafka messages that are sent in the Avro format.

Schema Registry is a distributed storage layer for Avro Schemas which uses Kafka as its underlying storage mechanism. Some key design decisions:

1. Assigns globally unique ID to each registered schema. Allocated IDs are guaranteed to be monotonically increasing but not necessarily consecutive.
2. Kafka provides the durable backend, and functions as a write-ahead changelog for the state of Schema Registry and the schemas it contains.
3. Schema Registry is designed to be distributed, with single-primary architecture, and ZooKeeper/Kafka coordinates primary election (based on the configuration).

## API

### Schemas

`GET /schemas/ids/{int: id}` - Get the schema string identified by the input ID

### Subjects

`GET /subjects` - Get a list of registered subjects. *[Missing]*

`GET /subjects/(string: subject)/versions` - Get a list of versions registered under the specified subject *[Missing]* 

`DELETE /subjects/(string: subject)` - Deletes the specified subject and its associated compatibility level if registered. It is recommended to use this API only when a topic needs to be recycled or in development environment.

`GET /subjects/(string: subject)/versions/(versionId: version)` - Get a specific version of the schema registered under this subject *Check response*

`GET /subjects/(string: subject)/versions/(versionId: version)/schema` - Get the avro schema for the specified version of this subject. The unescaped schema only is returned. *[Missing]*

`POST /subjects/(string: subject)/versions` - Register a new schema under the specified subject and receive a schema id

`POST /subjects/(string: subject)` - Check if a schema has already been registered under the specified subject. If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.

`DELETE /subjects/(string: subject)/versions/(versionId: version)` - Deletes a specific version of the schema registered under this subject. This only deletes the version and the schema ID remains intact making it still possible to decode data using the schema ID. This API is recommended to be used only in development environments or under extreme circumstances where-in, its required to delete a previously registered schema for compatibility purposes or re-register previously registered schema. *[Missing]*

### Compatibility

`POST /compatibility/subjects/(string: subject)/versions/(versionId: version)` - Test input schema against a particular version of a subject's schema for compatibility. Note that the compatibility level applied for the check is the configured compatibility level for the subject (http:get:: /config/(string: subject)). If this subject's compatibility level was never changed, then the global compatibility level applies (http:get:: /config).

These are the compatibility types:

*BACKWARD*: (default) consumers using the new schema can read data written by producers using the latest registered schema

*BACKWARD_TRANSITIVE*: consumers using the new schema can read data written by producers using all previously registered schemas

*FORWARD*: consumers using the latest registered schema can read data written by producers using the new schema

*FORWARD_TRANSITIVE*: consumers using all previously registered schemas can read data written by producers using the new schema

*FULL*: the new schema is forward and backward compatible with the latest registered schema

*FULL_TRANSITIVE*: the new schema is forward and backward compatible with all previously registered schemas

*NONE*: schema compatibility checks are disabled

### Config

`GET /config` - Get global compatibility level.

`PUT /config` - Update global compatibility level. *[Missing]*

`GET /config/(string: subject)` - Get compatibility level for a subject. *[Missing]*

`PUT /config/(string: subject)` - Update compatibility level for the specified subject.

Too know more about the API go [here](https://docs.confluent.io/current/schema-registry/develop/api.html)