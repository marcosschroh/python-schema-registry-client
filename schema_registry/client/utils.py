from collections import namedtuple

SchemaVersion = namedtuple("SchemaVersion", "subject schema_id schema version")

BACKWARD = "BACKWARD"
BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
FORWARD = "FORWARD"
FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
FULL = "FULL"
FULL_TRANSITIVE = "FULL_TRANSITIVE"
NONE = "NONE"

VALID_LEVELS = (BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE)
VALID_METHODS = ("GET", "POST", "PUT", "DELETE")
VALID_AUTH_PROVIDERS = (
    "URL",
    "USER_INFO",
)

HEADER_AVRO_JSON = "application/x-avro-json"
HEADER_AVRO = "application/avro"
HEADER_APPLICATION_JSON = "application/json"
HEADERS = "application/vnd.schemaregistry.v1+json"
ACCEPT_HEADERS = "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"

URL = "url"
SSL_CA_LOCATION = "ssl.ca.location"
SSL_CERTIFICATE_LOCATION = "ssl.certificate.location"
SSL_KEY_LOCATION = "ssl.key.location"
SSL_KEY_PASSWORD = "ssl.key.password"

AVRO_SCHEMA_TYPE = "AVRO"
JSON_SCHEMA_TYPE = "JSON"
