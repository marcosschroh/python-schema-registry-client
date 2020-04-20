from collections import namedtuple

SchemaVersion = namedtuple("SchemaVersion", "subject schema_id schema version")

BACKWARD = "BACKWARD"
BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
FORWARD = "FORWARD"
FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
FULL = "FULL"
FULL_TRANSITIVE = "FULL_TRANSITIVE"
NONE = "NONE"

VALID_LEVELS = (
    BACKWARD,
    BACKWARD_TRANSITIVE,
    FORWARD,
    FORWARD_TRANSITIVE,
    FULL,
    FULL_TRANSITIVE,
    NONE,
)
VALID_METHODS = ("GET", "POST", "PUT", "DELETE")
VALID_AUTH_PROVIDERS = ("URL", "USER_INFO", "SASL_INHERIT")

HEADERS = "application/vnd.schemaregistry.v1+json"
ACCEPT_HEADERS = "application/vnd.schema_registry.v1+json, application/vnd.schema_registry+json, application/json"
