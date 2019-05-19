from collections import namedtuple

SchemaVersion = namedtuple("SchemaVersion", "subject schema_id schema version")

VALID_LEVELS = ("NONE", "FULL", "FORWARD", "BACKWARD")
VALID_METHODS = ("GET", "POST", "PUT", "DELETE")
VALID_AUTH_PROVIDERS = ("URL", "USER_INFO", "SASL_INHERIT")

ACCEPT_HDR = "application/vnd.schema_registry.v1+json, application/vnd.schema_registry+json, application/json"
