import json
import logging
import requests
from collections import defaultdict

from schema_registry.client.errors import ClientError
from schema_registry.client.schema import AvroSchema
from schema_registry.client import status, utils

log = logging.getLogger(__name__)


class SchemaRegistryClient(requests.Session):
    """
    A client that talks to a Schema Registry over HTTP

    Args:
        url (str|dict) url: Url to schema registry or dictionary containing client configuration.
        ca_location (str): File or directory path to CA certificate(s) for verifying the Schema Registry key.
        cert_location (str): Path to public key used for authentication.
        key_location (str): Path to private key used for authentication.
        extra_headers (dict): Extra headers to add on every requests.
    """

    def __init__(
        self,
        url,
        ca_location=None,
        cert_location=None,
        key_location=None,
        extra_headers=None,
    ):
        super().__init__()

        conf = url
        if not isinstance(url, dict):
            conf = {
                "url": url,
                "ssl.ca.location": ca_location,
                "ssl.certificate.location": cert_location,
                "ssl.key.location": key_location,
            }

        # Ensure URL valid scheme is included; http[s]
        url = conf.get("url", "")
        if not isinstance(url, str):
            raise TypeError("URL must be of type str")

        if not url.startswith("http"):
            raise ValueError("Invalid URL provided for Schema Registry")

        self.url = url.rstrip("/")
        self.extra_headers = extra_headers

        # subj => { schema => id }
        self.subject_to_schema_ids = defaultdict(dict)
        # id => avro_schema
        self.id_to_schema = defaultdict(dict)
        # subj => { schema => version }
        self.subject_to_schema_versions = defaultdict(dict)

        self.verify = conf.pop("ssl.ca.location", None)
        self.cert = self._configure_client_tls(conf)
        self.auth = self._configure_basic_auth(conf)
        self.url = conf.pop("url")

        if len(conf) > 0:
            raise ValueError("fUnrecognized configuration properties:{conf}")

    @staticmethod
    def _configure_basic_auth(conf):
        url = conf["url"]
        auth_provider = conf.pop("basic.auth.credentials.source", "URL").upper()

        if auth_provider not in utils.VALID_AUTH_PROVIDERS:
            raise ValueError(
                f"schema.registry.basic.auth.credentials.source must be one of {utils.VALID_AUTH_PROVIDERS}"
            )
        if auth_provider == "SASL_INHERIT":
            if conf.pop("sasl.mechanism", "").upper() is ["GSSAPI"]:
                raise ValueError("SASL_INHERIT does not support SASL mechanisms GSSAPI")
            auth = (conf.pop("sasl.username", ""), conf.pop("sasl.password", ""))
        elif auth_provider == "USER_INFO":
            auth = tuple(conf.pop("basic.auth.user.info", "").split(":"))
        else:
            auth = requests.utils.get_auth_from_url(url)
        conf["url"] = requests.utils.urldefragauth(url)

        return auth

    @staticmethod
    def _configure_client_tls(conf):
        cert = (
            conf.pop("ssl.certificate.location", None),
            conf.pop("ssl.key.location", None),
        )
        # Both values can be None or no values can be None
        if bool(cert[0]) != bool(cert[1]):
            raise ValueError(
                "Both schema.registry.ssl.certificate.location and schema.registry.ssl.key.location must be set"
            )

        return cert

    def prepare_headers(self, body=None, headers=None):
        _headers = {"Accept": utils.ACCEPT_HEADERS}

        if self.extra_headers:
            _headers.update(self.extra_headers)

        if body:
            _headers["Content-Length"] = str(len(body))
            _headers["Content-Type"] = utils.HEADERS

        if headers:
            _headers.update(headers)

        return _headers

    def request(self, url, method="GET", body=None, headers=None):
        if method not in utils.VALID_METHODS:
            raise ClientError(
                f"Method {method} is invalid; valid methods include {utils.VALID_METHODS}"
            )

        _headers = self.prepare_headers(body=body, headers=headers)
        response = super().request(method, url, headers=_headers, json=body)

        try:
            return response.json(), response.status_code
        except ValueError:
            return response.content, response.status_code

    @staticmethod
    def _add_to_cache(cache, subject, schema, value):
        sub_cache = cache[subject]
        sub_cache[schema] = value

    def _cache_schema(self, schema, schema_id, subject=None, version=None):
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            self.id_to_schema[schema_id] = schema

        if subject:
            self._add_to_cache(self.subject_to_schema_ids, subject, schema, schema_id)
            if version:
                self._add_to_cache(
                    self.subject_to_schema_versions, subject, schema, version
                )

    def register(self, subject, avro_schema, headers=None):
        """
        POST /subjects/(string: subject)/versions
        Register a schema with the registry under the given subject
        and receive a schema id.
        avro_schema must be a parsed schema from the python avro library
        Multiple instances of the same schema will result in cache misses.

        Args:
            subject (str): subject name
            avro_schema (avro.schema.RecordSchema): Avro schema to be registered
            headers (dict): Extra headers to add on the requests

        Returns:
            int: schema_id
        """
        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id = schemas_to_id.get(avro_schema.name)

        if schema_id is not None:
            return schema_id

        url = "/".join([self.url, "subjects", subject, "versions"])

        body = {"schema": json.dumps(avro_schema.schema)}

        result, code = self.request(url, method="POST", body=body, headers=headers)

        msg = None
        if code in (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN):
            msg = "Unauthorized access"
        elif code == status.HTTP_409_CONFLICT:
            msg = "Incompatible Avro schema"
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            msg = "Invalid Avro schema"
        elif not status.is_success(code):
            msg = "Unable to register schema"

        if msg is not None:
            raise ClientError(message=msg, http_code=code, server_traceback=result)

        schema_id = result["id"]
        self._cache_schema(avro_schema, schema_id, subject)

        return schema_id

    def delete_subject(self, subject, headers=None):
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be
        recycled or in development environments.

        Args:
            subject (str): subject name
            headers (dict): Extra headers to add on the requests

        Returns:
            int: version of the schema deleted under this subject
        """
        url = "/".join([self.url, "subjects", subject])

        result, code = self.request(url, method="DELETE", headers=headers)
        if not status.is_success(code):
            raise ClientError(
                "Unable to delete subject", http_code=code, server_traceback=result
            )
        return result

    def get_by_id(self, schema_id, headers=None):
        """
        GET /schemas/ids/{int: id}
        Retrieve a parsed avro schema by id or None if not found

        Args:
            schema_id (int): Schema Id
            headers (dict): Extra headers to add on the requests

        Returns:
            avro.schema.RecordSchema: Avro Record schema
        """
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]
        # fetch from the registry
        url = "/".join([self.url, "schemas", "ids", str(schema_id)])

        result, code = self.request(url, headers=headers)
        if code == status.HTTP_404_NOT_FOUND:
            log.error(f"Schema not found: {code}")
        elif not status.is_success(code):
            log.error(f"Unable to get schema for the specific ID: {code}")
        else:
            # need to parse the schema
            schema_str = result.get("schema")
            try:
                result = AvroSchema(schema_str)

                # cache the result
                self._cache_schema(result, schema_id)
                return result
            except ClientError:
                # bad schema - should not happen
                raise ClientError(
                    f"Received bad schema (id {schema_id})",
                    http_code=code,
                    server_traceback=result,
                )

    def get_schema(self, subject, version="latest", headers=None):
        """
        GET /subjects/(string: subject)/versions/(versionId: version)
        Get a specific version of the schema registered under this subject

        Args:
            subject (str): subject name
            version (int, optional): version id. If is None, the latest schema is returned
            headers (dict): Extra headers to add on the requests

        Returns:
            SchemaVersion (nametupled): (subject, schema_id, schema, version)

            None: If server returns a not success response:
                404: Schema not found
                422: Unprocessable entity
                ~ (200 - 299): Not success
        """
        url = "/".join([self.url, "subjects", subject, "versions", str(version)])

        result, code = self.request(url, headers=headers)
        if code == status.HTTP_404_NOT_FOUND:
            log.error(f"Schema not found: {code}")
            return
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            log.error(f"Invalid version: {code}")
            return
        elif not status.is_success(code):
            log.error(f"Not success version: {code}")
            return

        schema_id = result.get("id")
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            try:
                schema = AvroSchema(result["schema"])
            except ClientError:
                raise

        version = result.get("version")
        self._cache_schema(schema, schema_id, subject, version)

        return utils.SchemaVersion(subject, schema_id, schema, version)

    def check_version(self, subject, avro_schema, headers=None):
        """
        POST /subjects/(string: subject)
        Check if a schema has already been registered under the specified subject.
        If so, this returns the schema string along with its globally unique identifier,
        its version under this subject and the subject name.

        Args:
            subject (str): subject name
            avro_schema (avro.schema.RecordSchema): Avro schema
            headers (dict): Extra headers to add on the requests

        Returns:
            dict:
                subject (string) -- Name of the subject that this schema is registered under
                id (int) -- Globally unique identifier of the schema
                version (int) -- Version of the returned schema
                schema (dict) -- The Avro schema

            None: If schema not found.
        """
        schemas_to_version = self.subject_to_schema_versions[subject]
        version = schemas_to_version.get(avro_schema)

        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id = schemas_to_id.get(avro_schema)

        if all((version, schema_id)):
            return utils.SchemaVersion(subject, schema_id, version, avro_schema)

        url = "/".join([self.url, "subjects", subject])
        body = {"schema": json.dumps(avro_schema.schema)}

        result, code = self.request(url, method="POST", body=body, headers=headers)
        if code == status.HTTP_404_NOT_FOUND:
            log.error(f"Not found: {code}")
            return
        elif not status.is_success(code):
            log.error(f"Unable to get version of a schema: {code}")
            return

        schema_id = result.get("id")
        version = result.get("version")
        self._cache_schema(avro_schema, schema_id, subject, version)

        return utils.SchemaVersion(subject, schema_id, version, result.get("schema"))

    def test_compatibility(self, subject, avro_schema, version="latest", headers=None):
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)
        Test the compatibility of a candidate parsed schema for a given subject.
        By default the latest version is checked against.

        Args:
            subject (str): subject name
            avro_schema (avro.schema.RecordSchema): Avro schema
            headers (dict): Extra headers to add on the requests

        Returns:
            bool: True if schema given compatible, False otherwise
        """
        url = "/".join(
            [self.url, "compatibility", "subjects", subject, "versions", str(version)]
        )
        body = {"schema": json.dumps(avro_schema.schema)}
        try:
            result, code = self.request(url, method="POST", body=body, headers=headers)
            if code == status.HTTP_404_NOT_FOUND:
                log.error(f"Subject or version not found: {code}")
                return False
            elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
                log.error(f"Invalid subject or schema: {code}")
                return False
            elif status.is_success(code):
                return result.get("is_compatible")
            else:
                log.error(
                    f"Unable to check the compatibility: {code}. Traceback: {result}"
                )
                return False
        except Exception as e:
            log.error(f"request() failed: {e}")
            return False

    def update_compatibility(self, level, subject=None, headers=None):
        """
        PUT /config/(string: subject)
        Update the compatibility level.
        If subject is None, the compatibility level is global.

        Args:
            level (str): one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
                FULL, FULL_TRANSITIVE, NONE
            subject (str): Option subject
            headers (dict): Extra headers to add on the requests

        Returns:
            bool: True if compatibility was updated

        Raises:
            ClientError: if the request was unsuccessful or an invalid
        """
        if level not in utils.VALID_LEVELS:
            raise ClientError(f"Invalid level specified: {level}")

        url = "/".join([self.url, "config"])
        if subject:
            url += "/" + subject

        body = {"compatibility": level}
        result, code = self.request(url, method="PUT", body=body, headers=headers)

        if status.is_success(code):
            return True

        raise ClientError(
            f"Unable to update level: {level}.", http_code=code, server_traceback=result
        )

    def get_compatibility(self, subject, headers=None):
        """
        Get the current compatibility level for a subject.

        Args:
            subject (str): subject name
            headers (dict): Extra headers to add on the requests

        Returns:
            str: one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
                FULL, FULL_TRANSITIVE, NONE
        Raises:
            ClientError: if the request was unsuccessful or an invalid
            compatibility level was returned
        """
        url = "/".join([self.url, "config"])
        if subject:
            url = "/".join([url, subject])

        result, code = self.request(url, headers=headers)

        if not status.is_success(code):
            raise ClientError(
                f"Unable to fetch compatibility level. Error code: {code}",
                http_code=code,
                server_traceback=result,
            )

        compatibility = result.get("compatibilityLevel")
        if compatibility not in utils.VALID_LEVELS:
            if compatibility is None:
                error_msg_suffix = "No compatibility was returned"
            else:
                error_msg_suffix = str(compatibility)
            raise ClientError(
                f"Invalid compatibility level received: {error_msg_suffix}",
                http_code=code,
                server_traceback=result,
            )

        return compatibility
