import json
import logging
import requests
from collections import defaultdict

from schema_registry.client.errors import ClientError
from schema_registry.client.schema import AvroSchema
from schema_registry.client import status, utils
from schema_registry.client.urls import UrlManager
from schema_registry.client.paths import paths


logger = logging.getLogger(__name__)


class SchemaRegistryClient(requests.Session):
    """
    A client that talks to a Schema Registry over HTTP

    Args:
        url (str|dict) url: Url to schema registry or dictionary containing client configuration.
        ca_location (str): File or directory path to CA certificate(s) for verifying the Schema Registry key.
        cert_location (str): Path to public key used for authentication.
        key_location (str): Path to ./vate key used for authentication.
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

        self.verify = conf.pop("ssl.ca.location", None)
        self.cert = self._configure_client_tls(conf)
        self.auth = self._configure_basic_auth(conf)

        url = conf.pop("url")
        self.url_manager = UrlManager(url, paths)

        if len(conf) > 0:
            raise ValueError("fUnrecognized configuration properties:{conf}")

        # CACHE:
        # subj => { schema => id }
        self.subject_to_schema_ids = defaultdict(dict)
        # id => avro_schema
        self.id_to_schema = defaultdict(dict)
        # subj => { schema => version }
        self.subject_to_schema_versions = defaultdict(dict)

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

    def register(
        self, subject: str, avro_schema: AvroSchema, headers: dict = None
    ) -> int:
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

        url, method = self.url_manager.url_for("register", subject=subject)
        body = {"schema": json.dumps(avro_schema.schema)}

        result, code = self.request(url, method=method, body=body, headers=headers)

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

    def get_subjects(self, headers: dict = None) -> list:
        """
        GET /subjects/(string: subject)
        Get list of all registered subjects in your Schema Registry.

        Args:
            subject (str): subject name
            headers (dict): Extra headers to add on the requests

        Returns:
            list [str]: list of registered subjects.
        """
        url, method = self.url_manager.url_for("get_subjects")
        result, code = self.request(url, method=method, headers=headers)

        if status.is_success(code):
            return result

        raise ClientError(
            "Unable to delete subject", http_code=code, server_traceback=result
        )

    def delete_subject(self, subject: str, headers: dict = None) -> list:
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be
        recycled or in development environments.

        Args:
            subject (str): subject name
            headers (dict): Extra headers to add on the requests

        Returns:
            list (int): version of the schema deleted under this subject
        """
        url, method = self.url_manager.url_for("delete_subject", subject=subject)
        result, code = self.request(url, method=method, headers=headers)

        if status.is_success(code):
            return result
        elif code == status.HTTP_404_NOT_FOUND:
            return []

        raise ClientError(
            "Unable to delete subject", http_code=code, server_traceback=result
        )

    def get_by_id(self, schema_id: int, headers: dict = None) -> AvroSchema:
        """
        GET /schemas/ids/{int: id}
        Retrieve a parsed avro schema by id or None if not found

        Args:
            schema_id (int): Schema Id
            headers (dict): Extra headers to add on the requests

        Returns:
            client.schema.AvroSchema: Avro Record schema
        """
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]

        url, method = self.url_manager.url_for("get_by_id", schema_id=schema_id)

        result, code = self.request(url, method=method, headers=headers)
        if code == status.HTTP_404_NOT_FOUND:
            logger.error(f"Schema not found: {code}")
            return
        elif not status.is_success(code):
            logger.error(f"Unable to get schema for the specific ID: {code}")
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

    def get_schema(
        self, subject: str, version: str = "latest", headers: dict = None
    ) -> utils.SchemaVersion:
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
        url, method = self.url_manager.url_for(
            "get_schema", subject=subject, version=version
        )

        result, code = self.request(url, method=method, headers=headers)
        if code == status.HTTP_404_NOT_FOUND:
            logger.error(f"Schema not found: {code}")
            return
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            logger.error(f"Invalid version: {code}")
            return
        elif not status.is_success(code):
            logger.error(f"Not success version: {code}")
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

    def get_versions(self, subject: str, headers: dict = None) -> list:
        """
        GET subjects/{subject}/versions
        Get a list of versions registered under the specified subject.

        Args:
            subject (str): subject name
            headers (dict): Extra headers to add on the requests

        Returns:
            list (str): version of the schema registered under this subject
        """
        url, method = self.url_manager.url_for("get_versions", subject=subject)

        result, code = self.request(url, method=method, headers=headers)
        if status.is_success(code):
            return result
        elif code == status.HTTP_404_NOT_FOUND:
            logger.error(f"Subject {subject} not found")
            return []

        raise ClientError(
            f"Unable to get the versions for subject {subject}",
            http_code=code,
            server_traceback=result,
        )

    def delete_version(self, subject: str, version="latest", headers: dict = None):
        """
        DELETE /subjects/(string: subject)/versions/(versionId: version)

        Deletes a specific version of the schema registered under this subject.
        This only deletes the version and the schema ID remains intact making
        it still possible to decode data using the schema ID.
        This API is recommended to be used only in development environments or
        under extreme circumstances where-in, its required to delete a previously
        registered schema for compatibility purposes or re-register previously registered schema.

        Args:
            subject (str): subject name
            version (str): Version of the schema to be deleted. 
                Valid values for versionId are between [1,2^31-1] or the string "latest".
                "latest" deletes the last registered schema under the specified subject.
            headers (dict): Extra headers to add on the requests

        Returns:
            int: version of the schema deleted
            None: If the subject or version does not exist.
        """
        url, method = self.url_manager.url_for(
            "delete_version", subject=subject, version=version
        )

        result, code = self.request(url, method=method, headers=headers)

        if status.is_success(code):
            return result
        elif status.is_client_error(code):
            return

        raise ClientError(
            "Unable to delete the version", http_code=code, server_traceback=result
        )

    def check_version(
        self, subject: str, avro_schema: AvroSchema, headers: dict = None
    ) -> dict:
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

        url, method = self.url_manager.url_for("check_version", subject=subject)
        body = {"schema": json.dumps(avro_schema.schema)}

        result, code = self.request(url, method=method, body=body, headers=headers)
        if code == status.HTTP_404_NOT_FOUND:
            logger.error(f"Not found: {code}")
            return
        elif status.is_success(code):
            schema_id = result.get("id")
            version = result.get("version")
            self._cache_schema(avro_schema, schema_id, subject, version)

            return utils.SchemaVersion(
                subject, schema_id, version, result.get("schema")
            )

        raise ClientError(
            "Unable to get version of a schema", http_code=code, server_traceback=result
        )

    def test_compatibility(
        self,
        subject: str,
        avro_schema: AvroSchema,
        version="latest",
        headers: dict = None,
    ) -> bool:
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
        url, method = self.url_manager.url_for(
            "test_compatibility", subject=subject, version=version
        )
        body = {"schema": json.dumps(avro_schema.schema)}
        result, code = self.request(url, method=method, body=body, headers=headers)

        if code == status.HTTP_404_NOT_FOUND:
            logger.error(f"Subject or version not found: {code}")
            return False
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            logger.error(f"Invalid subject or schema: {code}")
            return False
        elif status.is_success(code):
            return result.get("is_compatible")

        raise ClientError(
            "Unable to check the compatibility", http_code=code, server_traceback=result
        )

    def update_compatibility(
        self, level: str, subject: str = None, headers: dict = None
    ) -> bool:
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

        url, method = self.url_manager.url_for("update_compatibility", subject=subject)
        body = {"compatibility": level}

        result, code = self.request(url, method=method, body=body, headers=headers)

        if status.is_success(code):
            return True

        raise ClientError(
            f"Unable to update level: {level}.", http_code=code, server_traceback=result
        )

    def get_compatibility(self, subject: str = None, headers: dict = None) -> str:
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
        url, method = self.url_manager.url_for("get_compatibility", subject=subject)
        result, code = self.request(url, method=method, headers=headers)

        if status.is_success(code):
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

        raise ClientError(
            f"Unable to fetch compatibility level. Error code: {code}",
            http_code=code,
            server_traceback=result,
        )
