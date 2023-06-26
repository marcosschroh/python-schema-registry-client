import json
import logging
import typing
from abc import abstractmethod
from collections import defaultdict
from urllib.parse import urlparse

import httpx
from httpx import USE_CLIENT_DEFAULT, Auth, BasicAuth
from httpx._client import UseClientDefault
from httpx._types import TimeoutTypes

from . import status, utils
from .errors import ClientError
from .paths import paths
from .schema import AvroSchema, BaseSchema, JsonSchema, SchemaFactory, SubjectVersion
from .urls import UrlManager

logger = logging.getLogger(__name__)


def get_response_and_status_code(response: httpx.Response) -> tuple:
    """
    Returns a tuple containing response json and status code

    Args:
        response (httpx.Response): Http response object

    Returns:
       tuple(dict, int)
    """

    return (
        response.json(),
        response.status_code,
    )


class BaseClient:
    """
    A client that talks to a Schema Registry over HTTP

    Attributes:
        url str | typing.Dict: Url to schema registry or dictionary containing client configuration.
        ca_location str | None: File or directory path to CA certificate(s)
            for verifying the Schema Registry key.
        cert_location str | None: Path to public key used for authentication.
        key_location str | None: Path to private key used for authentication.
        key_password str | None: Key password
        extra_headers str | None: Extra headers to add on every requests.
        timeout httpx.Timeout | None: The timeout configuration to use when sending requests.
        pool_limits httpx.Limits | None: The connection pool configuration to use when
            determining the maximum number of concurrently open HTTP connections.
        auth httpx.Auth | None: Auth credentials.
    """

    def __init__(
        self,
        url: typing.Union[str, typing.Dict],
        ca_location: typing.Optional[str] = None,
        cert_location: typing.Optional[str] = None,
        key_location: typing.Optional[str] = None,
        key_password: typing.Optional[str] = None,
        extra_headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Optional[httpx.Timeout] = None,
        pool_limits: typing.Optional[httpx.Limits] = None,
        auth: typing.Optional[Auth] = None,
    ) -> None:
        if isinstance(url, str):
            conf = {
                utils.URL: url,
                utils.SSL_CA_LOCATION: ca_location,
                utils.SSL_CERTIFICATE_LOCATION: cert_location,
                utils.SSL_KEY_LOCATION: key_location,
                utils.SSL_KEY_PASSWORD: key_password,
            }
        else:
            conf = url

        self.conf = conf
        schema_server_url = conf.get(utils.URL, "")

        self.url_manager = UrlManager(schema_server_url, paths)  # type: ignore
        self.extra_headers = extra_headers
        self.timeout = timeout
        self.pool_limits = pool_limits
        self.auth = auth

        self.client_kwargs = self._get_client_kwargs()

        # Cache Schemas: subj => { schema => id }
        self.subject_to_schema_ids: dict = defaultdict(dict)
        # Cache Schemas: id => avro_schema
        self.id_to_schema: dict = defaultdict(dict)
        # Cache Schemas: subj => { schema => version }
        self.subject_to_schema_versions: dict = defaultdict(dict)

    def __eq__(self, obj: typing.Any) -> bool:
        return self.conf == obj.conf and self.extra_headers == obj.extra_headers

    @staticmethod
    def _schema_from_result(result: typing.Dict) -> typing.Union[JsonSchema, AvroSchema]:
        schema: str = result["schema"]
        schema_type = result.get("schemaType", utils.AVRO_SCHEMA_TYPE)
        return SchemaFactory.create_schema(schema, schema_type)

    def _configure_auth(self) -> Auth:
        # Check first if the credentials are sent in Auth
        if self.auth is not None:
            return self.auth

        # This part should be deprecated with a new mayor version. Url should be only a string
        url = self.conf["url"]
        auth_provider = self.conf.pop("basic.auth.credentials.source", "URL").upper()  # type: ignore

        if auth_provider not in utils.VALID_AUTH_PROVIDERS:
            raise ValueError(
                f"""
                schema.registry.basic.auth.credentials.source
                must be one of {utils.VALID_AUTH_PROVIDERS}
                """
            )

        if auth_provider == "USER_INFO":
            logger.warning("Deprecation warning: This will be deprecated in future versions. Use httpx.Auth instead")
            auth = BasicAuth(*self.conf.pop("basic.auth.user.info", "").split(":"))  # type: ignore
        else:
            # Credentials might be in the url.
            parsed_url = urlparse(url)
            auth = BasicAuth(parsed_url.username or "", parsed_url.password or "")

        # remove ignore after mypy fix https://github.com/python/mypy/issues/4805
        return auth  # type: ignore

    @staticmethod
    def _configure_client_tls(
        conf: dict,
    ) -> typing.Optional[typing.Union[str, typing.Tuple[str, str], typing.Tuple[str, str, str]]]:
        certificate = conf.get(utils.SSL_CERTIFICATE_LOCATION)

        if certificate is not None:
            key_path = conf.get(utils.SSL_KEY_LOCATION)
            key_password = conf.get(utils.SSL_KEY_PASSWORD)

            if key_path is not None:
                certificate = (
                    certificate,
                    key_path,
                )

                if key_password is not None:
                    certificate += (key_password,)

        return certificate

    def _get_client_kwargs(self) -> typing.Dict:
        verify = self.conf.get(utils.SSL_CA_LOCATION, False)
        certificate = self._configure_client_tls(self.conf)
        auth = self._configure_auth()

        client_kwargs = {
            "cert": certificate,
            "verify": verify,  # type: ignore
            "auth": auth,
        }

        # If these values haven't been explicitly defined let httpx sort out
        # the default values.
        if self.extra_headers is not None:
            client_kwargs["headers"] = self.extra_headers  # type:ignore

        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout  # type:ignore

        if self.pool_limits is not None:
            client_kwargs["limits"] = self.pool_limits  # type:ignore
        return client_kwargs

    def prepare_headers(
        self, body: typing.Optional[typing.Dict] = None, headers: typing.Optional[typing.Dict] = None
    ) -> dict:
        _headers = {"Accept": utils.ACCEPT_HEADERS}

        if body:
            _headers["Content-Type"] = utils.HEADERS

        if headers:
            _headers.update(headers)

        return _headers

    @staticmethod
    def _add_to_cache(
        cache: dict, subject: str, schema: typing.Union[BaseSchema, str], value: typing.Union[str, int]
    ) -> None:
        sub_cache = cache[subject]
        sub_cache[schema] = value

    def _cache_schema(
        self,
        schema: typing.Union[BaseSchema, str],
        schema_id: int,
        subject: typing.Optional[str] = None,
        version: typing.Union[str, int, None] = None,
    ) -> None:
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            self.id_to_schema[schema_id] = schema

        if subject:
            self._add_to_cache(self.subject_to_schema_ids, subject, schema, schema_id)
            if version:
                self._add_to_cache(self.subject_to_schema_versions, subject, schema, version)

    @abstractmethod
    def request(
        self,
        url: str,
        method: str = "GET",
        body: typing.Optional[typing.Dict] = None,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Union[tuple, httpx.Response, typing.Coroutine[typing.Any, typing.Any, typing.Any]]:
        _headers = self.prepare_headers(body=body, headers=headers)
        with httpx.Client(**self.client_kwargs) as client:
            response = client.request(method, url, headers=_headers, json=body, timeout=timeout)
        return response


class SchemaRegistryClient(BaseClient):
    """
    A client that talks to a Schema Registry over HTTP

    !!! Examlple
        ```python title="Usage"
        from schema_registry.client import SchemaRegistryClient, schema

        client = SchemaRegistryClient(url="http://127.0.0.1:8081")

        deployment_schema = {
            "type": "record",
            "namespace": "com.kubertenes",
            "name": "AvroDeployment",
            "fields": [
                {"name": "image", "type": "string"},
                {"name": "replicas", "type": "int"},
                {"name": "port", "type": "int"},
            ],
        }

        avro_schema = schema.AvroSchema(deployment_schema)
        schema_id = client.register("test-deployment", avro_schema)
        ```

    Attributes:
        url str | typing.Dict: Url to schema registry or dictionary containing client configuration.
        ca_location str | None: File or directory path to CA certificate(s)
            for verifying the Schema Registry key.
        cert_location str | None: Path to public key used for authentication.
        key_location str | None: Path to private key used for authentication.
        key_password str | None: Key password
        extra_headers str | None: Extra headers to add on every requests.
        timeout httpx.Timeout | None: The timeout configuration to use when sending requests.
        pool_limits httpx.Limits | None: The connection pool configuration to use when
            determining the maximum number of concurrently open HTTP connections.
        auth httpx.Auth | None: Auth credentials.
    """

    def request(
        self,
        url: str,
        method: str = "GET",
        body: typing.Optional[typing.Dict] = None,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> httpx.Response:
        if method not in utils.VALID_METHODS:
            raise ClientError(f"Method {method} is invalid; valid methods include {utils.VALID_METHODS}")

        _headers = self.prepare_headers(body=body, headers=headers)
        with httpx.Client(**self.client_kwargs) as client:
            response = client.request(method, url, headers=_headers, json=body, timeout=timeout)
        return response

    def register(
        self,
        subject: str,
        schema: typing.Union[BaseSchema, str],
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
        schema_type: str = utils.AVRO_SCHEMA_TYPE,
    ) -> int:
        """
        Register a schema for a subject

        Schema can be avro or json, and can be presented as a parsed schema or a string.
        If schema is a string, the `schema_type` kwarg must be used to indicate
        what type of schema the string is (`AVRO` by default).
        If the schema is already parsed, the schema_type is inferred directly from the parsed schema.
        Multiple instances of the same schema will result in cache misses.

        Attributes:
            subject str: subject name
            schema typing.Union[client.schema.BaseSchema, str]: Avro or JSON
                schema to be registered
            headers Dict: Extra headers to add on the requests
            timeout Union[TimeoutTypes, UseClientDefault]: The timeout configuration
                to use when sending requests. Default USE_CLIENT_DEFAULT
            schema_type typing.Union[AVRO, JSON]: The type of schema to
                parse if `schema` is a string. Default "AVRO"

        Returns:
            schema_id
        """
        schemas_to_id = self.subject_to_schema_ids[subject]

        if isinstance(schema, str):
            schema = SchemaFactory.create_schema(schema, schema_type)

        schema_id = schemas_to_id.get(schema)

        if schema_id is not None:
            return schema_id

        # Check if schema is already registered. This should normally work even if
        # the schema registry we're talking to is readonly, enabling a setup where
        # only CI/CD can do changes to Schema Registry, and production code has readonly
        # access

        response = self.check_version(subject, schema, headers=headers, timeout=timeout)
        if response is not None:
            return response.schema_id

        url, method = self.url_manager.url_for("register", subject=subject)
        body = {"schema": json.dumps(schema.raw_schema), "schemaType": schema.schema_type}

        (
            result,
            code,
        ) = get_response_and_status_code(self.request(url, method=method, body=body, headers=headers, timeout=timeout))
        msg = None
        if code in (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN):
            msg = "Unauthorized access"
        elif code == status.HTTP_409_CONFLICT:
            msg = "Incompatible schema"
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            msg = "Invalid schema"
        elif not status.is_success(code):
            msg = "Unable to register schema"

        if msg is not None:
            raise ClientError(message=msg, http_code=code, server_traceback=result)

        schema_id = result["id"]
        self._cache_schema(schema, schema_id, subject)

        return schema_id

    def get_subjects(
        self,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> list:
        """
        GET /subjects/(string: subject)
        Get list of all registered subjects in your Schema Registry.

        Attributes:
            subject str: subject name
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            List of registered subjects.
        """
        url, method = self.url_manager.url_for("get_subjects")
        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))

        if status.is_success(code):
            return result

        raise ClientError("Unable to get subjects", http_code=code, server_traceback=result)

    def delete_subject(
        self,
        subject: str,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> list:
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be
        recycled or in development environments.

        Attributes:
            subject str: subject name
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
             Default USE_CLIENT_DEFAULT

        Returns:
            List version of the schema deleted under this subject
        """
        url, method = self.url_manager.url_for("delete_subject", subject=subject)
        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))

        if status.is_success(code):
            return result
        elif code == status.HTTP_404_NOT_FOUND:
            return []

        raise ClientError("Unable to delete subject", http_code=code, server_traceback=result)

    def get_by_id(
        self,
        schema_id: int,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[typing.Union[AvroSchema, JsonSchema]]:
        """
        GET /schemas/ids/{int: id}
        Retrieve a parsed avro schema by id or None if not found

        Attributes:
            schema_id int: Schema Id
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                Default USE_CLIENT_DEFAULT

        Returns:
            Avro or JSON schema
        """
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]

        url, method = self.url_manager.url_for("get_by_id", schema_id=schema_id)

        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))
        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Schema {schema_id} not found: {code}")
            return None
        elif status.is_success(code):
            schema = self._schema_from_result(result)
            self._cache_schema(schema, schema_id)
            return schema

        raise ClientError(f"Received bad schema (id {schema_id})", http_code=code, server_traceback=result)

    def get_schema_subject_versions(
        self,
        schema_id: int,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[typing.List[SubjectVersion]]:
        """
        GET /schemas/ids/{int: id}/versions
        Get the subject-version pairs identified by the input ID.

        Attributes:
            schema_id int: Schema Id
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            List of Subject/Version pairs where Schema Id is registered
        """
        url, method = self.url_manager.url_for("get_schema_subject_versions", schema_id=schema_id)
        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))
        if code == status.HTTP_404_NOT_FOUND:
            logger.warning(f"Schema {schema_id} not found: {code}")
            return None
        elif status.is_success(code):
            ret = []
            for entry in result:
                ret.append(SubjectVersion(entry["subject"], entry["version"]))
            return ret

        raise ClientError(f"Received bad schema (id {schema_id})", http_code=code, server_traceback=result)

    def get_schema(
        self,
        subject: str,
        version: typing.Union[int, str] = "latest",
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[utils.SchemaVersion]:
        """
        GET /subjects/(string: subject)/versions/(versionId: version)
        Get a specific version of the schema registered under this subject

        Attributes:
            subject str: subject name
            version int, optional: version id. If is None, the latest schema is returned
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
             Default USE_CLIENT_DEFAULT

        Returns:
            The SchemaVersion utils.SchemaVersion if response was succeded
        """
        url, method = self.url_manager.url_for("get_schema", subject=subject, version=version)

        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))
        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Schema version {version} under subjet {subject} not found: {code}")
            return None
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            logger.info(f"Invalid version {version}: {code}")
            return None
        elif not status.is_success(code):
            logger.info(f"Not success version {version}: {code}")
            return None

        schema_id = result.get("id")
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            schema = self._schema_from_result(result)

        version = result["version"]
        self._cache_schema(schema, schema_id, subject, version)

        return utils.SchemaVersion(subject, schema_id, schema, version)

    def get_versions(
        self,
        subject: str,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> list:
        """
        GET subjects/{subject}/versions
        Get a list of versions registered under the specified subject.

        Attributes:
            subject str: subject name
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                Default USE_CLIENT_DEFAULT

        Returns:
            List  version of the schema registered under this subject
        """
        url, method = self.url_manager.url_for("get_versions", subject=subject)

        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))
        if status.is_success(code):
            return result
        elif code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Subject {subject} not found")
            return []

        raise ClientError(f"Unable to get the versions for subject {subject}", http_code=code, server_traceback=result)

    def delete_version(
        self,
        subject: str,
        version: typing.Union[int, str] = "latest",
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[int]:
        """
        DELETE /subjects/(string: subject)/versions/(versionId: version)
        Deletes a specific version of the schema registered under this subject.
        This only deletes the version and the schema ID remains intact making
        it still possible to decode data using the schema ID.
        This API is recommended to be used only in development environments or
        under extreme circumstances where-in, its required to delete a previously
        registered schema for compatibility purposes or re-register previously registered schema.

        Attributes:
            subject str: subject name
            version str: Version of the schema to be deleted.
                Valid values for versionId are between [1,2^31-1] or the string "latest".
                "latest" deletes the last registered schema under the specified subject.
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            Version of the schema deleted. If the subject or version does not exist.
        """
        url, method = self.url_manager.url_for("delete_version", subject=subject, version=version)

        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))

        if status.is_success(code):
            return result
        elif status.is_client_error(code):
            return None

        raise ClientError("Unable to delete the version", http_code=code, server_traceback=result)

    def check_version(
        self,
        subject: str,
        schema: typing.Union[BaseSchema, str],
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
        schema_type: str = utils.AVRO_SCHEMA_TYPE,
    ) -> typing.Optional[utils.SchemaVersion]:
        """
        POST /subjects/(string: subject)
        Check if a schema has already been registered under the specified subject.
        If so, this returns the schema string along with its globally unique identifier,
        its version under this subject and the subject name.

        Attributes:
            subject str: subject name
            schema typing.Union[client.schema.BaseSchema, str]: Avro or JSON schema
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes:
                The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
            schema_type typing.Union[AVRO, JSON]:
                The type of schema to parse if `schema` is a string. Default "AVRO"

        Returns:
            SchemaVersion If schema exist
        """
        schemas_to_version = self.subject_to_schema_versions[subject]

        if isinstance(schema, str):
            schema = SchemaFactory.create_schema(schema, schema_type)

        version = schemas_to_version.get(schema)

        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id = schemas_to_id.get(schema)

        if all((version, schema_id)):
            return utils.SchemaVersion(subject, schema_id, version, schema)

        url, method = self.url_manager.url_for("check_version", subject=subject)
        body = {"schema": json.dumps(schema.raw_schema), "schemaType": schema.schema_type}
        result, code = get_response_and_status_code(
            self.request(url, method=method, body=body, headers=headers, timeout=timeout)
        )
        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Schema {schema.name} under subject {subject} not found: {code}")
            return None
        elif status.is_success(code):
            schema_id = result.get("id")
            version = result.get("version")
            self._cache_schema(schema, schema_id, subject, version)

            return utils.SchemaVersion(subject, schema_id, version, result.get("schema"))

        raise ClientError("Unable to get version of a schema", http_code=code, server_traceback=result)

    def test_compatibility(
        self,
        subject: str,
        schema: typing.Union[AvroSchema, JsonSchema, str],
        version: typing.Union[int, str] = "latest",
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
        schema_type: str = utils.AVRO_SCHEMA_TYPE,
    ) -> bool:
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)
        Test the compatibility of a candidate parsed schema for a given subject.
        By default the latest version is checked against.

        Attributes:
            subject str: subject name
            schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]:
                Avro or JSON schema
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes:
                The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
            schema_type typing.Union[AVRO, JSON]:
                The type of schema to parse if `schema` is a string. Default "AVRO"

        Returns:
            Wether the schema is compatible with the latest version for a subject
        """
        url, method = self.url_manager.url_for("test_compatibility", subject=subject, version=version)

        if isinstance(schema, str):
            schema = SchemaFactory.create_schema(schema, schema_type)

        body = {"schema": json.dumps(schema.raw_schema), "schemaType": schema.schema_type}
        result, code = get_response_and_status_code(
            self.request(url, method=method, body=body, headers=headers, timeout=timeout)
        )

        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Subject or version not found: {code}")
            return False
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            logger.info(f"Unprocessable entity. Invalid subject or schema: {code}")
        elif status.is_success(code):
            return result.get("is_compatible")

        raise ClientError("Unable to check the compatibility", http_code=code, server_traceback=result)

    def update_compatibility(
        self,
        level: str,
        subject: typing.Optional[str] = None,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> bool:
        """
        PUT /config/(string: subject)
        Update the compatibility level.
        If subject is None, the compatibility level is global.

        Attributes:
            level str: one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
                FULL, FULL_TRANSITIVE, NONE
            subject str: Option subject
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            Whether the compatibility was updated

        Raises:
            ClientError: if the request was unsuccessful or an invalid
        """
        if level not in utils.VALID_LEVELS:
            raise ClientError(f"Invalid level specified: {level}")

        url, method = self.url_manager.url_for("update_compatibility", subject=subject)
        body = {"compatibility": level}

        result, code = get_response_and_status_code(
            self.request(url, method=method, body=body, headers=headers, timeout=timeout)
        )

        if status.is_success(code):
            return True

        raise ClientError(f"Unable to update level: {level}.", http_code=code, server_traceback=result)

    def get_compatibility(
        self,
        subject: typing.Optional[str] = None,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> str:
        """
        Get the current compatibility level for a subject.

        Attributes:
            subject str: subject name
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            One of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
                FULL, FULL_TRANSITIVE, NONE

        Raises:
            ClientError: if the request was unsuccessful or an invalid
                compatibility level was returned
        """
        url, method = self.url_manager.url_for("get_compatibility", subject=subject)
        result, code = get_response_and_status_code(self.request(url, method=method, headers=headers, timeout=timeout))

        if status.is_success(code):
            compatibility = result.get("compatibilityLevel")
            if compatibility not in utils.VALID_LEVELS:
                if compatibility is None:
                    error_msg_suffix = "No compatibility was returned"
                else:
                    error_msg_suffix = str(compatibility)
                raise ClientError(
                    f"Invalid compatibility level received: {error_msg_suffix}", http_code=code, server_traceback=result
                )

            return compatibility

        raise ClientError(
            f"Unable to fetch compatibility level. Error code: {code}", http_code=code, server_traceback=result
        )


class AsyncSchemaRegistryClient(BaseClient):
    """
    A client that talks to a Schema Registry over HTTP

    Attributes:
        url str | typing.Dict: Url to schema registry or dictionary containing client configuration.
        ca_location str | None: File or directory path to CA certificate(s)
            for verifying the Schema Registry key.
        cert_location str | None: Path to public key used for authentication.
        key_location str | None: Path to private key used for authentication.
        key_password str | None: Key password
        extra_headers str | None: Extra headers to add on every requests.
        timeout httpx.Timeout | None: The timeout configuration to use when sending requests.
        pool_limits httpx.Limits | None: The connection pool configuration to use when
            determining the maximum number of concurrently open HTTP connections.
        auth httpx.Auth | None: Auth credentials.
    """

    async def request(
        self,
        url: str,
        method: str = "GET",
        body: typing.Optional[typing.Dict] = None,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> httpx.Response:
        if method not in utils.VALID_METHODS:
            raise ClientError(f"Method {method} is invalid; valid methods include {utils.VALID_METHODS}")

        _headers = self.prepare_headers(body=body, headers=headers)
        async with httpx.AsyncClient(**self.client_kwargs) as client:
            response = await client.request(method, url, headers=_headers, json=body, timeout=timeout)

        return response

    async def register(
        self,
        subject: str,
        schema: typing.Union[BaseSchema, str],
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
        schema_type: str = utils.AVRO_SCHEMA_TYPE,
    ) -> int:
        """
        POST /subjects/(string: subject)/versions
        Register a schema with the registry under the given subject
        and receive a schema id.
        Schema can be avro or json, and can be presented as a parsed schema or a string.
        If schema is a string, the `schema_type` kwarg must be used to indicate
        what type of schema the string is ("AVRO" by default).
        If the schema is already parsed, the schema_type is inferred directly from the parsed schema.
        Multiple instances of the same schema will result in cache misses.

        Attributes:
            subject str: subject name
            schema typing.Union[client.schema.BaseSchema, str]: Avro or JSON
                schema to be registered
            headers Dict: Extra headers to add on the requests
            timeout Union[TimeoutTypes, UseClientDefault]: The timeout configuration
                to use when sending requests. Default USE_CLIENT_DEFAULT
            schema_type typing.Union[AVRO, JSON]: The type of schema to
                parse if `schema` is a string. Default "AVRO"

        Returns:
            schema_id
        """
        schemas_to_id = self.subject_to_schema_ids[subject]

        if isinstance(schema, str):
            schema = SchemaFactory.create_schema(schema, schema_type)

        schema_id = schemas_to_id.get(schema)

        if schema_id is not None:
            return schema_id

        # Check if schema is already registered. This should normally work even if
        # the schema registry we're talking to is readonly, enabling a setup where
        # only CI/CD can do changes to Schema Registry, and production code has readonly
        # access

        response = await self.check_version(subject, schema, headers=headers, timeout=timeout)

        if response is not None:
            return response.schema_id

        url, method = self.url_manager.url_for("register", subject=subject)
        body = {"schema": json.dumps(schema.raw_schema), "schemaType": schema.schema_type}

        result, code = get_response_and_status_code(
            await self.request(url, method=method, body=body, headers=headers, timeout=timeout)
        )

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
        self._cache_schema(schema, schema_id, subject)

        return schema_id

    async def get_subjects(
        self,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> list:
        """
        GET /subjects/(string: subject)
        Get list of all registered subjects in your Schema Registry.

        Attributes:
            subject str: subject name
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            List of registered subjects.
        """
        url, method = self.url_manager.url_for("get_subjects")
        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )

        if status.is_success(code):
            return result

        raise ClientError("Unable to get subjects", http_code=code, server_traceback=result)

    async def delete_subject(
        self,
        subject: str,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> list:
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be
        recycled or in development environments.

        Attributes:
            subject str: subject name
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
             Default USE_CLIENT_DEFAULT

        Returns:
            List version of the schema deleted under this subject
        """
        url, method = self.url_manager.url_for("delete_subject", subject=subject)
        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )

        if status.is_success(code):
            return result
        elif code == status.HTTP_404_NOT_FOUND:
            return []

        raise ClientError("Unable to delete subject", http_code=code, server_traceback=result)

    async def get_by_id(
        self,
        schema_id: int,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[typing.Union[AvroSchema, JsonSchema]]:
        """
        GET /schemas/ids/{int: id}
        Retrieve a parsed avro schema by id or None if not found

        Attributes:
            schema_id int: Schema Id
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                Default USE_CLIENT_DEFAULT

        Returns:
            Avro or JSON schema
        """
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]

        url, method = self.url_manager.url_for("get_by_id", schema_id=schema_id)

        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )
        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Schema {schema_id} not found: {code}")
            return None
        elif status.is_success(code):
            schema = self._schema_from_result(result)
            self._cache_schema(schema, schema_id)
            return schema

        raise ClientError(f"Received bad schema (id {schema_id})", http_code=code, server_traceback=result)

    async def get_schema(
        self,
        subject: str,
        version: typing.Union[int, str] = "latest",
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[utils.SchemaVersion]:
        """
        GET /subjects/(string: subject)/versions/(versionId: version)
        Get a specific version of the schema registered under this subject

        Args:
            subject (str): subject name
            version (int, optional): version id. If is None, the latest schema is returned
            headers (dict): Extra headers to add on the requests
            timeout (httpx._client.TimeoutTypes): The timeout configuration to use when sending requests.
                Default USE_CLIENT_DEFAULT

        Returns:
            SchemaVersion (nametupled): (subject, schema_id, schema, version)

            None: If server returns a not success response:
                404: Schema not found
                422: Unprocessable entity
                ~ (200 - 299): Not success
        """
        url, method = self.url_manager.url_for("get_schema", subject=subject, version=version)

        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )

        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Schema version {version} under subjet {subject} not found: {code}")
            return None
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            logger.info(f"Invalid version {version}: {code}")
            return None
        elif not status.is_success(code):
            logger.info(f"Not success version {version}: {code}")
            return None

        schema_id = result.get("id")
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            schema = self._schema_from_result(result)

        version = result["version"]
        self._cache_schema(schema, schema_id, subject, version)

        return utils.SchemaVersion(subject, schema_id, schema, version)

    async def get_schema_subject_versions(
        self,
        schema_id: int,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[typing.List[SubjectVersion]]:
        """
        GET /schemas/ids/{int: id}/versions
        Get the subject-version pairs identified by the input ID.

        Attributes:
            schema_id int: Schema Id
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            List of Subject/Version pairs where Schema Id is registered
        """
        url, method = self.url_manager.url_for("get_schema_subject_versions", schema_id=schema_id)
        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )

        if code == status.HTTP_404_NOT_FOUND:
            logger.warning(f"Schema {schema_id} not found: {code}")
            return None
        elif status.is_success(code):
            ret = []
            for entry in result:
                ret.append(SubjectVersion(entry["subject"], entry["version"]))
            return ret

        raise ClientError(f"Received bad schema (id {schema_id})", http_code=code, server_traceback=result)

    async def get_versions(
        self,
        subject: str,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> list:
        """
        GET subjects/{subject}/versions
        Get a list of versions registered under the specified subject.

        Attributes:
            subject str: subject name
            headers dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                Default USE_CLIENT_DEFAULT

        Returns:
            List  version of the schema registered under this subject
        """
        url, method = self.url_manager.url_for("get_versions", subject=subject)

        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )
        if status.is_success(code):
            return result
        elif code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Subject {subject} not found")
            return []

        raise ClientError(f"Unable to get the versions for subject {subject}", http_code=code, server_traceback=result)

    async def delete_version(
        self,
        subject: str,
        version: typing.Union[int, str] = "latest",
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> typing.Optional[int]:
        """
        DELETE /subjects/(string: subject)/versions/(versionId: version)
        Deletes a specific version of the schema registered under this subject.
        This only deletes the version and the schema ID remains intact making
        it still possible to decode data using the schema ID.
        This API is recommended to be used only in development environments or
        under extreme circumstances where-in, its required to delete a previously
        registered schema for compatibility purposes or re-register previously registered schema.

        Attributes:
            subject str: subject name
            version str: Version of the schema to be deleted.
                Valid values for versionId are between [1,2^31-1] or the string "latest".
                "latest" deletes the last registered schema under the specified subject.
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            Version of the schema deleted. If the subject or version does not exist.
        """
        url, method = self.url_manager.url_for("delete_version", subject=subject, version=version)

        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )

        if status.is_success(code):
            return result
        elif status.is_client_error(code):
            return None

        raise ClientError("Unable to delete the version", http_code=code, server_traceback=result)

    async def check_version(
        self,
        subject: str,
        schema: typing.Union[BaseSchema, str],
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
        schema_type: str = utils.AVRO_SCHEMA_TYPE,
    ) -> typing.Optional[utils.SchemaVersion]:
        """
        POST /subjects/(string: subject)
        Check if a schema has already been registered under the specified subject.
        If so, this returns the schema string along with its globally unique identifier,
        its version under this subject and the subject name.

        Attributes:
            subject str: subject name
            schema typing.Union[client.schema.BaseSchema, str]: Avro or JSON schema
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes:
                The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
            schema_type typing.Union[AVRO, JSON]:
                The type of schema to parse if `schema` is a string. Default "AVRO"

        Returns:
            SchemaVersion If schema exist
        """
        schemas_to_version = self.subject_to_schema_versions[subject]

        if isinstance(schema, str):
            schema = SchemaFactory.create_schema(schema, schema_type)

        version = schemas_to_version.get(schema)

        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id = schemas_to_id.get(schema)

        if all((version, schema_id)):
            return utils.SchemaVersion(subject, schema_id, version, schema)

        url, method = self.url_manager.url_for("check_version", subject=subject)
        body = {"schema": json.dumps(schema.raw_schema), "schemaType": schema.schema_type}

        result, code = get_response_and_status_code(
            await self.request(url, method=method, body=body, headers=headers, timeout=timeout)
        )
        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Schema {schema.name} under subject {subject} not found: {code}")
            return None
        elif status.is_success(code):
            schema_id = result.get("id")
            version = result.get("version")
            self._cache_schema(schema, schema_id, subject, version)

            return utils.SchemaVersion(subject, schema_id, version, result.get("schema"))

        raise ClientError("Unable to get version of a schema", http_code=code, server_traceback=result)

    async def test_compatibility(
        self,
        subject: str,
        schema: typing.Union[AvroSchema, JsonSchema, str],
        version: typing.Union[int, str] = "latest",
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
        schema_type: str = utils.AVRO_SCHEMA_TYPE,
    ) -> bool:
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)
        Test the compatibility of a candidate parsed schema for a given subject.
        By default the latest version is checked against.

        Attributes:
            subject str: subject name
            schema typing.Union[client.schema.AvroSchema, client.schema.JsonSchema, str]:
                Avro or JSON schema
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes:
                The timeout configuration to use when sending requests. Default USE_CLIENT_DEFAULT
            schema_type typing.Union[AVRO, JSON]:
                The type of schema to parse if `schema` is a string. Default "AVRO"

        Returns:
            Wether the schema is compatible with the latest version for a subject
        """
        url, method = self.url_manager.url_for("test_compatibility", subject=subject, version=version)

        if isinstance(schema, str):
            schema = SchemaFactory.create_schema(schema, schema_type)

        body = {"schema": json.dumps(schema.raw_schema), "schemaType": schema.schema_type}
        result, code = get_response_and_status_code(
            await self.request(url, method=method, body=body, headers=headers, timeout=timeout)
        )

        if code == status.HTTP_404_NOT_FOUND:
            logger.info(f"Subject or version not found: {code}")
            return False
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            logger.info(f"Unprocessable entity. Invalid subject or schema: {code}")
            return False
        elif status.is_success(code):
            return result.get("is_compatible")

        raise ClientError("Unable to check the compatibility", http_code=code, server_traceback=result)

    async def update_compatibility(
        self,
        level: str,
        subject: typing.Optional[str] = None,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> bool:
        """
        PUT /config/(string: subject)
        Update the compatibility level.
        If subject is None, the compatibility level is global.

        Attributes:
            level str: one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
                FULL, FULL_TRANSITIVE, NONE
            subject str: Option subject
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            Whether the compatibility was updated

        Raises:
            ClientError: if the request was unsuccessful or an invalid
        """
        if level not in utils.VALID_LEVELS:
            raise ClientError(f"Invalid level specified: {level}")

        url, method = self.url_manager.url_for("update_compatibility", subject=subject)
        body = {"compatibility": level}

        result, code = get_response_and_status_code(
            await self.request(url, method=method, body=body, headers=headers, timeout=timeout)
        )

        if status.is_success(code):
            return True

        raise ClientError(f"Unable to update level: {level}.", http_code=code, server_traceback=result)

    async def get_compatibility(
        self,
        subject: typing.Optional[str] = None,
        headers: typing.Optional[typing.Dict] = None,
        timeout: typing.Union[TimeoutTypes, UseClientDefault] = USE_CLIENT_DEFAULT,
    ) -> str:
        """
        Get the current compatibility level for a subject.

        Attributes:
            subject str: subject name
            headers typing.Dict: Extra headers to add on the requests
            timeout httpx._client.TimeoutTypes: The timeout configuration to use when sending requests.
                    Default USE_CLIENT_DEFAULT

        Returns:
            One of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE,
                FULL, FULL_TRANSITIVE, NONE

        Raises:
            ClientError: if the request was unsuccessful or an invalid
                compatibility level was returned
        """
        url, method = self.url_manager.url_for("get_compatibility", subject=subject)
        result, code = get_response_and_status_code(
            await self.request(url, method=method, headers=headers, timeout=timeout)
        )

        if status.is_success(code):
            compatibility = result.get("compatibilityLevel")
            if compatibility not in utils.VALID_LEVELS:
                if compatibility is None:
                    error_msg_suffix = "No compatibility was returned"
                else:
                    error_msg_suffix = str(compatibility)
                raise ClientError(
                    f"Invalid compatibility level received: {error_msg_suffix}", http_code=code, server_traceback=result
                )

            return compatibility

        raise ClientError(
            f"Unable to fetch compatibility level. Error code: {code}", http_code=code, server_traceback=result
        )
