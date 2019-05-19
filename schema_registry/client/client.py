import json
import logging
import warnings
import requests
from collections import defaultdict

from schema_registry.client.errors import ClientError
from schema_registry.client.load import loads
from schema_registry.client import status, utils


log = logging.getLogger(__name__)


class SchemaRegistryClient:
    """
    A client that talks to a Schema Registry over HTTP

    Use SchemaRegistryClient(dict: config) instead.
    Existing params ca_location, cert_location and key_location will be replaced with their librdkafka equivalents:
    `ssl.ca.location`, `ssl.certificate.location` and `ssl.key.location` respectively.
    Errors communicating to the server will result in a ClientError being raised.

    Args:
        url (str|dict) url: Url to schema registry or dictionary containing client configuration.
        ca_location (str): File or directory path to CA certificate(s) for verifying the Schema Registry key.
        cert_location (str): Path to client's public key used for authentication.
        key_location (str): Path to client's private key used for authentication.
    """

    def __init__(self, url, ca_location=None, cert_location=None, key_location=None):

        conf = url
        if not isinstance(url, dict):
            conf = {
                "url": url,
                "ssl.ca.location": ca_location,
                "ssl.certificate.location": cert_location,
                "ssl.key.location": key_location,
            }
            warnings.warn(
                "CachedSchemaRegistry constructor is being deprecated. "
                "Use SchemaRegistryClient(dict: config) instead. "
                "Existing params ca_location, cert_location and key_location will be replaced with their "
                "librdkafka equivalents as keys in the conf dict: `ssl.ca.location`, `ssl.certificate.location` and "
                "`ssl.key.location` respectively",
                category=DeprecationWarning,
                stacklevel=2,
            )

        # Ensure URL valid scheme is included; http[s]
        url = conf.get("url", "")
        if not isinstance(url, str):
            raise TypeError("URL must be of type str")

        if not url.startswith("http"):
            raise ValueError("Invalid URL provided for Schema Registry")

        self.url = url.rstrip("/")

        # subj => { schema => id }
        self.subject_to_schema_ids = defaultdict(dict)
        # id => avro_schema
        self.id_to_schema = defaultdict(dict)
        # subj => { schema => version }
        self.subject_to_schema_versions = defaultdict(dict)

        session = requests.Session()
        session.verify = conf.pop("ssl.ca.location", None)
        session.cert = self._configure_client_tls(conf)
        session.auth = self._configure_basic_auth(conf)
        self._session = session
        self.url = conf.pop("url")

        if len(conf) > 0:
            raise ValueError("fUnrecognized configuration properties:{conf}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._session.close()

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

    def send(self, url, method="GET", body=None, headers=None):
        if method not in utils.VALID_METHODS:
            raise ClientError(
                f"Method {method} is invalid; valid methods include {utils.VALID_METHODS}"
            )

        if headers is None:
            headers = {}

        _headers = {"Accept": utils.ACCEPT_HEADERS}
        if body:
            _headers["Content-Length"] = str(len(body))
            _headers["Content-Type"] = utils.HEADERS
        _headers.update(headers)

        response = self._session.request(method, url, headers=_headers, json=body)

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

    def register(self, subject, avro_schema):
        """
        POST /subjects/(string: subject)/versions
        Register a schema with the registry under the given subject
        and receive a schema id.
        avro_schema must be a parsed schema from the python avro library
        Multiple instances of the same schema will result in cache misses.

        Args:
            subject (str): subject name
            avro_schema (avro.schema.RecordSchema): Avro schema to be registered

        Returns:
            int: schema_id
        """
        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id = schemas_to_id.get(avro_schema.name)

        if schema_id is not None:
            return schema_id

        url = "/".join([self.url, "subjects", subject, "versions"])

        body = {"schema": json.dumps(avro_schema.to_json())}

        result, code = self.send(url, method="POST", body=body)

        if code in (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN):
            raise ClientError(f"Unauthorized access. Error code: {code}")
        elif code == status.HTTP_409_CONFLICT:
            raise ClientError(f"Incompatible Avro schema: {code}")
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            raise ClientError(f"Invalid Avro schema: {code}")
        elif not (status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES):
            raise ClientError(f"Unable to register schema. Error code: {code}")

        schema_id = result["id"]
        self._cache_schema(avro_schema, schema_id, subject)

        return schema_id

    def delete_subject(self, subject):
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be recycled or in development environments.

        Args:
            subject (str): subject name

        Returns:
            int: version of the schema deleted under this subject
        """
        url = "/".join([self.url, "subjects", subject])

        result, code = self.send(url, method="DELETE")
        if not (status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES):
            raise ClientError(f"Unable to delete subject: {result}")
        return result

    def get_by_id(self, schema_id):
        """
        GET /schemas/ids/{int: id}
        Retrieve a parsed avro schema by id or None if not found

        Args:
            schema_id (int): Schema Id

        Returns:
            avro.schema.RecordSchema: Avro Record schema
        """
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]
        # fetch from the registry
        url = "/".join([self.url, "schemas", "ids", str(schema_id)])

        result, code = self.send(url)
        if code == status.HTTP_404_NOT_FOUND:
            log.error(f"Schema not found: {code}")
        elif not (status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES):
            log.error(f"Unable to get schema for the specific ID: {code}")
        else:
            # need to parse the schema
            schema_str = result.get("schema")
            try:
                result = loads(schema_str)

                # cache the result
                self._cache_schema(result, schema_id)
                return result
            except ClientError as e:
                # bad schema - should not happen
                raise ClientError(
                    f"Received bad schema (id {schema_id}) from registry: {e}"
                )

    def get_schema(self, subject, version="latest"):
        """
        GET /subjects/(string: subject)/versions/(versionId: version)
        Get a specific version of the schema registered under this subject

        If the subject is not found a Nametupled (None,None,None) is returned.

        Args:
            subject (str): subject name
            version (int, optional): version id. If is None, the latest schema is returned

        Returns:
            SchemaVersion (nametupled): (subject, schema_id, schema, version)
        """
        url = "/".join([self.url, "subjects", subject, "versions", str(version)])

        result, code = self.send(url)
        if code == status.HTTP_404_NOT_FOUND:
            log.error(f"Schema not found: {code}")
            return utils.SchemaVersion(None, None, None, None)
        elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
            log.error(f"Invalid version: {code}")
            return utils.SchemaVersion(None, None, None, None)
        elif not (status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES):
            return utils.SchemaVersion(None, None, None, None)
        schema_id = result["id"]
        version = result["version"]
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            try:
                schema = loads(result["schema"])
            except ClientError:
                # bad schema - should not happen
                raise

        self._cache_schema(schema, schema_id, subject, version)

        return utils.SchemaVersion(subject, schema_id, schema, version)

    def check_version(self, subject, avro_schema):
        """
        POST /subjects/(string: subject)
        Check if a schema has already been registered under the specified subject.
        If so, this returns the schema string along with its globally unique identifier,
        its version under this subject and the subject name.

        Args:
            subject (str): subject name
            avro_schema (avro.schema.RecordSchema): Avro schema

        Returns:
            int: Schema version
            None: If schema not found.
        """
        schemas_to_version = self.subject_to_schema_versions[subject]
        version = schemas_to_version.get(avro_schema)

        if version is not None:
            return version

        url = "/".join([self.url, "subjects", subject])
        body = {"schema": json.dumps(avro_schema.to_json())}

        result, code = self.send(url, method="POST", body=body)
        if code == status.HTTP_404_NOT_FOUND:
            log.error(f"Not found: {code}")
            return
        elif not (status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES):
            log.error(f"Unable to get version of a schema: {code}")
            return

        schema_id = result["id"]
        version = result["version"]
        self._cache_schema(avro_schema, schema_id, subject, version)

        return version

    def test_compatibility(self, subject, avro_schema, version="latest"):
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)
        Test the compatibility of a candidate parsed schema for a given subject.
        By default the latest version is checked against.

        Args:
            subject (str): subject name
            avro_schema (avro.schema.RecordSchema): Avro schema

        Returns:
            bool: True if compatible, False if not compatible
        """
        url = "/".join(
            [self.url, "compatibility", "subjects", subject, "versions", str(version)]
        )
        body = {"schema": json.dumps(avro_schema.to_json())}
        try:
            result, code = self.send(url, method="POST", body=body)
            if code == status.HTTP_404_NOT_FOUND:
                log.error(f"Subject or version not found: {code}")
                return False
            elif code == status.HTTP_422_UNPROCESSABLE_ENTITY:
                log.error("Invalid subject or schema: {code}")
                return False
            elif status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES:
                return result.get("is_compatible")
            else:
                log.error(f"Unable to check the compatibility: {code}")
                return False
        except Exception as e:
            log.error("send() failed: %s", e)
            return False

    def update_compatibility(self, level, subject=None):
        """
        PUT /config/(string: subject)
        Update the compatibility level for a subject. Level must be one of:

        Args:
            level (str): ex: 'NONE','FULL','FORWARD', or 'BACKWARD'

        Returns:
            None
        """
        if level not in utils.VALID_LEVELS:
            raise ClientError("Invalid level specified: %s" % (str(level)))

        url = "/".join([self.url, "config"])
        if subject:
            url += "/" + subject

        body = {"compatibility": level}
        result, code = self.send(url, method="PUT", body=body)
        if status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES:
            return result["compatibility"]
        else:
            raise ClientError(f"Unable to update level: {level}. Error code: {code}")

    def get_compatibility(self, subject):
        """
        Get the current compatibility level for a subject.

        Args:
            subject (str): subject name

        Returns:
            str: one of 'NONE','FULL','FORWARD', or 'BACKWARD'

        Raises:
            ClientError: if the request was unsuccessful or an invalid
            compatibility level was returned
        """
        url = "/".join([self.url, "config"])
        if subject:
            url = "/".join([url, subject])

        result, code = self.send(url)
        is_successful_request = (
            status.HTTP_200_OK <= code < status.HTTP_300_MULTIPLE_CHOICES
        )
        if not is_successful_request:
            raise ClientError(
                f"Unable to fetch compatibility level. Error code: {code}"
            )

        compatibility = result.get("compatibilityLevel")
        if compatibility not in utils.VALID_LEVELS:
            if compatibility is None:
                error_msg_suffix = "No compatibility was returned"
            else:
                error_msg_suffix = str(compatibility)
            raise ClientError(
                f"Invalid compatibility level received: {error_msg_suffix}"
            )

        return compatibility
