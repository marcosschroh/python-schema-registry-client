import json
import logging
import warnings
from collections import defaultdict

import requests_async as requests
from requests import utils

from .errors import ClientError
from .load import loads


VALID_LEVELS = ['NONE', 'FULL', 'FORWARD', 'BACKWARD']
VALID_METHODS = ['GET', 'POST', 'PUT', 'DELETE']
VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO', 'SASL_INHERIT']

# Common accept header sent
ACCEPT_HDR = "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"
log = logging.getLogger(__name__)


class CachedSchemaRegistryClient:
    """
    A client that talks to a Schema Registry over HTTP
    See http://confluent.io/docs/current/schema-registry/docs/intro.html for more information.
    .. deprecated::
    Use CachedSchemaRegistryClient(dict: config) instead.
    Existing params ca_location, cert_location and key_location will be replaced with their librdkafka equivalents:
    `ssl.ca.location`, `ssl.certificate.location` and `ssl.key.location` respectively.
    Errors communicating to the server will result in a ClientError being raised.
    :param str|dict url: url(deprecated) to schema registry or dictionary containing client configuration.
    :param str ca_location: File or directory path to CA certificate(s) for verifying the Schema Registry key.
    :param str cert_location: Path to client's public key used for authentication.
    :param str key_location: Path to client's private key used for authentication.
    """

    def __init__(self, url, max_schemas_per_subject=1000, ca_location=None,
                 cert_location=None, key_location=None):

        conf = url
        if not isinstance(url, dict):
            conf = {
                'url': url,
                'ssl.ca.location': ca_location,
                'ssl.certificate.location': cert_location,
                'ssl.key.location': key_location
            }
            warnings.warn(
                "CachedSchemaRegistry constructor is being deprecated. "
                "Use CachedSchemaRegistryClient(dict: config) instead. "
                "Existing params ca_location, cert_location and key_location will be replaced with their "
                "librdkafka equivalents as keys in the conf dict: `ssl.ca.location`, `ssl.certificate.location` and "
                "`ssl.key.location` respectively",
                category=DeprecationWarning, stacklevel=2)

        # Ensure URL valid scheme is included; http[s]
        url = conf.get('url', '')
        if not isinstance(url, str):
            raise TypeError("URL must be of type str")

        if not url.startswith('http'):
            raise ValueError("Invalid URL provided for Schema Registry")

        self.url = url.rstrip('/')

        # subj => { schema => id }
        self.subject_to_schema_ids = defaultdict(dict)
        # id => avro_schema
        self.id_to_schema = defaultdict(dict)
        # subj => { schema => version }
        self.subject_to_schema_versions = defaultdict(dict)

        session = requests.Session()
        session.verify = conf.pop('ssl.ca.location', None)
        session.cert = self._configure_client_tls(conf)
        session.auth = self._configure_basic_auth(conf)
        self._session = session

        self.url = conf.pop('url')

        if len(conf) > 0:
            raise ValueError("fUnrecognized configuration properties:{conf}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self._session.close()

    @staticmethod
    def _configure_basic_auth(conf):
        url = conf['url']
        auth_provider = conf.pop('basic.auth.credentials.source', 'URL').upper()

        if auth_provider not in VALID_AUTH_PROVIDERS:
            raise ValueError(f"schema.registry.basic.auth.credentials.source must be one of {VALID_AUTH_PROVIDERS}")
        if auth_provider == 'SASL_INHERIT':
            if conf.pop('sasl.mechanism', '').upper() is ['GSSAPI']:
                raise ValueError("SASL_INHERIT does not support SASL mechanisms GSSAPI")
            auth = (conf.pop('sasl.username', ''), conf.pop('sasl.password', ''))
        elif auth_provider == 'USER_INFO':
            auth = tuple(conf.pop('basic.auth.user.info', '').split(':'))
        else:
            auth = utils.get_auth_from_url(url)
        conf['url'] = utils.urldefragauth(url)

        return auth

    @staticmethod
    def _configure_client_tls(conf):
        cert = conf.pop('ssl.certificate.location', None), conf.pop('ssl.key.location', None)
        # Both values can be None or no values can be None
        if bool(cert[0]) != bool(cert[1]):
            raise ValueError(
                "Both schema.registry.ssl.certificate.location and schema.registry.ssl.key.location must be set")

        return cert

    async def send(self, url, method='GET', body=None, headers={}):
        if method not in VALID_METHODS:
            raise ClientError(f"Method {method} is invalid; valid methods include {VALID_METHODS}")

        _headers = {'Accept': ACCEPT_HDR}
        if body:
            _headers["Content-Length"] = str(len(body))
            _headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
        _headers.update(headers)

        response = await self._session.request(
            method, url, headers=_headers, json=body)

        try:
            return response.json(), response.status_code
        except ValueError:
            return response.content, response.status_code

    @staticmethod
    def _add_to_cache(cache, subject, schema, value):
        sub_cache = cache[subject]
        sub_cache[schema] = value

    def _cache_schema(self, schema, schema_id, subject=None, version=None):
        # don't overwrite anything
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            self.id_to_schema[schema_id] = schema

        if subject:
            self._add_to_cache(self.subject_to_schema_ids,
                               subject, schema, schema_id)
            if version:
                self._add_to_cache(self.subject_to_schema_versions,
                                   subject, schema, version)

    async def register(self, subject, avro_schema):
        """
        POST /subjects/(string: subject)/versions
        Register a schema with the registry under the given subject
        and receive a schema id.
        avro_schema must be a parsed schema from the python avro library
        Multiple instances of the same schema will result in cache misses.
        :param str subject: subject name
        :param schema avro_schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        schemas_to_id = self.subject_to_schema_ids[subject]
        schema_id = schemas_to_id.get(avro_schema.name, None)

        if schema_id is not None:
            return schema_id
        # send it up
        url = '/'.join([self.url, 'subjects', subject, 'versions'])
        # body is { schema : json_string }

        body = {'schema': json.dumps(avro_schema.to_json())}

        result, code = await self.send(url, method='POST', body=body)

        if (code == 401 or code == 403):
            raise ClientError(f"Unauthorized access. Error code: {code}")
        elif code == 409:
            raise ClientError(f"Incompatible Avro schema: {code}")
        elif code == 422:
            raise ClientError("Invalid Avro schema: {code}")
        elif not (code >= 200 and code <= 299):
            raise ClientError("Unable to register schema. Error code: {code}")
        # result is a dict
        schema_id = result['id']
        self._cache_schema(avro_schema, schema_id, subject)

        return schema_id

    async def delete_subject(self, subject):
        """
        DELETE /subjects/(string: subject)
        Deletes the specified subject and its associated compatibility level if registered.
        It is recommended to use this API only when a topic needs to be recycled or in development environments.
        :param subject: subject name
        :returns: version of the schema deleted under this subject
        :rtype: (int)
        """

        url = '/'.join([self.url, 'subjects', subject])

        result, code = await self.send(url, method="DELETE")
        if not (code >= 200 and code <= 299):
            raise ClientError(f"Unable to delete subject: {result}")
        return result

    async def get_by_id(self, schema_id):
        """
        GET /schemas/ids/{int: id}
        Retrieve a parsed avro schema by id or None if not found
        :param int schema_id: int value
        :returns: Avro schema
        :rtype: schema
        """
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]
        # fetch from the registry
        url = '/'.join([self.url, 'schemas', 'ids', str(schema_id)])

        result, code = await self.send(url)
        if code == 404:
            log.error(f"Schema not found: {code}")
        elif not (code >= 200 and code <= 299):
            log.error(f"Unable to get schema for the specific ID: {code}")
        else:
            # need to parse the schema
            schema_str = result.get("schema")
            try:
                result = loads(schema_str)
                print(result, "the result")
                # cache it
                self._cache_schema(result, schema_id)
                return result
            except ClientError as e:
                # bad schema - should not happen
                raise ClientError(f"Received bad schema (id {schema_id}) from registry: {e}")

    async def get_latest_schema(self, subject):
        """
        GET /subjects/(string: subject)/versions/(versionId: version)
        Return the latest 3-tuple of:
        (the schema id, the parsed avro schema, the schema version)
        for a particular subject.
        This call always contacts the registry.
        If the subject is not found, (None,None,None) is returned.
        :param str subject: subject name
        :returns: (schema_id, schema, version)
        :rtype: (string, schema, int)
        """
        url = '/'.join([self.url, 'subjects', subject, 'versions', 'latest'])

        result, code = await self.send(url)
        if code == 404:
            log.error("Schema not found:" + str(code))
            return (None, None, None)
        elif code == 422:
            log.error("Invalid version:" + str(code))
            return (None, None, None)
        elif not (code >= 200 and code <= 299):
            return (None, None, None)
        schema_id = result['id']
        version = result['version']
        if schema_id in self.id_to_schema:
            schema = self.id_to_schema[schema_id]
        else:
            try:
                schema = loads(result['schema'])
            except ClientError:
                # bad schema - should not happen
                raise

        self._cache_schema(schema, schema_id, subject, version)
        return (schema_id, schema, version)

    async def get_version(self, subject, avro_schema):
        """
        POST /subjects/(string: subject)
        Get the version of a schema for a given subject.
        Returns None if not found.
        :param str subject: subject name
        :param: schema avro_schema: Avro schema
        :returns: version
        :rtype: int
        """
        schemas_to_version = self.subject_to_schema_versions[subject]
        version = schemas_to_version.get(avro_schema)

        if version is not None:
            return version

        url = '/'.join([self.url, 'subjects', subject])
        body = {'schema': json.dumps(avro_schema.to_json())}

        result, code = await self.send(url, method='POST', body=body)
        if code == 404:
            log.error(f"Not found: {code}")
            return None
        elif not (code >= 200 and code <= 299):
            log.error(f"Unable to get version of a schema: {code}")
            return None
        schema_id = result['id']
        version = result['version']
        self._cache_schema(avro_schema, schema_id, subject, version)
        return version

    async def test_compatibility(self, subject, avro_schema, version='latest'):
        """
        POST /compatibility/subjects/(string: subject)/versions/(versionId: version)
        Test the compatibility of a candidate parsed schema for a given subject.
        By default the latest version is checked against.
        :param: str subject: subject name
        :param: schema avro_schema: Avro schema
        :return: True if compatible, False if not compatible
        :rtype: bool
        """
        url = '/'.join([self.url, 'compatibility', 'subjects', subject,
                        'versions', str(version)])
        body = {'schema': json.dumps(avro_schema.to_json())}
        try:
            result, code = await self.send(url, method='POST', body=body)
            if code == 404:
                log.error(f"Subject or version not found: {code}")
                return False
            elif code == 422:
                log.error("Invalid subject or schema: {code}")
                return False
            elif code >= 200 and code <= 299:
                return result.get('is_compatible')
            else:
                log.error(f"Unable to check the compatibility: {code}")
                return False
        except Exception as e:
            log.error("send() failed: %s", e)
            return False

    async def update_compatibility(self, level, subject=None):
        """
        PUT /config/(string: subject)
        Update the compatibility level for a subject.  Level must be one of:
        :param str level: ex: 'NONE','FULL','FORWARD', or 'BACKWARD'
        """
        if level not in VALID_LEVELS:
            raise ClientError("Invalid level specified: %s" % (str(level)))

        url = '/'.join([self.url, 'config'])
        if subject:
            url += '/' + subject

        body = {"compatibility": level}
        result, code = await self.send(url, method='PUT', body=body)
        if code >= 200 and code <= 299:
            return result['compatibility']
        else:
            raise ClientError(f"Unable to update level: {level}. Error code: {code}")

    async def get_compatibility(self, subject=None):
        """
        GET /config
        Get the current compatibility level for a subject.  Result will be one of:
        :param str subject: subject name
        :raises ClientError: if the request was unsuccessful or an invalid compatibility level was returned
        :returns: one of 'NONE','FULL','FORWARD', or 'BACKWARD'
        :rtype: bool
        """
        url = '/'.join([self.url, 'config'])
        if subject:
            url = '/'.join([url, subject])

        result, code = await self.send(url)
        is_successful_request = code >= 200 and code <= 299
        if not is_successful_request:
            raise ClientError(f"Unable to fetch compatibility level. Error code: {code}")

        compatibility = result.get('compatibilityLevel', None)
        if compatibility not in VALID_LEVELS:
            if compatibility is None:
                error_msg_suffix = "No compatibility was returned"
            else:
                error_msg_suffix = str(compatibility)
            raise ClientError(f"Invalid compatibility level received: {error_msg_suffix}")

        return compatibility
