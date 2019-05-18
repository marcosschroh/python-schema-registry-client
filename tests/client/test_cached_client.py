import pytest

from schemaregistry.client import CachedSchemaRegistryClient, load

from tests.server import mock_registry
from tests.client import data_gen


@pytest.fixture
def client():
    server = mock_registry.ServerThread(0)
    server.start()
    yield CachedSchemaRegistryClient(f"http://127.0.0.1:{server.server.server_port}")
    server.shutdown()
    server.join()


def assertLatest(self, meta_tuple, sid, schema, version):
    self.assertNotEqual(sid, -1)
    self.assertNotEqual(version, -1)
    self.assertEqual(meta_tuple[0], sid)
    self.assertEqual(meta_tuple[1], schema)
    self.assertEqual(meta_tuple[2], version)


def test_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    schema_id = client.register('test', parsed)

    assert schema_id > 0
    assert len(client.id_to_schema) == 1


def test_multi_subject_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    schema_id = client.register('test', parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = client.register('other', parsed)
    assert schema_id == dupe_id
    assert len(client.id_to_schema) == 1


def test_dupe_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    subject = 'test'
    schema_id = client.register(subject, parsed)

    assert schema_id > 0
    latest = client.get_latest_schema(subject)

    # register again under same subject
    dupe_id = client.register(subject, parsed)
    assert schema_id == dupe_id
    dupe_latest = client.get_latest_schema(subject)
    assert latest == dupe_latest


def test_getters(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    subject = 'test'
    version = client.get_version(subject, parsed)
    assert version is None

    schema = client.get_by_id(1)
    assert schema is None

    latest = client.get_latest_schema(subject)
    assert latest == (None, None, None)

    # register
    schema_id = client.register(subject, parsed)
    latest = client.get_latest_schema(subject)
    version = client.get_version(subject, parsed)
    fetched = client.get_by_id(schema_id)

    assert fetched == parsed


def test_multi_register(client):
    basic = load.loads(data_gen.BASIC_SCHEMA)
    adv = load.loads(data_gen.ADVANCED_SCHEMA)
    subject = 'test'

    id1 = client.register(subject, basic)
    latest1 = client.get_latest_schema(subject)
    client.get_version(subject, basic)

    id2 = client.register(subject, adv)
    latest2 = client.get_latest_schema(subject)
    client.get_version(subject, adv)

    assert id1 != id2
    assert latest1 != latest2
    # ensure version is higher
    assert latest1[2] < latest2[2]

    client.register(subject, basic)
    latest3 = client.get_latest_schema(subject)
    # latest should not change with a re-reg
    assert latest2 == latest3


def test_context(client):
    with client as c:
        parsed = load.loads(data_gen.BASIC_SCHEMA)
        schema_id = c.register('test', parsed)
        assert schema_id > 0
        assert len(c.id_to_schema) == 1


def test_cert_no_key():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient(
            url='https://127.0.0.1:65534',
            cert_location='/path/to/cert')


def test_cert_with_key():
    client = CachedSchemaRegistryClient(
        url='https://127.0.0.1:65534',
        cert_location='/path/to/cert',
        key_location='/path/to/key')

    assert ('/path/to/cert', '/path/to/key') == client._session.cert


def test_cert_path():
    client = CachedSchemaRegistryClient(
        url='https://127.0.0.1:65534',
        ca_location='/path/to/ca')

    assert '/path/to/ca' == client._session.verify


def test_init_with_dict():
    client = CachedSchemaRegistryClient({
        'url': 'https://127.0.0.1:65534',
        'ssl.certificate.location': '/path/to/cert',
        'ssl.key.location': '/path/to/key'
    })
    assert 'https://127.0.0.1:65534' == client.url


def test_empty_url():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({
            'url': ''
        })


def test_invalid_type_url():
    with pytest.raises(TypeError):
        CachedSchemaRegistryClient(url=1)


def test_invalid_type_url_dict():
    with pytest.raises(TypeError):
        CachedSchemaRegistryClient({"url": 1})


def test_invalid_url():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({
            'url': 'example.com:65534'
        })


def test_basic_auth_url():
    client = CachedSchemaRegistryClient({
        'url': 'https://user_url:secret_url@127.0.0.1:65534',
    })

    assert ('user_url', 'secret_url') == client._session.auth


def test_basic_auth_userinfo():
    client = CachedSchemaRegistryClient({
        'url': 'https://user_url:secret_url@127.0.0.1:65534',
        'basic.auth.credentials.source': 'user_info',
        'basic.auth.user.info': 'user_userinfo:secret_userinfo'
    })
    assert ('user_userinfo', 'secret_userinfo') == client._session.auth


def test_basic_auth_sasl_inherit():
    client = CachedSchemaRegistryClient({
        'url': 'https://user_url:secret_url@127.0.0.1:65534',
        'basic.auth.credentials.source': 'SASL_INHERIT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'user_sasl',
        'sasl.password': 'secret_sasl'
    })
    assert ('user_sasl', 'secret_sasl') == client._session.auth


def test_basic_auth_invalid():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({
            'url': 'https://user_url:secret_url@127.0.0.1:65534',
            'basic.auth.credentials.source': 'VAULT',
        })


def test_invalid_conf():
    with pytest.raises(ValueError):
        CachedSchemaRegistryClient({
            'url': 'https://user_url:secret_url@127.0.0.1:65534',
            'basic.auth.credentials.source': 'SASL_INHERIT',
            'sasl.username': 'user_sasl',
            'sasl.password': 'secret_sasl',
            'invalid.conf': 1,
            'invalid.conf2': 2
        })
