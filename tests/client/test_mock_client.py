import pytest

from schema_registry.client import load

from tests.client import data_gen
from tests.client.mock_schema_registry_client import MockSchemaRegistryClient


@pytest.fixture
def client():
    return MockSchemaRegistryClient()


def test_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    schema_id = client.register("test", parsed)
    assert schema_id > 0
    assert len(client.id_to_schema) == 1


def test_multi_subject_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    schema_id = client.register("test", parsed)
    assert schema_id > 0

    # register again under different subject
    dupe_id = client.register("other", parsed)
    assert schema_id == dupe_id
    assert len(client.id_to_schema) == 1


def test_dupe_register(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    subject = "test"
    schema_id = client.register(subject, parsed)
    assert schema_id > 0
    latest = client.get_latest_schema(subject)

    # register again under same subject
    dupe_id = client.register(subject, parsed)
    assert schema_id == dupe_id
    dupe_latest = client.get_latest_schema(subject)
    assert latest == dupe_latest


def assertLatest(meta_tuple, sid, schema, version):
    assert sid != -1
    assert version != -1
    assert meta_tuple[0] == sid
    assert meta_tuple[1] == schema
    assert meta_tuple[2] == version


def test_getters(client):
    parsed = load.loads(data_gen.BASIC_SCHEMA)
    subject = "test"
    version = client.get_version(subject, parsed)
    assert version == -1

    schema = client.get_by_id(1)
    assert schema is None

    latest = client.get_latest_schema(subject)
    assert latest == (None, None, None)

    # register
    schema_id = client.register(subject, parsed)
    latest = client.get_latest_schema(subject)
    version = client.get_version(subject, parsed)
    assertLatest(latest, schema_id, parsed, version)

    fetched = client.get_by_id(schema_id)
    assert fetched == parsed


def test_multi_register(client):
    basic = load.loads(data_gen.BASIC_SCHEMA)
    adv = load.loads(data_gen.ADVANCED_SCHEMA)
    subject = "test"

    id1 = client.register(subject, basic)
    latest1 = client.get_latest_schema(subject)
    v1 = client.get_version(subject, basic)
    assertLatest(latest1, id1, basic, v1)

    id2 = client.register(subject, adv)
    latest2 = client.get_latest_schema(subject)
    v2 = client.get_version(subject, adv)
    assertLatest(latest2, id2, adv, v2)

    assert id1 != id2
    assert latest1 != latest2

    # ensure version is higher
    assert latest1[2] < latest2[2]

    client.register(subject, basic)
    latest3 = client.get_latest_schema(subject)
    # latest should not change with a re-reg
    assert latest2 == latest3
