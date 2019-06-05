from schema_registry.client import load

from tests import data_gen


def test_delete_subject(client, user_schema_v3):
    subject = "subject-to-delete"
    versions = [load.loads(data_gen.USER_V1), load.loads(data_gen.USER_V2)]

    for version in versions:
        client.register(subject, version)

    assert len(client.delete_subject(subject)) == len(versions)
