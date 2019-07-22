def test_version_does_not_exists(client, country_schema):
    assert client.check_version("test-schema-version", country_schema) is None


def test_get_versions(client, country_schema):
    subject = "test-schema-version"
    client.register(subject, country_schema)
    versions = client.get_versions(subject)

    assert versions


def test_get_versions_does_not_exist(client):
    assert not client.get_versions("random-subject")


def test_check_version(client, country_schema):
    subject = "test-schema-version"
    schema_id = client.register(subject, country_schema)
    result = client.check_version(subject, country_schema)

    assert subject == result.subject
    assert schema_id == result.schema_id


def test_delete_version(client, country_schema):
    subject = "test-schema-version"
    client.register(subject, country_schema)
    versions = client.get_versions(subject)
    latest_version = versions[-1]

    assert latest_version == client.delete_version(subject, latest_version)


def test_delete_version_does_not_exist(client, country_schema):
    subject = "test-schema-version"
    client.register(subject, country_schema)

    assert not client.delete_version("random-subject")
    assert not client.delete_version(subject, "random-version")
