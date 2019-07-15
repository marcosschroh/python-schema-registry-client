def test_version_does_not_exists(client, country_schema):
    assert client.check_version("test-schema-version", country_schema) is None


def test_version(client, country_schema):
    subject = "test-schema-version"
    schema_id = client.register(subject, country_schema)
    result = client.check_version(subject, country_schema)

    assert subject == result.subject
    assert schema_id == result.schema_id
