# Available urls supported with the client

paths = [
    ("register", "/subjects/{subject}/versions", "POST"),
    ("delete_subject", "/subjects/{subject}", "DELETE"),
    ("get_schema", "/subjects/{subject}/versions/{version}", "GET"),
    ("check_version", "/subjects/{subject}", "POST"),
    ("get_by_id", "/schemas/ids/{schema_id}", "GET"),
    (
        "test_compatibility",
        "/compatibility/subjects/{subject}/versions/{version}",
        "POST",
    ),
    ("update_compatibility", "/config/{subject}", "PUT"),
    ("get_compatibility", "/config/{subject}", "GET"),
]
