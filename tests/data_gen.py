import datetime
import os
import os.path

import faker

fake = faker.Faker()
epoch = datetime.datetime.utcfromtimestamp(0)

AVRO_SCHEMAS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "avro_schemas")
JSON_SCHEMAS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "json_schemas")


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def get_schema_path(fname):
    ext = os.path.splitext(fname)[1]
    if ext == ".avsc":
        return os.path.join(AVRO_SCHEMAS_DIR, fname)

    if ext == ".json":
        return os.path.join(JSON_SCHEMAS_DIR, fname)

    raise ValueError(
        f"File format '{ext}' not supported. Schemas files must have extensions of either avro (.avsc) or json (.json)."
    )


def load_schema_file(fname):
    fname = get_schema_path(fname)
    with open(fname) as f:
        return f.read()


def create_basic_item(i):
    return {"name": fake.first_name(), "number": fake.pyint(max_value=100)}


def create_adv_item(i):
    friends = map(create_basic_item, range(1, 3))
    family = map(create_basic_item, range(1, 3))
    basic = create_basic_item(i)
    basic["family"] = dict(map(lambda bi: (bi["name"], bi), family))
    basic["friends"] = dict(map(lambda bi: (bi["name"], bi), friends))

    return basic


def create_logical_item():
    return {
        "metadata": {
            "timestamp": fake.past_datetime(tzinfo=datetime.timezone.utc),
            "total": fake.pydecimal(left_digits=2, right_digits=2),
        }
    }


def create_nested_schema():
    return {
        "name": fake.first_name(),
        "uid": fake.pyint(min_value=0, max_value=9999, step=1),
        "order": {"uid": fake.pyint(min_value=0, max_value=9999, step=1)},
    }


AVRO_BASIC_SCHEMA = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "basic_schema.avsc"))
AVRO_ADVANCED_SCHEMA = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "adv_schema.avsc"))
AVRO_BASIC_ITEMS = map(create_basic_item, range(1, 20))
AVRO_USER_V1 = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "user_v1.avsc"))
AVRO_USER_V2 = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "user_v2.avsc"))
AVRO_LOGICAL_TYPES_SCHEMA = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "logical_types_schema.avsc"))
AVRO_ADVANCED_ITEMS = map(create_adv_item, range(1, 20))
AVRO_NESTED_SCHENA = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "nested_schema.avsc"))
AVRO_ORDER_SCHENA = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "order_schema.avsc"))

JSON_BASIC_SCHEMA = load_schema_file(os.path.join(JSON_SCHEMAS_DIR, "basic_schema.json"))
JSON_ADVANCED_SCHEMA = load_schema_file(os.path.join(JSON_SCHEMAS_DIR, "adv_schema.json"))
JSON_BASIC_ITEMS = map(create_basic_item, range(1, 20))
JSON_USER_V1 = load_schema_file(os.path.join(JSON_SCHEMAS_DIR, "user_v1.json"))
JSON_USER_V2 = load_schema_file(os.path.join(JSON_SCHEMAS_DIR, "user_v2.json"))
JSON_ADVANCED_ITEMS = map(create_adv_item, range(1, 20))
JSON_NESTED_SCHEMA = load_schema_file(os.path.join(JSON_SCHEMAS_DIR, "nested_schema.json"))
JSON_ORDER_SCHEMA = load_schema_file(os.path.join(JSON_SCHEMAS_DIR, "order_schema.json"))


def cleanup(files):
    for f in files:
        try:
            os.remove(f)
        except OSError:
            pass
