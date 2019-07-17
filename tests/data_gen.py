import os
import os.path
import faker
import datetime

fake = faker.Faker()
epoch = datetime.datetime.utcfromtimestamp(0)

AVRO_SCHEMAS_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "avro_schemas"
)


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def get_schema_path(fname):
    return os.path.join(AVRO_SCHEMAS_DIR, fname)


def load_schema_file(fname):
    fname = get_schema_path(fname)
    with open(fname) as f:
        return f.read()


def create_basic_item(i):
    return {"name": fake.first_name(), "number": fake.pyint(max=100)}


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
            "timestamp": unix_time_millis(fake.past_datetime()),
            "total": fake.pydecimal(left_digits=2, right_digits=2),
        }
    }


BASIC_SCHEMA = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "basic_schema.avsc"))
ADVANCED_SCHEMA = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "adv_schema.avsc"))
BASIC_ITEMS = map(create_basic_item, range(1, 20))
USER_V1 = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "user_v1.avsc"))
USER_V2 = load_schema_file(os.path.join(AVRO_SCHEMAS_DIR, "user_v2.avsc"))
LOGICAL_TYPES_SCHEMA = load_schema_file(
    os.path.join(AVRO_SCHEMAS_DIR, "logical_types_schema.avsc")
)
ADVANCED_ITEMS = map(create_adv_item, range(1, 20))


def cleanup(files):
    for f in files:
        try:
            os.remove(f)
        except OSError:
            pass
