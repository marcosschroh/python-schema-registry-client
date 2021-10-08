from schema_registry.serializers.message_serializer import MessageSerializer  # noqa
from schema_registry.serializers.message_serializer import AvroMessageSerializer  # noqa
from schema_registry.serializers.message_serializer import JsonMessageSerializer  # noqa

try:
    import faust  # noqa
    from schema_registry.serializers.faust_serializer import FaustSerializer  # noqa
    from schema_registry.serializers.faust_serializer import FaustJsonSerializer  # noqa
except ImportError:
    # Faust is not installed but the other serializers will work fine
    pass
