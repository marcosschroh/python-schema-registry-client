class SerializerError(Exception):
    """Generic error from serializer package"""

    def __init__(self, message):
        self.message = message

        def __repr__(self):
            return f"{self.__class__.__name__}(error={self.message})"

        def __str__(self):
            return self.message


class KeySerializerError(SerializerError):
    pass


class ValueSerializerError(SerializerError):
    pass
