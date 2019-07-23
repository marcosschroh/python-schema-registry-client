class SerializerError(Exception):
    """Generic error from serializer package"""

    def __init__(self, message: str) -> None:
        self.message = message

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(error={self.message})"

        def __str__(self) -> str:
            return self.message


class KeySerializerError(SerializerError):
    pass


class ValueSerializerError(SerializerError):
    pass
