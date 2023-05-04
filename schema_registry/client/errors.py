import typing


class ClientError(Exception):
    """Error thrown by Schema Registry client"""

    def __init__(
        self, message: str, http_code: typing.Optional[int] = None, server_traceback: typing.Optional[str] = None
    ) -> None:
        self.message = message
        self.server_traceback = server_traceback
        self.http_code = http_code
        super().__init__(message)

    def __repr__(self) -> str:
        return f"ClientError(error={self.message})"

    def __str__(self) -> str:
        return self.message
