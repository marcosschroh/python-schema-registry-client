class ClientError(Exception):
    """ Error thrown by Schema Registry clients """

    def __init__(
        self, message: str, http_code: int = None, server_traceback: str = None
    ) -> None:
        self.message = message
        self.server_traceback = server_traceback
        self.http_code = http_code
        super(ClientError, self).__init__(self.__str__())

    def __repr__(self) -> str:
        return f"ClientError(error={self.message})"

    def __str__(self) -> str:
        return self.message
