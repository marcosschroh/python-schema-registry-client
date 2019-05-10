class ClientError(Exception):
    """ Error thrown by Schema Registry clients """

    def __init__(self, message, http_code=None):
        self.message = message
        self.http_code = http_code
        super(ClientError, self).__init__(self.__str__())

    def __repr__(self):
        return "ClientError(error={error})".format(error=self.message)

    def __str__(self):
        return self.message
