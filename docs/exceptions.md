# ClientError

An instance of ClientError is returned when an error occurs.

```python
class ClientError(Exception):
    """ Error thrown by Schema Registry clients """

    def __init__(self, message, http_code=None, server_traceback=None):
        self.message = message
        self.server_traceback = server_traceback
        self.http_code = http_code
        super(ClientError, self).__init__(self.__str__())

    def __repr__(self):
        return f"ClientError(error={self.message})"

    def __str__(self):
        return self.message
```

For example:

```python
try:
    # Try to get the compatibility level of a subject that does not exist
    # The Schema Registry Server returns 404
    compatibility = client.get_compatibility("subject-does-not-exists")
except Exception as error:
    # Print 404
    print(error.http_code)
```