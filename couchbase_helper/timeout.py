class Timeout:
    """A simple container for timeout values of different operations.

    Has some sane defaults.

    Args:
        connection (int):
            The timeout of connection operations.
        kv (int):
            The timeout used for key-value operations.
        query (int):
            The timeout used for query operations.
    """

    def __init__(self, connection: int = 10, kv: int = 5, query: int = 30):
        self._connection = connection
        self._kv = kv
        self._query = query

    @classmethod
    def init(cls, connection: int = 10, kv: int = 5, query: int = 30):
        """Statically initiate class instance"""
        return cls(connection, kv, query)

    @property
    def connection(self) -> int:
        """Returns the connection timeout value"""
        return self._connection

    @property
    def kv(self) -> int:
        """Returns the key-value timeout value"""
        return self._kv

    @property
    def query(self) -> int:
        """Returns the query timeout value"""
        return self._query
