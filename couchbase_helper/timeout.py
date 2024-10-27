class Timeout:
    def __init__(self, connection: int = 10, kv: int = 5, query: int = 30):
        self._connection = connection
        self._kv = kv
        self._query = query

    @property
    def connection(self) -> int:
        return self._connection

    @property
    def kv(self) -> int:
        return self._kv

    @property
    def query(self) -> int:
        return self._query
