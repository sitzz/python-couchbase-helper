import logging
from typing import Protocol

from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.collection import Collection
from couchbase.scope import Scope

from .timeout import Timeout


class SessionProt(Protocol):
    logger: logging.Logger

    def connect(self): ...

    def disconnect(self): ...

    @property
    def connected(self): ...

    @property
    def cluster(self) -> Cluster: ...

    @property
    def bucket(self) -> Bucket: ...

    @property
    def bucket_name(self) -> str: ...

    @property
    def collection(self) -> Collection: ...

    @property
    def scope(self) -> Scope: ...

    def ping(self) -> bool: ...

    @property
    def timeout(self) -> Timeout: ...
