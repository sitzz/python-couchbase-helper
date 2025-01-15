from typing import Protocol

from couchbase.bucket import Collection, Scope
from couchbase.cluster import Bucket, Cluster

from .timeout import Timeout


class SessionProt(Protocol):
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
