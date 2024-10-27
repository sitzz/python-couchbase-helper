from datetime import timedelta
import logging
from typing import Tuple

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.collection import Collection
from couchbase.diagnostics import PingState
from couchbase.exceptions import (
    BucketAlreadyExistsException,
    CollectionAlreadyExistsException,
    ScopeAlreadyExistsException,
)
from couchbase.management.collections import CollectionSpec
from couchbase.management.logic.buckets_logic import CreateBucketSettings
from couchbase.options import (
    ClusterOptions,
    ClusterTimeoutOptions,
)

from .exceptions import BucketNotSet, ScopeNotSet
from .protocols import SessionProt
from .timeout import Timeout


class Session(SessionProt):
    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        *,
        bucket: str | None = None,
        scope: str = "_default",
        collection: str = "_default",
        tls: bool = False,
        timeout: Timeout | Tuple[int, int, int] | int | None = None,
        wan: bool = False,
        logger: logging.Logger = None,
    ):
        # Initiate logger
        if logger is None:
            logger = logging.getLogger()
        self.logger = logger

        # Set default cluster values
        self._hostname = hostname
        self._username = username
        self._password = password
        self._cluster = None
        self._bucket = None
        self._bucket_name = bucket
        self._scope = None
        self._scope_name = scope
        self._collection = None
        self._collection_name = collection
        self._connected = False

        # Set the session's timeouts
        if isinstance(timeout, Timeout):
            self._timeout = timeout
        elif isinstance(timeout, tuple):
            self._timeout = Timeout(*timeout)
        elif isinstance(timeout, int):
            self._timeout = Timeout(timeout, timeout, timeout)
        elif timeout is None:
            self._timeout = Timeout()

        # Set connection options values
        self._tls = tls
        timeout_options = ClusterTimeoutOptions(
            connect_timeout=timedelta(seconds=self._timeout.connection)
        )
        self.options = ClusterOptions(
            authenticator=PasswordAuthenticator(
                username=self._username,
                password=self._password,
            ),
            enable_tls=tls,
            timeout_options=timeout_options,
            enable_tracing=True,
            show_queries=True,
        )
        if wan:
            self.options.apply_profile("wan_development")

    @property
    def connection_string(self):
        return f"couchbase{'s' if self._tls else ''}://{self._hostname}"

    def connect(self):
        self.logger.debug("- Connecting to cluster: %s", self.connection_string)
        self._cluster = Cluster.connect(
            self.connection_string,
            self.options,
        )

        if self._bucket_name is not None:
            self.bucket = self._bucket_name

        if self._scope_name is not None:
            self.scope = self._scope_name

        if self._collection_name is not None:
            self.collection = self._collection_name

        self._cluster.wait_until_ready(timedelta(seconds=self._timeout.connection))
        self._connected = True

    def disconnect(self):
        if self._cluster is not None:
            self._cluster.close()
        self._cluster = None
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def cluster(self):
        return self._cluster

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = self._cluster.bucket(value)
        self._bucket_name = value

    @property
    def bucket_name(self):
        return self._bucket_name

    def create_bucket(self, name, settings: CreateBucketSettings):
        bucket_manager = self._cluster.buckets()
        try:
            if settings.name is None:
                settings = CreateBucketSettings(name=name, **settings)

            bucket_manager.create_bucket(settings)
        except BucketAlreadyExistsException:
            pass

    @property
    def scope(self):
        return self._scope

    @scope.setter
    def scope(self, value):
        if self._bucket is None:
            raise BucketNotSet("no bucket set")

        self._scope = self._bucket.scope(value)
        self._scope_name = value

    def create_scope(self, name):
        if name == "_default":
            return

        collection_manager = self.bucket.collections()
        try:
            collection_manager.create_scope(name)
        except ScopeAlreadyExistsException:
            pass

    @property
    def collection(self) -> Collection:
        return self._collection

    @collection.setter
    def collection(self, value):
        if self._scope is None:
            raise ScopeNotSet("no scope set")

        self._collection = self._scope.collection(value)
        self._collection_name = value

    def create_collection(self, name):
        if name == "_default":
            return

        if self._bucket is None:
            raise BucketNotSet("no bucket set")

        collection_manager = self._bucket.collections()
        collection_spec = CollectionSpec(self._collection_name, scope_name=name)
        try:
            collection_manager.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass

    def default_collection(self):
        self._scope = self.bucket.default_scope()
        self._scope_name = self._scope.name
        self._collection = self.bucket.default_collection()
        self._collection_name = self._collection.name

    def ping(self):
        if self._bucket is None:
            raise BucketNotSet("no bucket set")

        result = self._bucket.ping()
        for endpoint, reports in result.endpoints.items():
            for report in reports:
                if not report.state == PingState.OK:
                    return False

        return True

    @property
    def timeout(self, attr: str | None = None):
        if attr is None:
            return self._timeout
        if hasattr(self._timeout, attr):
            return getattr(self._timeout, attr)

        return None
