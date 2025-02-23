from datetime import timedelta
import logging
from typing import Optional, Tuple, Union

from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
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
from couchbase.scope import Scope

from .exceptions import BucketNotSet, ClusterNotSet, ScopeNotSet
from .protocols import SessionProt
from .timeout import Timeout


class Session(SessionProt):
    """Create a Couchbase cluster session

    The session instance exposes methods for connecting, choosing bucket, scope, and collection for Couchbase operations

    Args:
        hostname (str):
            The hostname (or IP address) to connect to. Either should be without protocol,
            e.g. "localhost" or "127.0.0.1".
        username (str):
            The username to be used for authentication.
        password (str):
            The password to be used for authentication.

    Optional args:
        bucket (str):
            The bucket name to connect to.
        scope (str):
            The scope name to connect to. Defaults to "_default".
        collection (str):
            The collection to connect to. Defaults to `"_default".
        tls (bool):
            Whether connection should use TLS. Defaults to `false`
        timeout (:class:`~couchbase_helper.timeout.Timeout`):
            The timeout settings for this session instance. Defaults to `Timeout()`
        wan (bool):
            Whether connection profile "wan_development" should be applied.
        logger (:class:`logging.Logger`):
            Any logging instance to be used. Defaults to root logger.
    """

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        *,
        bucket: Optional[str] = None,
        scope: str = "_default",
        collection: str = "_default",
        tls: bool = False,
        timeout: Optional[Union[Timeout, Tuple[int, int, int], int]] = None,
        wan: bool = False,
        logger: Optional[logging.Logger] = None,
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
    def connection_string(self) -> str:
        """Generates the cluster's connection string

        Returns:
            str: A combination of the protocol and hostname for the connection
        """
        return f"couchbase{'s' if self._tls else ''}://{self._hostname}"

    def connect(self):
        """Establish a connection to the cluster using the set bucket, scope, and collection."""
        self.logger.debug("- Connecting to cluster: %s", self.connection_string)

        if self._cluster is None:
            self._cluster = Cluster(self.connection_string, self.options)

        if self._bucket_name is not None:
            self.bucket = self._bucket_name

        if self._scope_name is not None:
            self.scope = self._scope_name

        if self._collection_name is not None:
            self.collection = self._collection_name

        self._cluster.wait_until_ready(timedelta(seconds=self._timeout.connection))
        self._connected = True

    def disconnect(self):
        """Shuts down the cluster instance and unsets class internal variables.

        According to Couchbase documentation...: "Use of this method is almost *always* unnecessary."
        It's available nonetheless.
        """
        if self._cluster is not None:
            self._cluster.close()
        self._cluster = None
        self._connected = False

    @property
    def connected(self) -> bool:
        """
        Returns:
            bool: Whether connected to cluster or not.
        """
        if self._cluster is None:
            return False

        return self._connected and self._cluster.connected

    @property
    def cluster(self) -> Cluster:
        """Returns the cluster instance"""
        return self._cluster

    @cluster.setter
    def cluster(self, value):
        connect = False
        if self._connected:
            connect = True
            self.disconnect()

        self._cluster = value

        if connect:
            self.connect()

    @property
    def bucket(self) -> Bucket:
        """Returns the bucket instance"""
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        """Set the bucket instance"""
        self._bucket = self._cluster.bucket(value)
        self._bucket_name = value

    @property
    def bucket_name(self) -> str:
        """Returns the bucket name"""
        if self._bucket is None:
            raise BucketNotSet("no bucket set")

        return self._bucket_name

    def create_bucket(self, name, settings: CreateBucketSettings):
        """Create a bucket

        Args:
            name (str):
                The name of the bucket to create.
            settings (:class:`couchbase.management.logic.buckets_logic.BucketSettings`):
                The settings of the bucket to create.
        """
        if self._cluster is None:
            raise ClusterNotSet("no cluster set")

        bucket_manager = self._cluster.buckets()
        try:
            if settings.name is None:
                settings = CreateBucketSettings(name=name, **settings)

            bucket_manager.create_bucket(settings)
        except BucketAlreadyExistsException:
            pass

    @property
    def scope(self) -> Scope:
        """Returns the scope instance"""
        return self._scope

    @scope.setter
    def scope(self, value):
        """Set the scope instance"""
        if self._bucket is None:
            raise BucketNotSet("no bucket set")

        self._scope = self._bucket.scope(value)
        self._scope_name = value

    def create_scope(self, name):
        """Create a scope

        Args:
            name (str):
                The name of the scope to create.
        """
        if name == "_default":
            return

        collection_manager = self.bucket.collections()
        try:
            collection_manager.create_scope(name)
        except ScopeAlreadyExistsException:
            pass

    @property
    def collection(self) -> Collection:
        """Returns the collection instance"""
        return self._collection

    @collection.setter
    def collection(self, value):
        """Set the collection instance"""
        if self._scope is None:
            raise ScopeNotSet("no scope set")

        self._collection = self._scope.collection(value)
        self._collection_name = value

    def create_collection(self, name):
        """Create a collection

        Args:
            name (str):
                The name of the collection to create
        """
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
        """Use the default scope and collection"""
        self._scope = self.bucket.default_scope()
        self._scope_name = self._scope.name
        self._collection = self.bucket.default_collection()
        self._collection_name = self._collection.name

    def ping(self):
        """Ping the bucket to check for connection"""
        if self._bucket is None:
            raise BucketNotSet("no bucket set")

        result = self._bucket.ping()
        for endpoint, reports in result.endpoints.items():
            for report in reports:
                if not report.state == PingState.OK:
                    return False

        return True

    @property
    def timeout(self) -> Timeout:
        """Returns the timeout configuration

        Returns:
            Timeout:
                Will return the entire :class:`~couchbase_helper.timeout.Timeout` instance.
        """
        return self._timeout
