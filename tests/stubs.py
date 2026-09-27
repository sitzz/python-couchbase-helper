"""Minimal stand-ins for the Couchbase SDK objects the package talks to.

These let tests drive every branch (failures, partial results, exceptions)
without a cluster, on every interpreter and SDK in the matrix.

Any response configured as an exception instance is raised instead of returned.
"""

import inspect
import logging
from types import SimpleNamespace

from couchbase.collection import Collection
from couchbase.diagnostics import PingState
from couchbase.options import forward_args

from couchbase_helper import Timeout


def _respond(response):
    if isinstance(response, BaseException):
        raise response
    return response


class RecordingCollectionManager:
    def __init__(self, raises=None):
        self.calls = []
        self.raises = raises

    def create_collection(self, *args, **kwargs):
        self.calls.append(("create_collection", args, kwargs))
        _respond(self.raises)

    def create_scope(self, *args, **kwargs):
        self.calls.append(("create_scope", args, kwargs))
        _respond(self.raises)


class RecordingBucketManager:
    def __init__(self, raises=None):
        self.calls = []
        self.raises = raises

    def create_bucket(self, settings):
        self.calls.append(settings)
        _respond(self.raises)


def sdk_options(op, args, kwargs):
    """The options the real SDK would apply for a recorded Collection call.

    Binds the call to the real method signature (so a missing or misnamed
    required argument fails here) and then merges options exactly as the SDK's
    request builders do. Every Collection method takes `*opts, **kwargs`, so
    this is the only way to tell whether options would actually take effect.
    """
    bound = inspect.signature(getattr(Collection, op)).bind(None, *args, **kwargs)
    return forward_args(
        bound.arguments.get("kwargs", {}), *bound.arguments.get("opts", ())
    )


class StubCollection:
    """Records every operation; `responses` maps operation name to its result."""

    def __init__(self, name="_default", **responses):
        self.name = name
        self.responses = responses
        self.calls = []

    def __getattr__(self, op):
        if op.startswith("_"):
            raise AttributeError(op)

        def call(*args, **kwargs):
            self.calls.append((op, args, kwargs))
            return _respond(self.responses.get(op))

        return call


class StubScope:
    def __init__(self, name="_default"):
        self.name = name

    def collection(self, name):
        return StubCollection(name)


class StubBucket:
    def __init__(self, name="test", *, ping_states=(PingState.OK,), view=None):
        self.name = name
        self.manager = RecordingCollectionManager()
        self.ping_states = ping_states
        self.view = view
        self.view_calls = []

    def collections(self):
        return self.manager

    def scope(self, name):
        return StubScope(name)

    def default_scope(self):
        return StubScope("_default")

    def default_collection(self):
        return StubCollection("_default")

    def ping(self):
        reports = [SimpleNamespace(state=state) for state in self.ping_states]
        return SimpleNamespace(endpoints={"kv": reports})

    def view_query(self, **kwargs):
        self.view_calls.append(kwargs)
        return _respond(self.view)


class StubCluster:
    def __init__(self, *args, query_result=None, bucket_manager=None):
        self.init_args = args
        self.connected = True
        self.closed = False
        self.query_result = query_result
        self.queries = []
        self.bucket_manager = bucket_manager or RecordingBucketManager()

    def bucket(self, name):
        return StubBucket(name)

    def buckets(self):
        return self.bucket_manager

    def wait_until_ready(self, *args, **kwargs):
        pass

    def close(self):
        self.closed = True

    def query(self, statement, **kwargs):
        self.queries.append((statement, kwargs))
        return _respond(self.query_result)


class StubMultiResult:
    def __init__(self, all_ok=True, exceptions=None, results=None):
        self.all_ok = all_ok
        self.exceptions = exceptions or {}
        self.results = results or {}


class StubDocument:
    def __init__(self, content):
        self.content_as = {dict: content}


class StubSession:
    """Satisfies what CouchbaseHelper and N1ql need from a session.

    Like Session, the bucket/scope/collection setters take a name; stub objects
    are accepted too, so tests can inject configured ones.
    """

    def __init__(self, *, connected=True, logger=None, collection=None, bucket=None):
        self.logger = logger
        self._connected = connected
        self.connect_calls = 0
        self.timeout = Timeout()
        self.cluster = StubCluster()
        self.bucket = bucket or StubBucket()
        self.scope = StubScope()
        self.collection = collection or StubCollection()

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = StubBucket(value) if isinstance(value, str) else value

    @property
    def scope(self):
        return self._scope

    @scope.setter
    def scope(self, value):
        self._scope = StubScope(value) if isinstance(value, str) else value

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = StubCollection(value) if isinstance(value, str) else value

    @property
    def connected(self):
        return self._connected

    def connect(self):
        self.connect_calls += 1
        self._connected = True


def named_logger(name):
    return logging.getLogger(f"couchbase_helper.tests.{name}")
