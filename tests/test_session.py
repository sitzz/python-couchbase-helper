"""Session semantics that are easy to regress and cheap to pin down.

Like test_compat.py, this module avoids importing `fake_couchbase`, so it keeps
running on every interpreter regardless of the fake's state.
"""

from typing import Protocol, runtime_checkable

import pytest

from couchbase_helper import Session
from couchbase.diagnostics import PingState
from couchbase.exceptions import (
    BucketAlreadyExistsException,
    CollectionAlreadyExistsException,
    ScopeAlreadyExistsException,
)
from couchbase.management.buckets import CreateBucketSettings

from couchbase_helper import Timeout
from couchbase_helper import session as session_module
from couchbase_helper.exceptions import BucketNotSet, ClusterNotSet, ScopeNotSet
from couchbase_helper.protocols import SessionProt

from .stubs import (
    RecordingBucketManager,
    RecordingCollectionManager,
    StubBucket,
    StubCluster,
)


@runtime_checkable
class _SessionProtExt(SessionProt, Protocol):
    pass


def _session(**kwargs):
    return Session(hostname="localhost", username="u", password="p", **kwargs)


def _connected_session(**kwargs):
    """A session with a bucket attached, without touching a cluster."""
    session = _session(bucket="test", **kwargs)
    session._bucket = StubBucket()
    return session


def test_bucket_name_available_before_connect():
    """The name is known from construction, so it must not require a connection."""
    assert _session(bucket="test").bucket_name == "test"


def test_bucket_name_raises_when_unconfigured():
    with pytest.raises(BucketNotSet):
        _session().bucket_name


def test_session_satisfies_protocol_at_runtime():
    """Regression: on Python <= 3.11 runtime_checkable protocol checks invoke
    properties via getattr (3.12+ uses inspect.getattr_static), so a property
    that raised here propagated out of isinstance() instead of returning a bool.
    """
    assert isinstance(_session(bucket="test"), _SessionProtExt)


def test_create_collection_uses_session_scope():
    """Regression: scope_name and collection_name were transposed."""
    session = _connected_session(scope="myscope")
    session.create_collection("mycoll")

    assert session._bucket.manager.calls == [
        ("create_collection", ("myscope", "mycoll"), {})
    ]


def test_create_collection_follows_session_scope_changes():
    """The scope is always the session's current one, never the caller's argument."""
    session = _connected_session(scope="myscope")
    session._scope_name = "otherscope"
    session.create_collection("mycoll")

    (_, args, _) = session._bucket.manager.calls[0]
    assert args == ("otherscope", "mycoll")


def test_create_collection_skips_default():
    session = _connected_session()
    session.create_collection("_default")
    assert session._bucket.manager.calls == []


def test_create_collection_requires_bucket():
    with pytest.raises(BucketNotSet):
        _session().create_collection("mycoll")


def test_create_collection_ignores_existing():
    session = _connected_session()
    session._bucket.manager = RecordingCollectionManager(
        raises=CollectionAlreadyExistsException()
    )
    session.create_collection("mycoll")
    assert len(session._bucket.manager.calls) == 1


@pytest.mark.parametrize(
    ("timeout", "expected"),
    [
        (None, (10, 5, 30)),
        (Timeout(1, 2, 3), (1, 2, 3)),
        ((4, 5, 6), (4, 5, 6)),
        (7, (7, 7, 7)),
    ],
)
def test_timeout_variants(timeout, expected):
    session = _session(timeout=timeout)
    assert (
        session.timeout.connection,
        session.timeout.kv,
        session.timeout.query,
    ) == expected


def test_wan_applies_profile(monkeypatch):
    profiles = []
    monkeypatch.setattr(
        session_module.ClusterOptions,
        "apply_profile",
        lambda self, name: profiles.append(name),
    )
    _session(wan=True)
    assert profiles == ["wan_development"]


@pytest.mark.parametrize(
    ("tls", "expected"),
    [(False, "couchbase://localhost"), (True, "couchbases://localhost")],
)
def test_connection_string(tls, expected):
    assert _session(tls=tls).connection_string == expected


@pytest.fixture
def stub_cluster_class(monkeypatch):
    monkeypatch.setattr(session_module, "Cluster", StubCluster)


def test_connect_creates_cluster(stub_cluster_class):
    session = _session(bucket="test", scope="s", collection="c")
    assert not session.connected

    session.connect()

    assert isinstance(session.cluster, StubCluster)
    assert session.cluster.init_args == ("couchbase://localhost", session.options)
    assert session.connected
    assert session.bucket.name == "test"
    assert session.scope.name == "s"
    assert session.collection.name == "c"


def test_disconnect(stub_cluster_class):
    session = _session(bucket="test")
    session.connect()
    cluster = session.cluster

    session.disconnect()

    assert cluster.closed
    assert session.cluster is None
    assert not session.connected


def test_disconnect_without_cluster():
    session = _session()
    session.disconnect()
    assert not session.connected


def test_cluster_setter_reconnects_when_connected(stub_cluster_class):
    session = _session(bucket="test")
    session.connect()
    old = session.cluster
    new = StubCluster()

    session.cluster = new

    assert old.closed
    assert session.cluster is new
    assert session.connected


def test_cluster_setter_does_not_connect_when_disconnected():
    session = _session()
    session.cluster = StubCluster()
    assert not session.connected


def test_create_bucket_requires_cluster():
    with pytest.raises(ClusterNotSet):
        _session().create_bucket("b", CreateBucketSettings(name="b"))


def test_create_bucket():
    session = _session()
    session._cluster = StubCluster()
    settings = CreateBucketSettings(name="b", ram_quota_mb=100)

    session.create_bucket("b", settings)

    assert session.cluster.bucket_manager.calls == [settings]


def test_create_bucket_fills_in_missing_name():
    class _Unnamed(dict):
        name = None

    session = _session()
    session._cluster = StubCluster()

    session.create_bucket("b", _Unnamed(ram_quota_mb=100))

    (settings,) = session.cluster.bucket_manager.calls
    assert settings.name == "b"


def test_create_bucket_ignores_existing():
    session = _session()
    session._cluster = StubCluster(
        bucket_manager=RecordingBucketManager(raises=BucketAlreadyExistsException())
    )
    session.create_bucket("b", CreateBucketSettings(name="b"))
    assert len(session.cluster.bucket_manager.calls) == 1


def test_scope_setter_requires_bucket():
    with pytest.raises(BucketNotSet):
        _session().scope = "s"


def test_collection_setter_requires_scope():
    with pytest.raises(ScopeNotSet):
        _session().collection = "c"


def test_create_scope():
    session = _connected_session()
    session.create_scope("s")
    assert session._bucket.manager.calls == [("create_scope", ("s",), {})]


def test_create_scope_skips_default():
    session = _connected_session()
    session.create_scope("_default")
    assert session._bucket.manager.calls == []


def test_create_scope_ignores_existing():
    session = _connected_session()
    session._bucket.manager = RecordingCollectionManager(
        raises=ScopeAlreadyExistsException()
    )
    session.create_scope("s")
    assert len(session._bucket.manager.calls) == 1


def test_default_collection():
    session = _connected_session(scope="s", collection="c")
    session.default_collection()
    assert session.scope.name == "_default"
    assert session.collection.name == "_default"


def test_ping_requires_bucket():
    with pytest.raises(BucketNotSet):
        _session().ping()


def test_ping_ok():
    session = _connected_session()
    session._bucket = StubBucket(ping_states=(PingState.OK, PingState.OK))
    assert session.ping()


def test_ping_reports_unhealthy_endpoint():
    session = _connected_session()
    session._bucket = StubBucket(ping_states=(PingState.OK, PingState.TIMEOUT))
    assert not session.ping()
