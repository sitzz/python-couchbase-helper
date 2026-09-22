"""Session semantics that are easy to regress and cheap to pin down.

Like test_compat.py, this module avoids importing `fake_couchbase`, so it keeps
running on every interpreter regardless of the fake's state.
"""

from typing import Protocol, runtime_checkable

import pytest

from couchbase_helper import Session
from couchbase_helper.exceptions import BucketNotSet
from couchbase_helper.protocols import SessionProt


@runtime_checkable
class _SessionProtExt(SessionProt, Protocol):
    pass


def _session(**kwargs):
    return Session(hostname="localhost", username="u", password="p", **kwargs)


class _RecordingCollectionManager:
    def __init__(self):
        self.calls = []

    def create_collection(self, *args, **kwargs):
        self.calls.append((args, kwargs))


class _StubBucket:
    def __init__(self):
        self.manager = _RecordingCollectionManager()

    def collections(self):
        return self.manager


def _connected_session(**kwargs):
    """A session with a bucket attached, without touching a cluster."""
    session = _session(bucket="test", **kwargs)
    session._bucket = _StubBucket()
    return session


##################################################
# bucket_name                                    #
##################################################
def test_bucket_name_available_before_connect():
    """The name is known from construction, so it must not require a connection."""
    assert _session(bucket="test").bucket_name == "test"


def test_bucket_name_raises_when_unconfigured():
    with pytest.raises(BucketNotSet):
        _session().bucket_name


##################################################
# Protocol conformance                           #
##################################################
def test_session_satisfies_protocol_at_runtime():
    """Regression: on Python <= 3.11 runtime_checkable protocol checks invoke
    properties via getattr (3.12+ uses inspect.getattr_static), so a property
    that raised here propagated out of isinstance() instead of returning a bool.
    """
    assert isinstance(_session(bucket="test"), _SessionProtExt)


##################################################
# create_collection                              #
##################################################
def test_create_collection_uses_session_scope():
    """Regression: scope_name and collection_name were transposed."""
    session = _connected_session(scope="myscope")
    session.create_collection("mycoll")

    (args, kwargs) = session._bucket.manager.calls[0]
    assert (args, kwargs) == (("myscope", "mycoll"), {})


def test_create_collection_follows_session_scope_changes():
    """The scope is always the session's current one, never the caller's argument."""
    session = _connected_session(scope="myscope")
    session._scope_name = "otherscope"
    session.create_collection("mycoll")

    (args, _) = session._bucket.manager.calls[0]
    assert args == ("otherscope", "mycoll")


def test_create_collection_skips_default():
    session = _connected_session()
    session.create_collection("_default")
    assert session._bucket.manager.calls == []


def test_create_collection_requires_bucket():
    with pytest.raises(BucketNotSet):
        _session().create_collection("mycoll")
