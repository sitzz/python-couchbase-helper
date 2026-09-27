from datetime import timedelta

import pytest

from couchbase_helper import Timeout
from couchbase_helper.options import _TYPES, build_opts


class _TimeoutSession:
    def __init__(self, timeout):
        self.timeout = timeout


@pytest.mark.parametrize("type_", sorted(_TYPES))
def test_builds_matching_options_class(type_):
    assert isinstance(build_opts(type_), _TYPES[type_])


def test_unknown_type_raises():
    with pytest.raises(AttributeError, match="unknown options type"):
        build_opts("nope")


@pytest.mark.parametrize(
    ("type_", "seconds"),
    [("get", 5), ("insert_multi", 5), ("query", 30), ("view", 30)],
)
def test_default_timeout(type_, seconds):
    assert build_opts(type_)["timeout"] == timedelta(seconds=seconds)


def test_explicit_timeout_is_kept():
    opts = build_opts("get", opts={"timeout": timedelta(seconds=99)})
    assert opts["timeout"] == timedelta(seconds=99)


def test_session_timeout_overrides_default():
    session = _TimeoutSession(Timeout(1, 2, 3))
    assert build_opts("get", session=session)["timeout"] == timedelta(seconds=2)
    assert build_opts("query", session=session)["timeout"] == timedelta(seconds=3)


def test_expiry_int_is_converted():
    assert build_opts("insert", expiry=60)["expiry"] == timedelta(seconds=60)


def test_expiry_timedelta_passes_through():
    expiry = timedelta(minutes=5)
    assert build_opts("upsert", expiry=expiry)["expiry"] == expiry
