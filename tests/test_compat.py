from pathlib import Path
import re
from unittest import mock

import pytest

from couchbase_helper import _compat

_PACKAGE_DIR = Path(__file__).resolve().parent.parent / "couchbase_helper"
_PRIVATE_IMPORT = re.compile(
    r"^\s*(?:from|import)\s+couchbase\.(?:\w+\.)*logic\b", re.MULTILINE
)


@pytest.mark.parametrize(
    "source_file", sorted(_PACKAGE_DIR.glob("*.py")), ids=lambda p: p.name
)
def test_no_private_sdk_imports(source_file):
    """The package must only import from public `couchbase.*` modules."""
    offenders = _PRIVATE_IMPORT.findall(source_file.read_text(encoding="utf-8"))
    assert not offenders, (
        f"{source_file.name} imports a private couchbase module: {offenders}. "
        "Use the documented public path instead."
    )


def test_public_sdk_imports_resolve():
    """Every SDK symbol the package relies on is importable from its public path."""
    from couchbase.auth import PasswordAuthenticator  # noqa: F401
    from couchbase.bucket import Bucket  # noqa: F401
    from couchbase.cluster import Cluster  # noqa: F401
    from couchbase.collection import Collection  # noqa: F401
    from couchbase.diagnostics import PingState, ServiceType  # noqa: F401
    from couchbase.management.buckets import CreateBucketSettings  # noqa: F401
    from couchbase.n1ql import N1QLQuery, QueryScanConsistency  # noqa: F401
    from couchbase.result import GetResult, MultiGetResult, QueryResult  # noqa: F401
    from couchbase.scope import Scope  # noqa: F401


def test_sdk_version_detected():
    """We can always tell which SDK we are running against."""
    assert _compat.SDK_VERSION, "could not determine the installed couchbase version"
    assert _compat.SDK_VERSION[0] >= 4


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("4.6.3", (4, 6, 3)),
        ("4.1.9", (4, 1, 9)),
        ("4.6.0rc2", (4, 6, 0)),
        ("4.2", (4, 2)),
        ("not-a-version", ()),
    ],
)
def test_sdk_version_parsing(raw, expected):
    with mock.patch("importlib.metadata.version", return_value=raw):
        assert _compat._sdk_version() == expected


class _RecordingManager:
    def __init__(self):
        self.calls = []

    def create_collection(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def test_create_collection_named_api(monkeypatch):
    """SDK >= 4.1.9 gets the (scope_name, collection_name) signature."""
    monkeypatch.setattr(_compat, "_HAS_NAMED_CREATE_COLLECTION", True)
    manager = _RecordingManager()
    _compat.create_collection(manager, "myscope", "mycoll")
    assert manager.calls == [(("myscope", "mycoll"), {})]


def test_create_collection_legacy_api(monkeypatch):
    """SDK < 4.1.9 falls back to the CollectionSpec overload, same semantics."""
    monkeypatch.setattr(_compat, "_HAS_NAMED_CREATE_COLLECTION", False)
    manager = _RecordingManager()
    _compat.create_collection(manager, "myscope", "mycoll")

    (spec,), _ = manager.calls[0]
    assert spec.name == "mycoll"
    assert spec.scope_name == "myscope"
