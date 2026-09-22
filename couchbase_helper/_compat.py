"""Compatibility helpers for the range of Couchbase SDK versions we support.

Everything in this module exists because a public API of the `couchbase` SDK
changed shape at some point in the 4.x series. Keeping it all here means there
is exactly one file to look at when the SDK shifts again.

Rules of thumb for this package:
    * Only ever import from documented, public `couchbase.*` modules. Private
      paths (`couchbase.logic.*`, `couchbase.management.logic.*`) get renamed
      without notice -- `couchbase.management.logic.buckets_logic` became
      `couchbase.management.logic.bucket_mgmt_types` in 4.6.0, for instance.
    * When a call signature changes, branch on `SDK_VERSION` here rather than
      at the call site.

Full disclosure:
    * This module was written with a lot of help from Claude but has been
      checked for sanity.
"""

from typing import Any, Tuple

__all__ = ["SDK_VERSION", "create_collection"]


def _sdk_version() -> Tuple[int, ...]:
    """Best-effort detection of the installed SDK version as a tuple of ints.

    Returns an empty tuple if the version cannot be determined, in which case
    callers should assume the newest API shape.
    """
    raw = None
    try:
        from importlib.metadata import PackageNotFoundError, version

        try:
            raw = version("couchbase")
        except PackageNotFoundError:
            raw = None
    except ImportError:  # pragma: no cover - importlib.metadata is stdlib on 3.9+
        raw = None

    if raw is None:
        try:
            from couchbase._version import __version__ as raw  # type: ignore[no-redef]
        except Exception:
            return ()

    parts = []
    for chunk in str(raw).split(".")[:3]:
        digits = ""
        for char in chunk:
            if not char.isdigit():
                break
            digits += char
        if not digits:
            break
        parts.append(int(digits))

    return tuple(parts)


SDK_VERSION = _sdk_version()

# SDK 4.1.9 replaced `create_collection(CollectionSpec)` with
# `create_collection(scope_name, collection_name, settings=None)`. The
# CollectionSpec overload still works but is deprecated and documented for
# removal, so prefer the newer signature whenever it is available. An unknown
# SDK version is treated as new rather than old.
_HAS_NAMED_CREATE_COLLECTION = not SDK_VERSION or SDK_VERSION >= (4, 1, 9)


def create_collection(manager: Any, scope_name: str, collection_name: str) -> None:
    """Create a collection using whichever manager API the installed SDK offers.

    Args:
        manager:
            A :class:`couchbase.management.collections.CollectionManager`.
        scope_name (str):
            The name of the scope to create the collection in.
        collection_name (str):
            The name of the collection to create.
    """
    if _HAS_NAMED_CREATE_COLLECTION:
        manager.create_collection(scope_name, collection_name)
        return

    # Imported lazily: CollectionSpec is deprecated and will eventually be
    # dropped, and we must not fail at import time on SDKs that no longer
    # ship it.
    from couchbase.management.collections import CollectionSpec

    manager.create_collection(CollectionSpec(collection_name, scope_name=scope_name))
