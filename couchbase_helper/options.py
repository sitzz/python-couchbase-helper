from datetime import timedelta
from typing import Any, Dict, Optional, Union

from couchbase.options import (
    GetMultiOptions,
    GetOptions,
    InsertMultiOptions,
    InsertOptions,
    QueryOptions,
    RemoveMultiOptions,
    RemoveOptions,
    ReplaceMultiOptions,
    ReplaceOptions,
    UpsertMultiOptions,
    UpsertOptions,
    ViewOptions,
)

from .session import Session
from .timeout import Timeout

_TIMEOUT = Timeout()
_TYPES = {
    "get": GetOptions,
    "get_multi": GetMultiOptions,
    "insert": InsertOptions,
    "insert_multi": InsertMultiOptions,
    "query": QueryOptions,
    "remove": RemoveOptions,
    "remove_multi": RemoveMultiOptions,
    "replace": ReplaceOptions,
    "replace_multi": ReplaceMultiOptions,
    "upsert": UpsertOptions,
    "upsert_multi": UpsertMultiOptions,
    "view": ViewOptions,
}


def build_opts(
    type_: str,
    *,
    opts: Optional[Dict[str, Any]] = None,
    expiry: Optional[Union[int, timedelta]] = None,
    session: Optional[Session] = None,
) -> Union[
    InsertOptions,
    InsertMultiOptions,
    UpsertOptions,
    UpsertMultiOptions,
    ReplaceOptions,
    ReplaceMultiOptions,
    GetOptions,
    GetMultiOptions,
    QueryOptions,
    RemoveOptions,
    RemoveMultiOptions,
    ViewOptions,
]:
    """Generates operation options for specified operation type.

    Args:
        type_ (str):
            The type of operation to return options for.
        opts (Dict[str, Any]):
            Optional options to use for initiating the operation options instance.
        expiry (int | timedelta | None):
            Optional general document expiry to use for the operations.
        session (couchbase_helper.Session):
            Optional session to fetch timeout settings from

    Returns:
        Dict[str, Any]
    """
    if type_ not in _TYPES:
        raise AttributeError(f"unknown options type {type_}")

    base_options = _TYPES[type_]

    if opts is None:
        opts = {}

    if session is not None:
        timeout = session.timeout
    else:
        timeout = _TIMEOUT

    if "timeout" not in opts:
        if type_ in ("query", "view"):
            opts["timeout"] = timedelta(seconds=timeout.query)
        else:
            opts["timeout"] = timedelta(seconds=timeout.kv)

    if expiry is not None:
        if isinstance(expiry, int):
            expiry = timedelta(seconds=expiry)
        opts["expiry"] = expiry

    return base_options(**opts)
