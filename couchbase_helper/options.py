from datetime import timedelta
from typing import Any, Dict, Optional, Union

from couchbase.options import (
    InsertMultiOptions,
    InsertOptions,
    GetMultiOptions,
    GetOptions,
    QueryOptions,
    RemoveMultiOptions,
    RemoveOptions,
    UpsertMultiOptions,
    UpsertOptions,
    ViewOptions,
)

from .session import Session
from .timeout import Timeout

_TIMEOUT = Timeout()


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
    if opts is None:
        opts = {}

    if session is not None:
        timeout = session.timeout
    else:
        timeout = _TIMEOUT

    default_timeout = timedelta(seconds=timeout.kv)
    if type_ == "insert":
        base_options = InsertOptions
    elif type_ == "insert_multi":
        base_options = InsertMultiOptions
    elif type_ == "upsert":
        base_options = UpsertOptions
    elif type_ == "upsert_multi":
        base_options = UpsertMultiOptions
    elif type_ == "get":
        base_options = GetOptions
    elif type_ == "get_multi":
        base_options = GetMultiOptions
    elif type_ == "query":
        base_options = QueryOptions
        default_timeout = timedelta(seconds=timeout.query)
    elif type_ == "remove":
        base_options = RemoveOptions
    elif type_ == "remove_multi":
        base_options = RemoveMultiOptions
    elif type_ == "view":
        base_options = ViewOptions
    else:
        raise AttributeError(f"invalid attribute value {type_}")

    if "timeout" not in opts:
        opts["timeout"] = default_timeout

    ret = base_options(**opts)

    if ret is None:
        ret = {}

    if expiry is not None and isinstance(expiry, int):
        ret["expiry"] = timedelta(seconds=expiry)

    return ret
