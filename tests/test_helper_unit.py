"""CouchbaseHelper failure paths, driven through stubs rather than a cluster.

test_helper.py covers the happy paths end-to-end against fake_couchbase; this
module covers what the fake cannot easily produce: partial failures, SDK
exceptions and view queries.
"""

import logging

import pytest
from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from couchbase.options import InsertOptions, ReplaceOptions, UpsertOptions

from couchbase_helper import CouchbaseHelper

from .stubs import (
    StubBucket,
    StubCollection,
    StubDocument,
    StubMultiResult,
    StubSession,
    named_logger,
)


def _helper(**responses):
    return CouchbaseHelper(StubSession(collection=StubCollection(**responses)))


def _calls(helper):
    return helper.session.collection.calls


##################################################
# __init__                                       #
##################################################
def test_init_explicit_logger_is_deprecated(caplog):
    logger = named_logger("explicit")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        helper = CouchbaseHelper(StubSession(), logger=logger)
    assert helper.logger is logger
    assert "deprecated" in caplog.text


def test_init_uses_session_logger():
    logger = named_logger("session")
    assert CouchbaseHelper(StubSession(logger=logger)).logger is logger


def test_init_falls_back_to_root_logger():
    assert CouchbaseHelper(StubSession()).logger is logging.getLogger()


def test_init_connects_session():
    session = StubSession(connected=False)
    CouchbaseHelper(session)
    assert session.connect_calls == 1


def test_init_leaves_connected_session_alone():
    session = StubSession()
    CouchbaseHelper(session)
    assert session.connect_calls == 0


##################################################
# Single-document operations                     #
##################################################
def test_insert_passes_expiry_and_options():
    helper = _helper()
    assert helper.insert("k", {"a": 1}, expiry=10)
    ((op, kwargs),) = _calls(helper)
    assert op == "insert"
    assert (kwargs["key"], kwargs["value"]) == ("k", {"a": 1})
    assert isinstance(kwargs["opts"], InsertOptions)
    assert kwargs["opts"]["expiry"].total_seconds() == 10


def test_insert_existing_returns_false():
    assert not _helper(insert=DocumentExistsException()).insert("k", {})


def test_upsert_not_found_returns_false():
    assert not _helper(upsert=DocumentNotFoundException()).upsert("k", {})


def test_replace_not_found_returns_false():
    assert not _helper(replace=DocumentNotFoundException()).replace("k", {})


def test_get_returns_content_or_raw():
    document = StubDocument({"a": 1})
    helper = _helper(get=document)
    assert helper.get("k") == {"a": 1}
    assert helper.get("k", raw=True) is document


def test_get_not_found_returns_none():
    assert _helper(get=DocumentNotFoundException()).get("k") is None


def test_get_multi_returns_contents():
    result = StubMultiResult(results={"a": StubDocument({"a": 1})})
    assert _helper(get_multi=result).get_multi(["a"]) == [{"a": 1}]


def test_get_multi_not_found_returns_none():
    assert _helper(get_multi=DocumentNotFoundException()).get_multi(["k"]) is None


def test_remove_not_found_returns_false():
    assert not _helper(remove=DocumentNotFoundException()).remove("k")


def test_delete_aliases_remove():
    helper = _helper()
    assert helper.delete("k")
    assert _calls(helper)[0][0] == "remove"


##################################################
# Multi-document operations                      #
##################################################
_MULTI_OPS = [
    ("insert_multi", InsertOptions),
    ("upsert_multi", UpsertOptions),
    ("replace_multi", ReplaceOptions),
]


@pytest.mark.parametrize(("op", "per_key_type"), _MULTI_OPS)
def test_multi_per_key_options(op, per_key_type):
    helper = _helper(**{op: StubMultiResult()})

    assert getattr(helper, op)({"a": {}}, per_key_opts={"a": {"expiry": None}})

    ((_, kwargs),) = _calls(helper)
    per_key = kwargs["opts"]["per_key_options"]
    assert isinstance(per_key["a"], per_key_type)
    assert "timeout" in per_key["a"]


@pytest.mark.parametrize(("op", "_"), _MULTI_OPS)
def test_multi_partial_failure_logs_each_key(op, _, caplog):
    result = StubMultiResult(all_ok=False, exceptions={"a": "boom-a", "b": "boom-b"})
    helper = _helper(**{op: result})

    with caplog.at_level(logging.ERROR):
        assert not getattr(helper, op)({"a": {}, "b": {}})

    assert "boom-a" in caplog.text
    assert "boom-b" in caplog.text


@pytest.mark.parametrize(("op", "_"), _MULTI_OPS)
def test_multi_unexpected_exception_is_logged(op, _, caplog):
    helper = _helper(**{op: RuntimeError("kaboom")})

    with caplog.at_level(logging.ERROR):
        assert not getattr(helper, op)({"a": {}})

    assert "RuntimeError" in caplog.text


def test_remove_multi_partial_failure(caplog):
    result = StubMultiResult(all_ok=False, exceptions={"a": "boom-a"})
    with caplog.at_level(logging.ERROR):
        assert not _helper(remove_multi=result).remove_multi(["a"])
    assert "boom-a" in caplog.text


def test_remove_multi_unexpected_exception(caplog):
    with caplog.at_level(logging.ERROR):
        assert not _helper(remove_multi=RuntimeError("x")).remove_multi(["a"])
    assert "RuntimeError" in caplog.text


def test_delete_multi_aliases_remove_multi():
    helper = _helper(remove_multi=StubMultiResult())
    assert helper.delete_multi(["a"])
    assert _calls(helper)[0][0] == "remove_multi"


##################################################
# view_query                                     #
##################################################
class _ViewResult:
    def __init__(self, rows):
        self._rows = rows

    def rows(self):
        return self._rows


def _view_helper(view):
    return CouchbaseHelper(StubSession(bucket=StubBucket(view=view)))


def test_view_query_passes_limit_and_skip():
    helper = _view_helper(_ViewResult(["r"]))
    helper.view_query("ddoc", "view", limit=5, skip=2)

    (kwargs,) = helper.session.bucket.view_calls
    assert (kwargs["design_doc"], kwargs["view_name"]) == ("ddoc", "view")
    assert (kwargs["limit"], kwargs["skip"]) == (5, 2)


def test_view_query_returns_rows():
    assert _view_helper(_ViewResult(["r"])).view_query("d", "v") == ["r"]


def test_view_query_error_is_logged(caplog):
    with caplog.at_level(logging.ERROR):
        assert _view_helper(RuntimeError("view broke")).view_query("d", "v") is None
    assert "view broke" in caplog.text


def test_view_query_error_without_args_is_logged(caplog):
    """Regression: the handler read _exc.args[0] and raised IndexError itself."""
    with caplog.at_level(logging.ERROR):
        assert _view_helper(RuntimeError()).view_query("d", "v") is None
    assert "RuntimeError" in caplog.text
