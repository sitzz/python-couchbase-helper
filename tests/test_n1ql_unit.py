"""N1ql query building checked against the exact SQL++ sent to the cluster.

Each test gets a fresh N1ql over a stub session, so unlike test_n1ql.py nothing
here depends on test order or on fake_couchbase.
"""

from datetime import timedelta
import logging

import pytest
from couchbase.n1ql import QueryScanConsistency

from couchbase_helper import Timeout
from couchbase_helper.n1ql import N1ql

from .stubs import StubCluster, StubCollection, StubScope, StubSession, named_logger


class _QueryResult:
    def __init__(self, rows):
        self._rows = rows

    def rows(self):
        return self._rows


@pytest.fixture
def session():
    session = StubSession()
    session.cluster = StubCluster(query_result=_QueryResult(["row"]))
    return session


@pytest.fixture
def n1ql(session):
    return N1ql(session)


def _sent(n1ql):
    """The single (statement, options) pair the cluster received."""
    ((statement, kwargs),) = n1ql.session.cluster.queries
    return statement, kwargs


##################################################
# __init__                                       #
##################################################
def test_init_explicit_logger_is_deprecated(caplog):
    logger = named_logger("n1ql-explicit")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        n1ql = N1ql(StubSession(), logger=logger)
    assert n1ql.logger is logger
    assert "deprecated" in caplog.text


def test_init_uses_session_logger():
    logger = named_logger("n1ql-session")
    assert N1ql(StubSession(logger=logger)).logger is logger


def test_init_falls_back_to_root_logger():
    assert N1ql(StubSession()).logger is logging.getLogger()


def test_init_connects_session():
    session = StubSession(connected=False)
    N1ql(session)
    assert session.connect_calls == 1


##################################################
# Builder methods                                #
##################################################
def test_select_none_resets_to_star(n1ql):
    n1ql.select("a").select(None)
    assert n1ql._columns == ["*"]


def test_orwhere_aliases_or_where(n1ql):
    n1ql.where("a=", 1).orwhere("b=", 2)
    assert n1ql._conditions == [("AND", "a =", 1), ("OR", "b =", 2)]


def test_skip_aliases_offset(n1ql):
    assert n1ql.skip(7)._offset == 7


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("age", "age ="),
        ("age=", "age ="),
        ("age >", "age >"),
        ("age>=", "age >="),
        ("age<", "age <"),
        ("age<=", "age <="),
        ("age!=", "age !="),
        ("age<>", "age <>"),
        ("age==", "age =="),
        ("name like", "name LIKE"),
        ("name NOT  like", "name NOT LIKE"),
        ("unlike", "unlike ="),
        ("value>", "`value` >"),
    ],
)
def test_where_keeps_operator(n1ql, key, expected):
    """Regression: every operator used to collapse into '='."""
    n1ql.where(key, 5)
    assert n1ql._conditions[0][1] == expected


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("deleted", "deleted IS NULL"),
        ("deleted is null", "deleted IS NULL"),
        ("deleted IS NOT NULL", "deleted IS NOT NULL"),
    ],
)
def test_where_null_checks(n1ql, key, expected):
    """Regression: where(key, None) produced 'key =' instead of 'key IS NULL'."""
    n1ql.where(key, None)
    assert n1ql._conditions[0][1] == expected


def test_where_explicit_operator_with_none_is_a_parameter(n1ql):
    n1ql.where("deleted=", None)
    assert n1ql._conditions == [("AND", "deleted =", None)]


@pytest.mark.parametrize(
    ("word", "expected"),
    [("name", "name"), ("select", "`select`"), ("Value", "`Value`"), ("a.b", "`a.b`")],
)
def test_enclose_reserved_word(word, expected):
    assert N1ql._enclose_reserved_word(word) == expected


##################################################
# rows(): generated statement                    #
##################################################
def test_rows_minimal(n1ql):
    assert n1ql.rows() == ["row"]
    statement, kwargs = _sent(n1ql)
    assert statement == "SELECT * FROM test t"
    assert "positional_parameters" not in kwargs


def test_rows_full_statement(n1ql):
    n1ql.select("name, value").distinct().where("type=", "shop").or_where(
        "type=", "bakery"
    ).limit(10).offset(20)

    n1ql.rows()

    statement, kwargs = _sent(n1ql)
    assert statement == (
        "SELECT DISTINCT t.name, t.`value` FROM test t "
        "WHERE t.type = $1 OR t.type = $2  LIMIT 10 OFFSET 20"
    )
    assert kwargs["positional_parameters"] == ["shop", "bakery"]


def test_rows_null_checks_take_no_parameter(n1ql):
    n1ql.where("a=", 1).where("b", None).or_where("c is not null", None).where("d>", 2)

    n1ql.rows()

    statement, kwargs = _sent(n1ql)
    assert statement == (
        "SELECT * FROM test t "
        "WHERE t.a = $1 AND t.b IS NULL OR t.c IS NOT NULL AND t.d > $2"
    )
    assert kwargs["positional_parameters"] == [1, 2]


def test_rows_non_default_keyspace(n1ql, session):
    session.scope = StubScope("inventory")
    session.collection = StubCollection("items")

    n1ql.rows()

    statement, _ = _sent(n1ql)
    assert statement == "SELECT * FROM test.inventory.items t"


def test_rows_resets_builder_state(n1ql):
    n1ql.select("a").distinct().where("a=", 1).limit(1).offset(1)
    n1ql.rows()
    assert n1ql._columns == ["*"]
    assert not n1ql._distinct
    assert n1ql._conditions == []
    assert n1ql._limit is None
    assert n1ql._offset is None


def test_rows_returns_none_when_query_fails(n1ql, session, caplog):
    session.cluster.query_result = RuntimeError("cluster down")
    with caplog.at_level(logging.ERROR):
        assert n1ql.rows() is None
    assert "cluster down" in caplog.text
    assert "RuntimeError" in caplog.text


##################################################
# _execute                                       #
##################################################
def test_execute_uses_session_query_timeout(n1ql, session):
    session.timeout = Timeout(query=42)
    n1ql._execute("SELECT 1")
    statement, kwargs = _sent(n1ql)
    assert statement == "SELECT 1"
    assert kwargs["timeout"] == timedelta(seconds=42)
    assert "scan_consistency" not in kwargs


def test_execute_forwards_consistency(n1ql):
    """Regression: consistency was set on an N1QLQuery that was then discarded."""
    n1ql._execute("SELECT 1", consistency=QueryScanConsistency.REQUEST_PLUS)
    _, kwargs = _sent(n1ql)
    assert kwargs.get("scan_consistency") == QueryScanConsistency.REQUEST_PLUS
