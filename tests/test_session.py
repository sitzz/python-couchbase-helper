import logging

import pytest
from couchbase_helper.protocols import SessionProt
from couchbase_helper import Timeout

from .couchbase_session import session

HOSTNAME = "localhost"
USERNAME = "test"
PASSWORD = "testtest"


@pytest.mark.order(1)
def test_init():
    # Check session implements SessionProt
    assert isinstance(session, SessionProt)

    # Check session creates a logger instance
    assert isinstance(session.logger, logging.Logger)

    # Check session set's a default timeout
    _timeout = Timeout()
    assert isinstance(session.timeout, Timeout)
    assert session.timeout.connection == _timeout.connection
    assert session.timeout.kv == _timeout.kv
    assert session.timeout.query == _timeout.query

    # Check no connection was attempted/created
    assert not session.connected


@pytest.mark.order(2)
def test_connect():
    session.connect()
    assert session.connected
