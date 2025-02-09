import json
import logging
from typing import Protocol, runtime_checkable

import pytest
from couchbase_helper.protocols import SessionProt
from couchbase_helper import Timeout

from .couchbase_session import instance, session
from .helpers import get_item, get_items


# Hack for checking if session complies
@runtime_checkable
class SessionProtExt(SessionProt, Protocol):
    pass


##################################################
# Test initiation and connection operations      #
##################################################
@pytest.mark.order(1)
def test_init():
    # Check session implements SessionProt
    assert isinstance(session, SessionProtExt)

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


##################################################
# Test inserts and update operations             #
##################################################
def test_insert():
    key, document = get_item()
    assert instance().insert(key, document)


def test_insert_duplicate():
    key, document = get_item()
    assert not instance().insert(key, document)


def test_insert_multi():
    documents_raw = get_items(100)
    documents = {key: document for key, document in documents_raw}
    assert instance().insert_multi(documents)


def test_upsert_existing():
    key, document = get_item()
    assert instance().upsert(key, document)


def test_upsert():
    key, document = get_item("second")
    assert instance().upsert(key, document)


def test_upsert_multi():
    documents_raw = get_items(100)
    documents = {key: document for key, document in documents_raw}
    assert instance().upsert_multi(documents)


@pytest.mark.order(after="test_insert")
def test_update():
    key, document = get_item()
    document["update"] = True
    assert instance().upsert(key, document)


##################################################
# Test get and get_multi operations              #
##################################################
def test_get():
    key, document = get_item("get_test")
    instance().insert(key, document)
    document_fetched = instance().get(key)

    assert json.dumps(document) == json.dumps(document_fetched)
    assert instance().remove(key)


def test_get_not_found():
    key, _ = get_item("get_not_found_test")
    assert not instance().get(key)


def test_get_multi():
    documents_raw = get_items(100, "get_multi_test")
    documents = {key: document for key, document in documents_raw}
    instance().insert_multi(documents)

    keys = [key for key, _ in documents_raw]
    documents_fetched = instance().get_multi(keys, raw=True)
    assert len(documents) == len(documents_fetched)
    assert all(doc.key in keys for doc in documents_fetched)
    assert instance().remove_multi(keys)


##################################################
# Test remove and remove_multi operations        #
##################################################
@pytest.mark.order(after="test_insert")
def test_remove():
    key, document = get_item()
    assert instance().remove(key, document)


@pytest.mark.order(after="test_upsert")
def test_remove_second():
    key, document = get_item("second")
    assert instance().remove(key, document)


@pytest.mark.order(after="test_remove")
def test_remove_not_found():
    key, document = get_item()
    assert not instance().remove(key, document)


@pytest.mark.order(after="test_insert_multi")
def test_remove_multi():
    keys = [key for key, _ in get_items(100)]
    assert instance().remove_multi(keys)


@pytest.mark.order(after="test_remove_multi")
def test_remove_multi_not_found():
    keys = [key for key, _ in get_items(100)]
    assert not instance().remove_multi(keys)
