import pytest

from .couchbase_session import instance
from .helpers import get_item, get_items


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
