import pytest

from .couchbase_session import instance
from .helpers import get_item, get_items


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
