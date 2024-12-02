import json

from .couchbase_session import instance
from .helpers import get_item, get_items


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
