from couchbase_helper import Timeout


def test_defaults():
    timeout = Timeout()
    assert (timeout.connection, timeout.kv, timeout.query) == (10, 5, 30)


def test_init_classmethod():
    timeout = Timeout.init(1, 2, 3)
    assert isinstance(timeout, Timeout)
    assert (timeout.connection, timeout.kv, timeout.query) == (1, 2, 3)
