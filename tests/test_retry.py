import pytest

from couchbase_helper import retry as retry_module
from couchbase_helper.retry import RetryPolicy, retry


class _Flaky:
    """Raises `exc` for the first `failures` calls, then returns "ok"."""

    def __init__(self, failures, exc=ValueError):
        self.failures = failures
        self.exc = exc
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exc("boom")
        return "ok"


@pytest.fixture
def sleeps(monkeypatch):
    recorded = []
    monkeypatch.setattr(retry_module, "sleep", recorded.append)
    return recorded


def test_success_first_try(sleeps):
    func = _Flaky(0)
    assert retry(exceptions=(ValueError,))(func)() == "ok"
    assert func.calls == 1
    assert sleeps == []


def test_retries_then_succeeds(sleeps):
    func = _Flaky(2)
    assert retry(attempts=3, exceptions=(ValueError,))(func)() == "ok"
    assert func.calls == 3
    assert len(sleeps) == 2


def test_reraises_when_attempts_exhausted(sleeps):
    func = _Flaky(5)
    with pytest.raises(ValueError):
        retry(attempts=3, exceptions=(ValueError,))(func)()
    assert func.calls == 3
    assert len(sleeps) == 2


def test_unlisted_exception_is_not_retried(sleeps):
    func = _Flaky(1, exc=KeyError)
    with pytest.raises(KeyError):
        retry(exceptions=(ValueError,))(func)()
    assert func.calls == 1
    assert sleeps == []


def test_no_exceptions_configured_never_retries(sleeps):
    func = _Flaky(1)
    with pytest.raises(ValueError):
        retry()(func)()
    assert func.calls == 1


def test_zero_attempts_returns_none():
    func = _Flaky(0)
    assert retry(attempts=0)(func)() is None
    assert func.calls == 0


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (RetryPolicy.FLAT, [1.0, 1.0, 1.0]),
        (RetryPolicy.LINEAR, [1.0, 2.0, 3.0]),
        (RetryPolicy.EXPONENTIAL, [1.0, 2.0, 4.0]),
    ],
)
def test_backoff_policies(sleeps, policy, expected):
    func = _Flaky(3)
    retry(attempts=4, delay=1.0, policy=policy, exceptions=(ValueError,))(func)()
    assert sleeps == expected


def test_backoff_random(sleeps, monkeypatch):
    monkeypatch.setattr(retry_module, "randint", lambda low, high: 50)
    func = _Flaky(2)
    retry(delay=2.0, policy=RetryPolicy.RANDOM, exceptions=(ValueError,))(func)()
    assert sleeps == [1.0, 1.0]


def test_wraps_preserves_metadata():
    @retry()
    def documented():
        """docstring"""

    assert documented.__name__ == "documented"
    assert documented.__doc__ == "docstring"
