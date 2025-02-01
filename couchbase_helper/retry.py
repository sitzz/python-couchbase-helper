# Inspired by: https://docs.couchbase.com/python-sdk/current/howtos/error-handling.html
from enum import Enum
from functools import wraps
from random import randint
from time import sleep
from typing import Callable, Optional, Tuple


class RetryPolicy(Enum):
    FLAT = 1
    LINEAR = 2
    EXPONENTIAL = 3
    RANDOM = 4


def retry(
    attempts: int = 3,
    delay: float = 0.1,
    policy: RetryPolicy = RetryPolicy.FLAT,
    exceptions: Optional[Tuple[Exception]] = None,
) -> Callable:
    def handler(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempts_left in reversed(range(attempts)):
                try:
                    return func(*args, **kwargs)
                except Exception as _exc:
                    if exceptions is None or not isinstance(_exc, exceptions):
                        raise

                    if attempts_left == 0:
                        raise

                    backoff = delay
                    if policy == RetryPolicy.LINEAR:
                        backoff = delay * (attempts - attempts_left)
                    if policy == RetryPolicy.EXPONENTIAL:
                        backoff = (delay / 2) * (2 ** (attempts - attempts_left))
                    if policy == RetryPolicy.RANDOM:
                        backoff = delay * (randint(17, 233) / 100)

                    sleep(backoff)

        return wrapper

    return handler
