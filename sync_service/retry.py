import functools
import random
import time
from .exceptions import RetryableError


def retry(max_attempts: int = 3, base_delay: float = 0.1, max_delay: float = 2.0):
    """Exponential backoff with full jitter. Respects retry_after on exceptions."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except RetryableError as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        raise
                    if hasattr(e, 'retry_after') and e.retry_after is not None:
                        sleep_time = e.retry_after
                    else:
                        backoff = min(base_delay * (2 ** attempt), max_delay)
                        sleep_time = random.uniform(0, backoff)
                    time.sleep(sleep_time)
            raise last_exception
        return wrapper
    return decorator