import abc
import threading
import time


class RateLimiter(abc.ABC):
    @abc.abstractmethod
    def allow(self, org_id:str, object_type: str, provider_type: str) -> None: ...


class MockRedisRateLimiter(RateLimiter):
    def __init__(self, rate: float):
        """
        Simulates a Redis token bucket with bucket size = 1.
        rate : max tokens (requests) per second
        """
        self._rate = rate
        self._buckets: dict[str, tuple[float, float]] = {}  # key -> (tokens, last_refill)
        self._lock = threading.Lock()

    def _key(self, org_id: str, provider_type: str) -> str:
        return f"{org_id}:{provider_type}"

    def allow(self, org_id: str, object_type: str, provider_type: str) -> bool:
        key = self._key(org_id, provider_type)
        with self._lock:
            now = time.monotonic()
            tokens, last = self._buckets.get(key, (1.0, now))
            elapsed = now - last
            new_tokens = elapsed * self._rate
            tokens = min(1.0, tokens + new_tokens)
            last = now
            if tokens >= 1:
                tokens -= 1
                self._buckets[key] = (tokens, last)
                return True
            self._buckets[key] = (tokens, last)
            return False