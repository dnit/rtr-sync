import abc
from cachetools import TTLCache

class IdempotencyStore(abc.ABC):
    @abc.abstractmethod
    def is_processed(self, event_id: str) -> bool: ...

    @abc.abstractmethod
    def mark_processed(self, event_id: str) -> None: ...


class InMemoryIdempotencyStore(IdempotencyStore):
    def __init__(self, ttl_seconds: float = 300):
        self._cache = TTLCache(maxsize=100, ttl=ttl_seconds)

    def is_processed(self, event_id: str) -> bool:
        return event_id in self._cache

    def mark_processed(self, event_id: str) -> None:
        self._cache[event_id] = True