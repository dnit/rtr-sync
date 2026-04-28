import abc
import random
import threading
import time

from ..models import ExternalContact
from ..exceptions import RateLimitExceededError, RetryableError
from .rate_limiter import RateLimiter
from .credential_store import CredentialStore


class ExternalAPIClient(abc.ABC):
    @abc.abstractmethod
    def create(self, contact: ExternalContact) -> ExternalContact: ...
    @abc.abstractmethod
    def update(self, external_id: str, contact: ExternalContact) -> None: ...
    @abc.abstractmethod
    def delete(self, external_id: str) -> None: ...
    @abc.abstractmethod
    def get_by_external_id(self, external_id: str) -> ExternalContact: ...


class MockExternalAPIClient(ExternalAPIClient):
    def __init__(self, credential_store: CredentialStore,
                 failure_rate: float = 0.15, rate_limit_prob: float = 0.05):
        self._credential_store = credential_store
        self._failure_rate = failure_rate
        self._rate_limit_prob = rate_limit_prob      # probability of a 429
        self._lock = threading.RLock()
        self._store: dict[str, ExternalContact] = {}

    def _maybe_fail(self, operation: str):
        if random.random() < self._rate_limit_prob:
            raise RateLimitExceededError(
                f"429 Rate limit exceeded on {operation}",
                retry_after=random.uniform(0.5, 2.0)
            )
        if random.random() < self._failure_rate:
            raise RetryableError(f"Transient error in {operation}")


    def create(self, contact: ExternalContact) -> ExternalContact:
        self._maybe_fail("create")
        with self._lock:
            ext_id = f"ext-{int(time.time() * 1e9)}"
            contact.external_id = ext_id
            self._store[ext_id] = contact
            return contact

    def update(self, external_id: str, contact: ExternalContact) -> None:
        self._maybe_fail("update")
        with self._lock:
            if external_id not in self._store:
                raise KeyError(f"External ID {external_id} not found")
            contact.external_id = external_id
            self._store[external_id] = contact

    def delete(self, external_id: str) -> None:
        self._maybe_fail("delete")
        with self._lock:
            if external_id in self._store:
                del self._store[external_id]

    def get_by_external_id(self, external_id: str) -> ExternalContact:
        with self._lock:
            if external_id not in self._store:
                raise KeyError(f"External ID {external_id} not found")
            return self._store[external_id]
