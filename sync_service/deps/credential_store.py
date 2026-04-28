import abc
import threading
from typing import Any


class CredentialStore(abc.ABC):
    @abc.abstractmethod
    def get_credentials(self, org_id: str) -> dict[str, Any]:
        ...


class MockCredentialStore(CredentialStore):
    def __init__(self):
        self._lock = threading.RLock()
        self._store: dict[str, dict[str, Any]] = {}

    def add_credentials(self, org_id: str, credentials: dict[str, Any]):
        with self._lock:
            self._store[org_id] = credentials

    def get_credentials(self, org_id: str) -> dict[str, Any]:
        with self._lock:
            if org_id not in self._store:
                raise ValueError(f"No credentials for org {org_id}")
            return self._store[org_id]
