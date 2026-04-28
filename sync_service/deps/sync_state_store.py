# src/sync_service/dependencies/sync_state_store.py

import abc
from pydantic import BaseModel

from datetime import datetime, timezone
from typing import Optional
import threading


class SyncStateRecord(BaseModel):
    """The state stored per (org_id, internal_id, provider)."""
    external_id: str
    last_sync_hash: str
    last_sync_direction: str   # "internal_to_external" or "external_to_internal"
    last_synced_at: datetime

class SyncStateStore(abc.ABC):
    @abc.abstractmethod
    def get_state(self, org_id: str, internal_id: str, provider: str) -> Optional[SyncStateRecord]:
        ...

    @abc.abstractmethod
    def upsert_state(self, org_id: str, internal_id: str, provider: str,
                     external_id: Optional[str], sync_hash: str,
                     direction: str) -> None:
        ...

    @abc.abstractmethod
    def delete_state(self, org_id: str, internal_id: str, provider: str) -> None:
        ...


class MockSyncStateStore(SyncStateStore):
    def __init__(self):
        self._lock = threading.RLock()
        # key = (org_id, internal_id, provider)
        self._data: dict[tuple[str, str, str], SyncStateRecord] = {}

    def get_state(self, org_id: str, internal_id: str, provider: str) -> Optional[SyncStateRecord]:
        with self._lock:
            return self._data.get((org_id, internal_id, provider))

    def upsert_state(self, org_id: str, internal_id: str, provider: str,
                     external_id: Optional[str], sync_hash: str,
                     direction: str) -> None:
        with self._lock:
            self._data[(org_id, internal_id, provider)] = SyncStateRecord(
                external_id=external_id,
                last_sync_hash=sync_hash,
                last_sync_direction=direction,
                last_synced_at=datetime.now(tz=timezone.utc)
            )

    def delete_state(self, org_id: str, internal_id: str, provider: str) -> None:
        with self._lock:
            self._data.pop((org_id, internal_id, provider), None)