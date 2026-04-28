import logging
import hashlib
import random
import threading
import time

from queue import Queue, Empty

from .exceptions import RetryableError
from .models import SyncEvent
from .retry import retry
from .tranformers.registry import get_transformer
from .deps.rate_limiter import RateLimiter
from .deps.sync_state_store import SyncStateStore
from .deps.idempotency_store import IdempotencyStore
from .deps.api_client import ExternalAPIClient


logger = logging.getLogger(__name__)


class ConsumerWorkerI2E(threading.Thread):
    def __init__(self, consumer_id: int, state_store: SyncStateStore, rate_limier: RateLimiter,
                 idempotency_store: IdempotencyStore, api_client: ExternalAPIClient, dlq: Queue):
        super().__init__(daemon=True)
        self.consumer_id = consumer_id
        self._sync_store = state_store
        self._rate_limiter = rate_limier
        self._idempotency = idempotency_store
        self._api_client = api_client
        self._dlq = dlq
        self._queue = Queue()
        self._stop_event = threading.Event()

    def enqueue(self, event: SyncEvent):
        self._queue.put(event)

    def run(self):
        while not self._stop_event.is_set():
            try:
                event = self._queue.get(timeout=0.1)
            except Empty:
                continue
            if event is None:
                break
            self._process_event(event)
            self._queue.task_done()

    def stop(self):
        self._stop_event.set()


    def _compute_hash(self, payload) -> str:
        # deterministic JSON dump
        raw = payload.model_dump_json(exclude={'updated_at'})  # maybe exclude timestamp fields
        return hashlib.sha256(raw.encode()).hexdigest()

    def _process_event(self, event: SyncEvent):
        logger = logging.getLogger(__name__)
        logger.info(f"[C{self.consumer_id}] {event.event_id} org={event.org_id} {event.event_type}")

        if self._idempotency.is_processed(event.event_id):
            logger.info(f"Already processed {event.event_id}")
            return

        new_hash = self._compute_hash(event.payload)
        state = self._sync_store.get_state(event.org_id, event.internal_id, event.crm_type)
        if state and state.last_sync_hash == new_hash:
            logger.info(f"Skipping event {event.event_id}: hash unchanged")
            return

        try:
            transformer = get_transformer(event.org_id, event.crm_type, event.object_type)
        except ValueError as e:
            logger.error(f"Transformer not found: {e}, sending to DLQ")
            self._dlq.put(event)
            return

        try:
            self._process_with_retry(event, transformer, new_hash)
        except Exception:
            logger.exception(f"Permanent failure for {event.event_id}, sending to DLQ")
            try:
                self._dlq.put(event, block=False)
            except Exception:
                logger.error(f"DLQ full, dropping {event.event_id}")

    def _wait_for_rate_limit(
            self,
            org_id: str,
            object_type: str,
            provider_type: str,
            max_wait: float = 30.0,
        ) -> None:
        """
        Non‑blocking check of the rate limiter; if not allowed, sleep with
        exponential backoff + jitter until allowed or timeout.
        Raises RetryableError on timeout so the outer retry loop can retry.
        """
        base_delay = 0.05
        attempt = 0
        deadline = time.time() + max_wait

        while True:
            if self._rate_limiter.allow(org_id, object_type, provider_type):
                return

            if time.time() > deadline:
                raise RetryableError(
                    f"Rate limit wait timeout for org={org_id}, provider={provider_type}"
                )

            backoff = min(base_delay * (2 ** attempt), 2.0)
            sleep = random.uniform(0, backoff)
            time.sleep(sleep)
            attempt += 1

    @retry(max_attempts=3, base_delay=0.1, max_delay=2.0)
    def _process_with_retry(self, event: SyncEvent, transformer, new_hash: str):
        org = event.org_id
        if event.event_type == "create":
            self._handle_create(org, event, transformer, new_hash)
        elif event.event_type == "update":
            self._handle_update(org, event, transformer, new_hash)
        elif event.event_type == "delete":
            self._handle_delete(org, event, transformer)
        else:
            raise ValueError(f"Unknown event type: {event.event_type}")
        
        try:
            self._idempotency.mark_processed(event.event_id)
        except:
            logger.warning(f"Failed to updated idempotency key as processed {event.event_id}")


    def _handle_create(self, org_id, event, transformer, new_hash):
        ext = transformer.to_external(event.payload)
        created = self._api_client.create(ext)
        self._sync_store.upsert_state(
            org_id, event.internal_id, event.crm_type,
            external_id=created.external_id,
            sync_hash=new_hash,
            direction="internal_to_external"
        )

    def _handle_update(self, org_id, event, transformer, new_hash):
        state = self._sync_store.get_state(org_id, event.internal_id, event.crm_type)
        new_hash = self._compute_hash(event.payload)
        if not state or not state.external_id:
            # fallback to create
            return self._handle_create(org_id, event, transformer, new_hash)

        ext_payload = transformer.to_external(event.payload)
        ext_payload.external_id = state.external_id
        self._api_client.update(state.external_id, ext_payload)

        self._sync_store.upsert_state(
            org_id, event.internal_id, event.crm_type,
            external_id=state.external_id,
            sync_hash=new_hash,
            direction="internal_to_external"
        )

    def _handle_delete(self, org_id, event, transformer):
        sync_state = self._sync_store.get_state(org_id, event.internal_id, event.crm_type)
        
        if not sync_state:
            logger.info(f"Unable to delete {event.internal_id} from provider because external_id not found")
            return
        ext_id = sync_state.external_id
        self._api_client.delete(ext_id)
        self._sync_store.delete_state(org_id, event.internal_id, event.crm_type)
