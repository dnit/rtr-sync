import pytest
from queue import Queue
from sync_service.models import SyncEvent, InternalContact
from sync_service.worker import ConsumerWorkerI2E
from sync_service.deps.sync_state_store import MockSyncStateStore
from sync_service.deps.idempotency_store import InMemoryIdempotencyStore
from sync_service.deps.api_client import MockExternalAPIClient
from sync_service.deps.rate_limiter import MockRedisRateLimiter
from sync_service.deps.credential_store import MockCredentialStore
from datetime import datetime


@pytest.fixture
def setup_worker():
    state = MockSyncStateStore()
    idemp = InMemoryIdempotencyStore()
    creds = MockCredentialStore()
    creds.add_credentials("org-1", {"access_token": "token-org1"})
    limiter = MockRedisRateLimiter(rate=100)
    client = MockExternalAPIClient(creds, failure_rate=0, rate_limit_prob=0)
    dlq = Queue()
    worker = ConsumerWorkerI2E(0, state,limiter, idemp, client, dlq)
    return worker, state, idemp, client, dlq


def test_create_sets_sync_state(setup_worker):
    worker, state, _, _, _ = setup_worker
    event = SyncEvent(
        event_id="ev1", event_type="create", internal_id="int-1",
        org_id="org-1", object_type="contact", crm_type="salesforce",
        payload=InternalContact(
            id="int-1", first_name="A", last_name="B",
            email="a@b.com", phone="", updated_at=datetime.now()
        ),
        timestamp=datetime.now()
    )
    worker._process_event(event)
    assert state.get_state(event.org_id, event.internal_id, event.crm_type) is not None


def test_duplicate_event_ignored(setup_worker):
    worker, _, idemp, api_client, _ = setup_worker
    idemp.is_processed("ev-dup")
    event = SyncEvent(
        event_id="ev-dup", event_type="create", internal_id="int-1",
        org_id="org-1", object_type="contact", crm_type="salesforce",
        payload=InternalContact(
            id="int-1", first_name="A", last_name="B",
            email="a@b.com", phone="", updated_at=datetime.now()
        ),
        timestamp=datetime.now()
    )
    worker._process_event(event)
    