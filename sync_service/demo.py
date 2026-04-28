import logging
import random
from datetime import datetime
from queue import Queue

from .models import SyncEvent, InternalContact
from .deps.sync_state_store import MockSyncStateStore
from .deps.rate_limiter import MockRedisRateLimiter
from .deps.idempotency_store import InMemoryIdempotencyStore
from .deps.credential_store import MockCredentialStore
from .deps.api_client import MockExternalAPIClient
from .worker import ConsumerWorkerI2E
from .scheduler import EventScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def main():

    NUM_CONSUMERS = 4
    TOTAL_EVENTS = 30
    GLOBAL_RPS = 20
    FAILURE_RATE = 0.15
    RATE_LIMIT_PROB = 0.05
    BATCH_SIZE = 5

    # Setup mocks
    creds = MockCredentialStore()
    for org in ["org-1", "org-2"]:
        creds.add_credentials(org, {"access_token": f"token-{org}"})

    rate_limiter = MockRedisRateLimiter(rate=GLOBAL_RPS)
    state = MockSyncStateStore()
    idempotency = InMemoryIdempotencyStore(ttl_seconds=300)
    api_client = MockExternalAPIClient(creds, failure_rate=FAILURE_RATE, rate_limit_prob=RATE_LIMIT_PROB)
    dlq = Queue()

    # Create workers
    consumers = []
    for i in range(NUM_CONSUMERS):
        worker = ConsumerWorkerI2E(i, state, rate_limiter, idempotency, api_client, dlq)
        consumers.append(worker)
        worker.start()

    input_queue = Queue()
    scheduler = EventScheduler(input_queue, consumers, batch_size=BATCH_SIZE)
    scheduler.start()

    # Feed events
    for e in range(TOTAL_EVENTS):
        org_id = random.choice(["org-1", "org-2"])
        provider = random.choice(["salesforce", "hubspot"])
        event_type = random.choice(["create", "update", "delete"])
        internal_id = f"int-rec{e % 6}"
        event = SyncEvent(
            event_id=f"evt-{e}",
            event_type=event_type,
            internal_id=internal_id,
            org_id=org_id,
            object_type="contact",
            payload=InternalContact(
                id=internal_id,
                first_name=f"First{e}",
                last_name=f"Last{e}",
                email=f"user{e}@example.com",
                phone=f"+91-{e:10d}",
                updated_at=datetime.now()
            ),
            timestamp=datetime.now(),
            crm_type=provider
        )
        input_queue.put(event)

    input_queue.put(None)   # signal end
    scheduler.join()
    for c in consumers:
        c.join()

    # Summary
    dlq_count = dlq.qsize()
    logging.info(f"State mappings: {len(state._data)}")
    for k,v in state._data.items():
        logging.info(f"  {k} -> {v}")
    logging.info(f"Dead-lettered events: {dlq_count}")

if __name__ == "__main__":
    main()