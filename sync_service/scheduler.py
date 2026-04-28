import hashlib
import logging
import threading
from queue import Queue, Empty
from typing import List
from .models import SyncEvent
from .worker import ConsumerWorkerI2E


class EventScheduler(threading.Thread):
    def __init__(self, input_queue: Queue, consumers: List[ConsumerWorkerI2E], batch_size: int = 10):
        super().__init__(daemon=True)
        self._input_queue = input_queue
        self._consumers = consumers
        self._batch_size = batch_size
        self._stop_event = threading.Event()
        self._num_consumers = len(consumers)

    def _hash_to_consumer(self, internal_id: str) -> int:
        digest = hashlib.sha256(internal_id.encode()).hexdigest()
        return int(digest[:8], 16) % self._num_consumers

    def run(self):
        logger = logging.getLogger(__name__)
        batch = []
        while not self._stop_event.is_set():
            try:
                first = self._input_queue.get(timeout=0.2)
                if first is None:
                    for consumer in self._consumers:
                        consumer.enqueue(None)
                    return
                batch.append(first)
            except Empty:
                continue

            for _ in range(self._batch_size - 1):
                try:
                    event = self._input_queue.get_nowait()
                    if event is None:
                        batch.append(None)
                        break
                    batch.append(event)
                except Empty:
                    break

            for event in batch:
                if event is None:
                    for consumer in self._consumers:
                        consumer.enqueue(None)
                    return
                idx = self._hash_to_consumer(event.internal_id)
                self._consumers[idx].enqueue(event)
            batch.clear()

    def stop(self):
        self._stop_event.set()