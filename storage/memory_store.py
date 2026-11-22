"""
In-memory reference implementation of QueueStore.

Used (1) by the broker's own tests and (2) as a stand-in until the SQLite
store (storage/queue_store.py, Labrador) is merged. Same semantics as the
durable store, except that nothing survives a broker restart.
"""
from __future__ import annotations

import itertools
import threading
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Set, Tuple

from storage.interface import QueueStore, StoredMessage


class MemoryQueueStore(QueueStore):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = itertools.count(1)
        self._messages: Dict[str, StoredMessage] = {}
        self._subs: Dict[str, Set[str]] = defaultdict(set)
        self._pending: Dict[Tuple[str, str], bool] = {}   # (client_id, msg_id) -> acked

    def save_message(self, msg_id: str, topic: str, payload: Any, publisher_id: str,
                     timestamp: int, subscriber_ids: Iterable[str]) -> bool:
        with self._lock:
            if msg_id in self._messages:
                return False
            self._messages[msg_id] = StoredMessage(msg_id, topic, payload, publisher_id,
                                                   timestamp, next(self._seq))
            for cid in subscriber_ids:
                self._pending[(cid, msg_id)] = False
            return True

    def register_subscription(self, client_id: str, pattern: str) -> None:
        with self._lock:
            self._subs[client_id].add(pattern)

    def remove_subscription(self, client_id: str, pattern: str) -> None:
        with self._lock:
            self._subs.get(client_id, set()).discard(pattern)
            if client_id in self._subs and not self._subs[client_id]:
                del self._subs[client_id]

    def load_subscriptions(self) -> Dict[str, Set[str]]:
        with self._lock:
            return {cid: set(p) for cid, p in self._subs.items() if p}

    def get_pending(self, client_id: str) -> List[StoredMessage]:
        with self._lock:
            msgs = [self._messages[mid] for (cid, mid), acked in self._pending.items()
                    if cid == client_id and not acked]
        return sorted(msgs, key=lambda m: m.seq)

    def mark_acked(self, client_id: str, msg_id: str) -> bool:
        with self._lock:
            if self._pending.get((client_id, msg_id)) is False:
                self._pending[(client_id, msg_id)] = True
                return True
            return False
