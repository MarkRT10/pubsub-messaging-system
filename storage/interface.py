"""
Storage interface between the broker (M1, Jainar) and durable storage (M2, Labrador).

The broker depends ONLY on this abstract class. Labrador's SQLite
implementation lives in storage/queue_store.py as `SQLiteQueueStore(db_path)`
and must pass tests/test_storage_contract.py. Until then the broker can run
with the in-memory reference implementation (storage/memory_store.py), which
has identical semantics but loses data when the broker process exits.

Delivery model (one row per subscriber per message, as in Labrador's L1 schema):

    messages(seq, msg_id UNIQUE, topic, payload, publisher_id, timestamp)
    subscriptions(client_id, pattern)            PRIMARY KEY (client_id, pattern)
    deliveries(client_id, msg_id, acked)         PRIMARY KEY (client_id, msg_id)

The broker decides WHO receives a message (topic routing). Storage only
records it, so storage never needs to understand wildcards.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set


@dataclass(frozen=True)
class StoredMessage:
    """A message as stored and as delivered. `seq` is the storage insertion
    order (None for a message that has not been read back from storage)."""
    msg_id: str
    topic: str
    payload: Any
    publisher_id: str
    timestamp: int
    seq: Optional[int] = None


class QueueStore(ABC):
    """Durable message queue store. ALL methods must be thread-safe: the broker
    calls them concurrently from one thread per client connection."""

    @abstractmethod
    def save_message(self, msg_id: str, topic: str, payload: Any, publisher_id: str,
                     timestamp: int, subscriber_ids: Iterable[str]) -> bool:
        """Persist a published message and one *pending* delivery row for each
        subscriber in `subscriber_ids`, atomically (single transaction).

        MUST be durable (committed) before returning: the broker sends PUBACK
        to the publisher right after this returns.

        Idempotent: if `msg_id` already exists (publisher retried after a lost
        PUBACK) do nothing and return False. Return True if newly stored.
        `subscriber_ids` may be empty (message is still stored)."""

    @abstractmethod
    def register_subscription(self, client_id: str, pattern: str) -> None:
        """Durably record that `client_id` subscribes to `pattern`. Idempotent."""

    @abstractmethod
    def remove_subscription(self, client_id: str, pattern: str) -> None:
        """Delete a subscription (UNSUBSCRIBE). No error if it does not exist.
        Already-queued deliveries are NOT removed."""

    @abstractmethod
    def load_subscriptions(self) -> Dict[str, Set[str]]:
        """Return {client_id: {pattern, ...}} for every durable subscription.
        Called once when the broker starts, so subscriptions survive restart."""

    @abstractmethod
    def get_pending(self, client_id: str) -> List[StoredMessage]:
        """Return every un-acked message for `client_id`, ordered by `seq`
        ascending (i.e. publish order). Must include `seq`."""

    @abstractmethod
    def mark_acked(self, client_id: str, msg_id: str) -> bool:
        """Mark the delivery (client_id, msg_id) as acknowledged so it is never
        returned by get_pending() again. Idempotent. Return True if a pending
        row changed, False if it was already acked or never existed."""

    def close(self) -> None:
        """Release resources (DB connections). Optional."""
