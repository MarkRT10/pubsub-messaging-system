"""
Subscription registry + topic routing engine (J4).

Topic-based filtering (Coulouris et al., Sec 6.3.1): every published message
carries one topic string; a subscription is a topic *pattern*. The broker
delivers a message to every subscriber with at least one matching pattern,
and to nobody else (no cross-topic leakage).

Topic syntax: dot-separated segments, e.g. "stocks.tech".
Pattern syntax: a topic, where "*" matches exactly one segment and "#"
(last segment only) matches zero or more trailing segments.

    stocks.tech   matches  stocks.tech
    stocks.*      matches  stocks.tech, stocks.banking   (not stocks, stocks.tech.us)
    stocks.#      matches  stocks, stocks.tech, stocks.tech.us
"""
from __future__ import annotations

import logging
import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Set, Tuple

from common.protocol import validate_pattern, validate_topic
from storage.interface import QueueStore, StoredMessage

if TYPE_CHECKING:                                  # pragma: no cover
    from broker.delivery import DeliveryEngine

log = logging.getLogger("broker.router")


def topic_matches(pattern: str, topic: str) -> bool:
    """True if subscription `pattern` matches concrete `topic`."""
    p_segs = pattern.split(".")
    t_segs = topic.split(".")
    for i, seg in enumerate(p_segs):
        if seg == "#":
            return True                            # matches the rest (incl. nothing)
        if i >= len(t_segs):
            return False
        if seg != "*" and seg != t_segs[i]:
            return False
    return len(p_segs) == len(t_segs)


class SubscriptionRegistry:
    """Thread-safe in-memory map client_id -> set of patterns.

    The durable copy lives in storage; this is the fast lookup used on every
    PUBLISH. A reader-heavy workload, but a single lock keeps it simple and
    correct; matching is O(#subscriptions), which is fine at our scale."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._subs: Dict[str, Set[str]] = defaultdict(set)

    def load(self, subs: Dict[str, Set[str]]) -> None:
        with self._lock:
            for cid, patterns in subs.items():
                self._subs[cid].update(patterns)

    def add(self, client_id: str, pattern: str) -> bool:
        with self._lock:
            new = pattern not in self._subs[client_id]
            self._subs[client_id].add(pattern)
            return new

    def remove(self, client_id: str, pattern: str) -> bool:
        with self._lock:
            patterns = self._subs.get(client_id)
            if not patterns or pattern not in patterns:
                return False
            patterns.discard(pattern)
            if not patterns:
                del self._subs[client_id]
            return True

    def patterns_for(self, client_id: str) -> List[str]:
        with self._lock:
            return sorted(self._subs.get(client_id, ()))

    def match(self, topic: str) -> List[str]:
        """Client ids with >= 1 pattern matching `topic` (each id once)."""
        with self._lock:
            return sorted(cid for cid, patterns in self._subs.items()
                          if any(topic_matches(p, topic) for p in patterns))

    def snapshot(self) -> Dict[str, List[str]]:
        with self._lock:
            return {cid: sorted(p) for cid, p in self._subs.items()}


class Router:
    """Glues registry, durable storage and the delivery engine together."""

    def __init__(self, registry: SubscriptionRegistry, store: QueueStore,
                 delivery: "DeliveryEngine") -> None:
        self.registry = registry
        self.store = store
        self.delivery = delivery
        # Serialises "match + save" so a message is never routed against a
        # half-applied subscription change (see subscribe()).
        self._route_lock = threading.Lock()

    def subscribe(self, client_id: str, patterns: Iterable[Any]) -> List[str]:
        patterns = [validate_pattern(p) for p in patterns]
        with self._route_lock:
            for p in patterns:
                self.store.register_subscription(client_id, p)   # durable first
                if self.registry.add(client_id, p):
                    log.info("SUBSCRIBE %s -> %s", client_id, p)
        return patterns

    def unsubscribe(self, client_id: str, patterns: Iterable[Any]) -> List[str]:
        patterns = [validate_pattern(p) for p in patterns]
        with self._route_lock:
            for p in patterns:
                self.store.remove_subscription(client_id, p)
                if self.registry.remove(client_id, p):
                    log.info("UNSUBSCRIBE %s -x %s", client_id, p)
        return patterns

    def publish(self, publisher_id: str, msg_id: str, topic: str, payload: Any,
                timestamp: int) -> Tuple[bool, List[str]]:
        """Route one message. Returns (is_new, recipients).

        Order matters for at-least-once delivery:
          1. match subscribers       (routing)
          2. persist message + one pending row per recipient  (durability)
          3. caller sends PUBACK     (only after step 2 committed)
          4. hand to delivery engine for online subscribers; offline ones get
             it from storage on reconnect (catch-up)."""
        validate_topic(topic)
        with self._route_lock:
            recipients = self.registry.match(topic)
            is_new = self.store.save_message(msg_id, topic, payload, publisher_id,
                                             timestamp, recipients)
        if not is_new:
            log.info("PUBLISH %s dup msg_id=%s (already stored, not re-routed)", publisher_id, msg_id)
            return False, recipients
        log.info("PUBLISH %s [%s] msg_id=%s -> %s", publisher_id, topic, msg_id,
                 ", ".join(recipients) or "(no subscribers)")
        msg = StoredMessage(msg_id, topic, payload, publisher_id, timestamp)
        for cid in recipients:
            self.delivery.enqueue(cid, msg)
        return True, recipients
