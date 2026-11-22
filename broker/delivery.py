"""
Delivery engine (J5) + reconnect catch-up (J6).

Gives at-least-once delivery to subscribers:

  * Storage is the source of truth. A message stays "pending" for a
    subscriber until that subscriber ACKs it (mark_acked). Nothing is ever
    dropped because a subscriber is slow, offline, or its ACK got lost.
  * Each ONLINE subscriber has a session with
      - an outbound queue (FIFO, publish order), and
      - an in-flight table: msg_id -> (message, last_sent, attempts).
    A dispatcher thread per session moves messages from outbound to the wire
    while fewer than `max_inflight` are un-acked (flow control window).
  * One retransmit thread scans in-flight tables; if no ACK arrives within
    ack_timeout (doubling per attempt, capped at 8x) the DELIVER is re-sent
    with attempt+1. Retries continue while the subscriber stays connected.
  * When a subscriber disconnects its session is discarded; its un-acked
    messages remain pending in storage.
  * Catch-up: on CONNECT with a known client_id, attach() loads get_pending()
    (ordered by seq) into the new session's outbound queue, so the backlog is
    flushed in publish order before / interleaved with new live messages.

The consequence of at-least-once is that duplicates are possible (e.g. the
ACK was lost, or the subscriber crashed after processing but before ACKing).
Subscribers de-duplicate by msg_id (Tortuya, T5); every DELIVER carries
`attempt` and `backlog` fields to make this visible in the demo.

Concurrency notes (Coulouris et al., Ch 7 - threads):
  lock order is  DeliveryEngine._lock  ->  session.cond . Socket writes are
  never done while holding either lock, so a slow subscriber cannot block
  publishers or other subscribers.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict, deque
from typing import Callable, Deque, Dict, List, Optional

from common.protocol import DELIVER, FrameStream
from storage.interface import QueueStore, StoredMessage

log = logging.getLogger("broker.delivery")


class _InFlight:
    __slots__ = ("msg", "sent_at", "attempts", "backlog")

    def __init__(self, msg: StoredMessage, backlog: bool):
        self.msg = msg
        self.sent_at = 0.0
        self.attempts = 0
        self.backlog = backlog


class SubscriberSession:
    """State for one connected subscriber. Guarded by `cond`."""

    def __init__(self, client_id: str, stream: FrameStream):
        self.client_id = client_id
        self.stream = stream
        self.cond = threading.Condition()
        self.outbound: Deque[_InFlight] = deque()
        self.inflight: "OrderedDict[str, _InFlight]" = OrderedDict()
        self.known_ids = set()          # msg_ids in outbound or inflight (dedup)
        self.active = True
        self.delivered = 0              # DELIVER frames sent (incl. retries)
        self.acked = 0
        self.thread: Optional[threading.Thread] = None


def deliver_frame(item: _InFlight) -> Dict:
    m = item.msg
    return {
        "type": DELIVER,
        "msg_id": m.msg_id,
        "topic": m.topic,
        "payload": m.payload,
        "publisher_id": m.publisher_id,
        "timestamp": m.timestamp,
        "attempt": item.attempts,       # 1 = first send in this session
        "backlog": item.backlog,        # True = queued while subscriber was offline
    }


class DeliveryEngine:
    def __init__(self, store: QueueStore, ack_timeout: float = 5.0, max_inflight: int = 32,
                 retransmit_interval: float = 0.2,
                 on_send_failure: Optional[Callable[[FrameStream], None]] = None) -> None:
        self.store = store
        self.ack_timeout = ack_timeout
        self.max_inflight = max(1, max_inflight)
        self.retransmit_interval = retransmit_interval
        self._on_send_failure = on_send_failure or (lambda stream: stream.close())
        self._lock = threading.Lock()
        self._sessions: Dict[str, SubscriberSession] = {}
        self._stop = threading.Event()
        self._retx_thread = threading.Thread(target=self._retransmit_loop,
                                             name="retransmit", daemon=True)
        self.retransmissions = 0

    # ------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self._retx_thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for s in sessions:
            self._deactivate(s)
        if self._retx_thread.is_alive():
            self._retx_thread.join(timeout=2)

    # ------------------------------------------------------------ sessions
    def attach(self, client_id: str, stream: FrameStream) -> int:
        """Subscriber (re)connected: create its session and load its backlog.
        Returns the number of pending (backlog) messages queued for delivery."""
        with self._lock:
            old = self._sessions.pop(client_id, None)
            # Read pending under the engine lock: a concurrent enqueue() for this
            # client waits, then de-duplicates against known_ids. So a message is
            # neither lost nor queued twice during the attach window.
            pending = self.store.get_pending(client_id)
            s = SubscriberSession(client_id, stream)
            for m in pending:
                s.outbound.append(_InFlight(m, backlog=True))
                s.known_ids.add(m.msg_id)
            self._sessions[client_id] = s
        if old is not None:
            self._deactivate(old)
        s.thread = threading.Thread(target=self._dispatch_loop, args=(s,),
                                    name=f"deliver-{client_id}", daemon=True)
        s.thread.start()
        if pending:
            log.info("CATCH-UP %s: %d pending message(s) queued in order (seq %s..%s)",
                     client_id, len(pending), pending[0].seq, pending[-1].seq)
        return len(pending)

    def detach(self, client_id: str, stream: FrameStream) -> None:
        """Subscriber disconnected. Only removes the session if it still belongs
        to `stream` (a newer connection may have taken over the client_id)."""
        with self._lock:
            s = self._sessions.get(client_id)
            if s is None or s.stream is not stream:
                return
            del self._sessions[client_id]
        self._deactivate(s)
        log.info("DETACH %s: %d in flight + %d queued remain pending in storage",
                 client_id, len(s.inflight), len(s.outbound))

    def is_online(self, client_id: str) -> bool:
        with self._lock:
            return client_id in self._sessions

    @staticmethod
    def _deactivate(s: SubscriberSession) -> None:
        with s.cond:
            s.active = False
            s.cond.notify_all()

    # ------------------------------------------------------------ messages
    def enqueue(self, client_id: str, msg: StoredMessage) -> bool:
        """Queue a newly published (already persisted) message for an online
        subscriber. Returns False if the subscriber is offline - the message
        then simply stays pending in storage until catch-up."""
        with self._lock:
            s = self._sessions.get(client_id)
            if s is None:
                return False
            with s.cond:
                if msg.msg_id not in s.known_ids:
                    s.known_ids.add(msg.msg_id)
                    s.outbound.append(_InFlight(msg, backlog=False))
                    s.cond.notify()
        return True

    def ack(self, client_id: str, msg_id: str) -> bool:
        """Subscriber ACKed: persist first, then forget the in-flight entry."""
        changed = self.store.mark_acked(client_id, msg_id)
        with self._lock:
            s = self._sessions.get(client_id)
        if s is not None:
            with s.cond:
                if s.inflight.pop(msg_id, None) is not None:
                    s.acked += 1
                elif msg_id in s.known_ids:
                    # ACK for a message still in outbound (e.g. late ACK from a
                    # previous session) - no need to send it again.
                    s.outbound = deque(i for i in s.outbound if i.msg.msg_id != msg_id)
                s.known_ids.discard(msg_id)
                s.cond.notify()                     # window space freed
        log.debug("ACK %s msg_id=%s (%s)", client_id, msg_id, "new" if changed else "dup/unknown")
        return changed

    # ------------------------------------------------------------ threads
    def _dispatch_loop(self, s: SubscriberSession) -> None:
        while True:
            with s.cond:
                while s.active and not self._stop.is_set() and (
                        not s.outbound or len(s.inflight) >= self.max_inflight):
                    s.cond.wait(0.5)
                if not s.active or self._stop.is_set():
                    return
                item = s.outbound.popleft()
                item.attempts = 1
                item.sent_at = time.monotonic()
                s.inflight[item.msg.msg_id] = item
                frame = deliver_frame(item)
            if not self._send(s, frame):
                return

    def _retransmit_loop(self) -> None:
        while not self._stop.wait(self.retransmit_interval):
            with self._lock:
                sessions = list(self._sessions.values())
            now = time.monotonic()
            for s in sessions:
                due: List[Dict] = []
                with s.cond:
                    if not s.active:
                        continue
                    for item in s.inflight.values():
                        timeout = self.ack_timeout * min(2 ** (item.attempts - 1), 8)
                        if now - item.sent_at >= timeout:
                            item.attempts += 1
                            item.sent_at = now
                            due.append(deliver_frame(item))
                for frame in due:
                    self.retransmissions += 1
                    log.info("RETRANSMIT %s msg_id=%s attempt=%d (no ACK within timeout)",
                             s.client_id, frame["msg_id"], frame["attempt"])
                    if not self._send(s, frame):
                        break

    def _send(self, s: SubscriberSession, frame: Dict) -> bool:
        try:
            s.stream.send(frame)
            s.delivered += 1
            log.debug("DELIVER %s msg_id=%s attempt=%d", s.client_id, frame["msg_id"], frame["attempt"])
            return True
        except OSError as exc:
            log.warning("send to %s failed (%s); closing connection", s.client_id, exc)
            self._deactivate(s)
            self._on_send_failure(s.stream)
            return False

    # ------------------------------------------------------------ introspection
    def stats(self) -> Dict[str, Dict[str, int]]:
        with self._lock:
            sessions = list(self._sessions.values())
        out = {}
        for s in sessions:
            with s.cond:
                out[s.client_id] = {"queued": len(s.outbound), "inflight": len(s.inflight),
                                    "delivered": s.delivered, "acked": s.acked}
        return out
