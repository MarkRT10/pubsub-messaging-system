"""Minimal raw-protocol client used by the broker tests.

Deliberately NOT the real publisher/subscriber clients (M3/M4): the broker
must be testable on its own against PROTOCOL.md alone.
"""
from __future__ import annotations

import queue
import socket
import threading
import time
from typing import Any, Dict, List, Optional

from common import protocol as P
from common.protocol import FrameStream


class RawClient:
    def __init__(self, port: int, client_id: str, role: str, auto_ack: bool = True):
        self.client_id = client_id
        self.role = role
        self.auto_ack = auto_ack
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.settimeout(None)
        self.stream = FrameStream(self.sock)
        self.inbox: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.delivered: List[Dict[str, Any]] = []
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        try:
            while True:
                f = self.stream.recv()
                if f is None:
                    break
                if f["type"] == P.DELIVER:
                    self.delivered.append(f)
                    if self.auto_ack:
                        self.send({"type": P.ACK, "msg_id": f["msg_id"]})
                self.inbox.put(f)
        except (OSError, P.ProtocolError):
            pass
        self.inbox.put({"type": "_CLOSED"})

    def send(self, frame: Dict[str, Any]) -> None:
        self.stream.send(frame)

    def send_raw(self, data: bytes) -> None:
        self.sock.sendall(data)

    def expect(self, ftype: str, timeout: float = 3.0) -> Dict[str, Any]:
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise AssertionError(f"{self.client_id}: timed out waiting for {ftype}")
            f = self.inbox.get(timeout=remaining)
            if f["type"] == ftype:
                return f
            if f["type"] == "_CLOSED":
                raise AssertionError(f"{self.client_id}: connection closed while waiting for {ftype}")

    def connect(self) -> Dict[str, Any]:
        self.send({"type": P.CONNECT, "client_id": self.client_id, "role": self.role, "version": 1})
        return self.expect(P.CONNACK)

    def subscribe(self, *topics: str) -> Dict[str, Any]:
        self.send({"type": P.SUBSCRIBE, "topics": list(topics)})
        return self.expect(P.SUBACK)

    def publish(self, topic: str, payload: Any, msg_id: Optional[str] = None) -> Dict[str, Any]:
        msg_id = msg_id or P.new_msg_id()
        self.send({"type": P.PUBLISH, "msg_id": msg_id, "topic": topic, "payload": payload,
                   "timestamp": P.now_ms()})
        ack = self.expect(P.PUBACK)
        assert ack["msg_id"] == msg_id
        return ack

    def wait_for(self, n: int, timeout: float = 5.0) -> List[Dict[str, Any]]:
        deadline = time.time() + timeout
        while len(self.delivered) < n and time.time() < deadline:
            time.sleep(0.01)
        return list(self.delivered)

    def kill(self) -> None:
        """Abrupt close (simulates a crashed client)."""
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    def close(self) -> None:
        try:
            self.send({"type": P.DISCONNECT})
        except OSError:
            pass
        self.stream.close()
