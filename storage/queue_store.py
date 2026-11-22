from __future__ import annotations

import json
import sqlite3
import threading
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Set, Tuple
from storage.interface import QueueStore, StoredMessage

class SQLiteQueueStore(QueueStore):
    def __init__(self, db_path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._conn.execute("PRAGMA journal_mode = WAL;")

        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS messages (
                     seq INTEGER PRIMARY KEY AUTOINCREMENT,
                     msg_id TEXT UNIQUE NOT NULL,
                     topic TEXT NOT NULL,
                     payload TEXT NOT NULL,
                     publisher_id TEXT NOT NULL,
                     timestamp INTEGER NOT NULL
                     );
                CREATE TABLE IF NOT EXISTS subscriptions (
                client_id TEXT NOT NULL, 
                pattern TEXT NOT NULL, 
                PRIMARY KEY (client_id, pattern)
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                client_id TEXT NOT NULL,
                msg_id TEXT NOT NULL, 
                acked INTEGER NOT NULL DEFAULT 0, 
                PRIMARY KEY (client_id, msg_id), 
                FOREIGN KEY (msg_id) REFERENCES messages(msg_id)
                );
                CREATE INDEX IF NOT EXISTS idx_deliveries_pending ON deliveries(client_id, acked);
            """)

    def save_message(self, msg_id: str, topic: str, payload: Any, publisher_id: str, timestamp: int, subscriber_ids: Iterable[str]) -> bool:
        payload_json = json.dumps(payload)

        with self._lock:
            with self._conn:
                cur = self._conn.execute("INSERT OR IGNORE INTO messages (msg_id, topic, payload, publisher_id, timestamp) VALUES (?, ?, ?, ?, ?)",(msg_id, topic, payload_json, publisher_id, timestamp))
                
                if cur.rowcount == 0:
                    return False
                self._conn.executemany("INSERT OR IGNORE INTO deliveries (client_id, msg_id, acked) VALUES (?, ?, 0)",[(cid, msg_id) for cid in subscriber_ids])
                return True
                
    def register_subscription(self, client_id: str, pattern: str) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute("INSERT OR IGNORE INTO subscriptions (client_id, pattern) VALUES (?, ?)",(client_id, pattern))

    def remove_subscription(self, client_id: str, pattern: str) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute("DELETE FROM subscriptions WHERE client_id = ? AND pattern = ?",(client_id, pattern))

    def load_subscriptions(self) -> Dict[str, Set[str]]:
        subs: Dict[str, Set[str]] = defaultdict(set)

        with self._lock:
            rows = self._conn.execute("SELECT client_id, pattern FROM subscriptions").fetchall()

            for client_id, pattern in rows:
                subs[client_id].add(pattern)

        return dict(subs)

    def get_pending(self, client_id: str) -> List[StoredMessage]:
        with self._lock:
            rows = self._conn.execute("""
            SELECT m.msg_id, m.topic, m.payload, m.publisher_id, m.timestamp, m.seq
            FROM messages m
            JOIN deliveries d ON m.msg_id = d.msg_id
            WHERE d.client_id = ? AND d.acked = 0
            ORDER BY m.seq ASC
            """, (client_id,)
            ).fetchall()

        result: List[StoredMessage] = []
        for msg_id, topic, payload_json, publisher_id, timestamp, seq in rows:
            result.append(StoredMessage(msg_id, topic, json.loads(payload_json), publisher_id, timestamp, seq))
        return result

    def mark_acked(self, client_id: str, msg_id: str) -> bool:
        with self._lock:
            with self._conn:
                cur = self._conn.execute("UPDATE deliveries SET acked = 1 WHERE client_id = ? AND msg_id = ? AND acked = 0",(client_id, msg_id))
                return cur.rowcount > 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()
            