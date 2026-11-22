"""
Contract tests for storage.interface.QueueStore.

Every implementation must pass these. They run against the in-memory
reference store, and automatically also against Labrador's
storage.queue_store.SQLiteQueueStore once that module exists.

    python -m unittest tests.test_storage_contract -v
"""
import os
import tempfile
import threading
import unittest

from storage.memory_store import MemoryQueueStore

try:
    from storage.queue_store import SQLiteQueueStore     # M2 (Labrador)
except ImportError:                                      # not merged yet
    SQLiteQueueStore = None


class StoreContract:
    """Mixin: subclasses provide make_store()."""

    def make_store(self):
        raise NotImplementedError

    def setUp(self):
        self.store = self.make_store()

    def tearDown(self):
        self.store.close()

    def test_save_and_get_pending_in_order(self):
        for i in range(5):
            self.assertTrue(self.store.save_message(f"m{i}", "stocks.tech", {"i": i}, "pubA", 1000 + i, ["s1"]))
        pending = self.store.get_pending("s1")
        self.assertEqual([m.msg_id for m in pending], [f"m{i}" for i in range(5)])
        self.assertEqual([m.payload for m in pending], [{"i": i} for i in range(5)])
        self.assertTrue(all(m.seq is not None for m in pending))
        self.assertEqual(pending[0].topic, "stocks.tech")
        self.assertEqual(pending[0].publisher_id, "pubA")
        self.assertEqual(pending[0].timestamp, 1000)

    def test_only_recipients_get_message(self):
        self.store.save_message("m1", "crypto", 1, "pubB", 1, ["s2", "s3"])
        self.assertEqual(self.store.get_pending("s1"), [])
        self.assertEqual(len(self.store.get_pending("s2")), 1)
        self.assertEqual(len(self.store.get_pending("s3")), 1)

    def test_save_is_idempotent(self):
        self.assertTrue(self.store.save_message("m1", "crypto", 1, "pubB", 1, ["s3"]))
        self.assertFalse(self.store.save_message("m1", "crypto", 1, "pubB", 1, ["s3"]))
        self.assertEqual(len(self.store.get_pending("s3")), 1)

    def test_mark_acked_removes_only_that_subscriber(self):
        self.store.save_message("m1", "crypto", 1, "pubB", 1, ["s2", "s3"])
        self.assertTrue(self.store.mark_acked("s2", "m1"))
        self.assertFalse(self.store.mark_acked("s2", "m1"))          # idempotent
        self.assertFalse(self.store.mark_acked("s2", "nope"))        # unknown
        self.assertEqual(self.store.get_pending("s2"), [])
        self.assertEqual(len(self.store.get_pending("s3")), 1)

    def test_ack_out_of_order_keeps_rest_ordered(self):
        for i in range(4):
            self.store.save_message(f"m{i}", "t", i, "p", i, ["s"])
        self.store.mark_acked("s", "m1")
        self.assertEqual([m.msg_id for m in self.store.get_pending("s")], ["m0", "m2", "m3"])

    def test_subscriptions(self):
        self.store.register_subscription("s2", "stocks.banking")
        self.store.register_subscription("s2", "crypto")
        self.store.register_subscription("s2", "crypto")              # idempotent
        self.store.register_subscription("s1", "stocks.tech")
        self.assertEqual(self.store.load_subscriptions(),
                         {"s2": {"stocks.banking", "crypto"}, "s1": {"stocks.tech"}})
        self.store.remove_subscription("s2", "crypto")
        self.store.remove_subscription("s2", "never-subscribed")
        self.assertEqual(self.store.load_subscriptions()["s2"], {"stocks.banking"})

    def test_message_with_no_subscribers(self):
        self.assertTrue(self.store.save_message("m1", "nobody.listens", None, "p", 1, []))

    def test_concurrent_writers(self):
        def worker(w):
            for i in range(50):
                self.store.save_message(f"w{w}-{i}", "t", i, f"p{w}", i, ["s"])
        threads = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        pending = self.store.get_pending("s")
        self.assertEqual(len(pending), 200)
        self.assertEqual(len({m.msg_id for m in pending}), 200)
        seqs = [m.seq for m in pending]
        self.assertEqual(seqs, sorted(seqs))


class MemoryStoreContract(StoreContract, unittest.TestCase):
    def make_store(self):
        return MemoryQueueStore()


@unittest.skipIf(SQLiteQueueStore is None, "storage/queue_store.py (M2) not merged yet")
class SQLiteStoreContract(StoreContract, unittest.TestCase):
    def make_store(self):
        self._dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._dir.name, "test.db")
        return SQLiteQueueStore(self.db_path)

    def tearDown(self):
        super().tearDown()
        self._dir.cleanup()

    def test_survives_restart(self):
        self.store.register_subscription("s2", "crypto")
        self.store.save_message("m1", "crypto", {"p": 1}, "pubB", 1, ["s2"])
        self.store.close()
        self.store = SQLiteQueueStore(self.db_path)
        self.assertEqual(self.store.load_subscriptions(), {"s2": {"crypto"}})
        self.assertEqual([m.msg_id for m in self.store.get_pending("s2")], ["m1"])


if __name__ == "__main__":
    unittest.main()
