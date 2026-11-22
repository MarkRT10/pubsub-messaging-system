import os, tempfile, unittest
from storage.queue_store import SQLiteQueueStore

class TestSQLiteQueueStore(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "test_queue.db")
        self.store = SQLiteQueueStore(self.db_path)
    def tearDown(self):
        self.store.close()
        self._tmp.cleanup()

    def test_ordering(self):
        self.store.save_message("m1", "sensors/temp", {"v": 1}, "pub1", 100, ["sub1"])
        self.store.save_message("m2", "sensors/temp", {"v": 2}, "pub1", 200, ["sub1"])
        self.store.save_message("m3", "sensors/temp", {"v": 3}, "pub1", 300, ["sub1"])

        pending = self.store.get_pending("sub1")
        self.assertEqual([m.msg_id for m in pending], ["m1", "m2", "m3"])
        self.assertTrue(pending[0].seq < pending[1].seq < pending[2].seq)

    def test_ack_removal(self):
        self.store.save_message("m1", "sensors/temp", {"v": 1}, "pub1", 100, ["sub1", "sub2"])
        self.store.save_message("m2", "sensors/temp", {"v": 2}, "pub1", 200, ["sub1", "sub2"])
        self.assertTrue(self.store.mark_acked("sub1", "m1"))
        self.assertFalse(self.store.mark_acked("sub1", "m1"))   
        self.assertEqual([m.msg_id for m in self.store.get_pending("sub1")], ["m2"])    
        self.assertEqual([m.msg_id for m in self.store.get_pending("sub2")], ["m1", "m2"])

    def test_restart_survival(self):   
        self.store.register_subscription("sub1", "sensors/#")
        self.store.save_message("m1", "sensors/temp", {"v": 1}, "pub1", 100, ["sub1"])
        self.store.save_message("m2", "sensors/temp", {"v": 2}, "pub1", 200, ["sub1"])
        self.store.mark_acked("sub1", "m1")
        self.store.close()
        self.store = SQLiteQueueStore(self.db_path)
        self.assertEqual(self.store.load_subscriptions(), {"sub1": {"sensors/#"}})
        self.assertEqual([m.msg_id for m in self.store.get_pending("sub1")], ["m2"])

if __name__ == "__main__":
    unittest.main()