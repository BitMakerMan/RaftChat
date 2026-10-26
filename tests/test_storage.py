"""Disk storage tests: durability, recovery, compaction.

These tests write to a temporary directory and clean up after themselves.
"""

import shutil
import tempfile
import unittest

from raft.core import LEADER
from raft.statemachine import ChatStateMachine
from raft.storage import SqliteStorage
from tests.sim import SimCluster


class TestSqliteStorage(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="raft-test-")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_roundtrip_and_persistence(self):
        st = SqliteStorage(f"{self.dir}/n1.db", "n1")
        st.set_meta("current_term", 3)
        st.append({"index": 1, "term": 3, "kind": "command",
                   "payload": {"type": "submit", "msg_id": "m1"}, "command_id": "c1"})
        st.save_snapshot(1, 3, {"configs": [], "messages": {}, "proposals": {},
                                "applied": {}, "history": []})
        st.set_meta("commit_index", 1)
        st.close()

        st2 = SqliteStorage(f"{self.dir}/n1.db", "n1")
        self.assertEqual(st2.meta("current_term"), 3)
        self.assertEqual(st2.meta("commit_index"), 1)
        self.assertIsNotNone(st2.snapshot())
        self.assertEqual(st2.snapshot()["index"], 1)
        st2.close()

    def test_node_recovers_state_from_disk(self):
        c = SimCluster(["n1"], cfg={"snapshot_threshold": 1000})
        core = c.nodes["n1"]
        # replace the in-memory storage with a real file-backed one
        st = SqliteStorage(f"{self.dir}/n1.db", "n1")
        core.storage = st
        c.run(500)
        self.assertEqual(core.role, LEADER)
        for i in range(6):
            c.submit("n1", {"type": "submit", "msg_id": f"m{i}", "author": "a",
                            "text": "x"}, f"c{i}")
        self.assertEqual(len(core.sm.state["messages"]), 6)
        st.close()

        # reopen exactly as a restarted process would
        st2 = SqliteStorage(f"{self.dir}/n1.db", "n1")
        recovered = ChatStateMachine("n1")
        from raft.core import RaftCore
        core2 = RaftCore("n1", st2, recovered, clock=c._clock,
                         initial_config=["n1"], cfg=c.cfg, addr="sim://n1")
        self.assertEqual(sorted(recovered.state["messages"]),
                         sorted(core.sm.state["messages"]))
        self.assertEqual(core2.commit_index, core.commit_index)
        self.assertEqual(core2.term, core.term)
        st2.close()

    def test_compaction_on_disk_keeps_state_readable(self):
        c = SimCluster(["n1"], cfg={"snapshot_threshold": 5})
        core = c.nodes["n1"]
        core.storage = SqliteStorage(f"{self.dir}/n2.db", "n2")
        c.run(500)
        for i in range(12):
            c.submit("n1", {"type": "submit", "msg_id": f"m{i}", "author": "a",
                            "text": "x"}, f"c{i}")
        snap = core.storage.snapshot()
        self.assertIsNotNone(snap)
        self.assertLess(core.storage.log_size(), 12)
        self.assertEqual(len(core.sm.state["messages"]), 12)
        # snapshot + remaining log must together cover the whole state
        covered = len(snap["data"]["messages"]) + len(core.storage.entries_from(snap["index"] + 1))
        self.assertEqual(covered, 12)
        core.storage.close()


if __name__ == "__main__":
    unittest.main()
