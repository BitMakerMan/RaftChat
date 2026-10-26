"""Deterministic core tests (election, replication, commit, compaction, membership).

Rule of the project: a passing test is never deleted or weakened. If one goes red,
the code is fixed, not the test.
"""

import unittest

from raft.core import CANDIDATE, FOLLOWER, LEADER, PRECANDIDATE
from tests.sim import SimCluster


class TestElection(unittest.TestCase):
    def test_single_node_elects_itself(self):
        c = SimCluster(["n1"])
        c.run(1000)
        self.assertEqual(c.nodes["n1"].role, LEADER)
        self.assertEqual(c.nodes["n1"].leader, "n1")

    def test_three_nodes_exactly_one_leader(self):
        c = SimCluster(["n1", "n2", "n3"], seed=11)
        c.run(1500)
        leaders = [n for n, core in c.nodes.items() if core.role == LEADER]
        self.assertEqual(len(leaders), 1)

    def test_prevote_does_not_inflate_term_in_partition(self):
        c = SimCluster(["n1", "n2", "n3"], seed=5)
        c.run(1500)
        first_leader = c.leader()
        others = [n for n in c.node_ids if n != first_leader]
        for o in others:
            c.cut(first_leader, o)
        c.run(c.now + 1200)
        new_leader = c.leader()
        self.assertIn(new_leader, others)
        # the isolated node keeps its stale term: pre-vote must not inflate it
        self.assertLessEqual(c.nodes[first_leader].term, c.nodes[new_leader].term)
        self.assertEqual(c.nodes[first_leader].stats["votes_won"], 1)
        for o in others:
            c.heal(first_leader, o)
        c.run(c.now + 1200)
        self.assertEqual(c.leader(), new_leader)
        self.assertEqual(c.nodes[first_leader].role, FOLLOWER)
        self.assertEqual(len([n for n in c.nodes if c.nodes[n].role == LEADER]), 1)

    def test_election_requires_real_majority(self):
        c = SimCluster(["n1", "n2", "n3"], seed=9)
        c.run(1200)
        leader = c.leader()
        c.kill("n2")
        c.kill("n3")
        c.kill(leader)
        lone = SimCluster(["x1"], seed=1)
        lone.run(1000)
        self.assertEqual(lone.leader(), "x1")


class TestReplication(unittest.TestCase):
    def test_command_commits_on_quorum(self):
        c = SimCluster(["n1", "n2", "n3"], seed=13)
        c.run(1500)
        leader = c.leader()
        reply = c.submit(leader, {"type": "submit", "msg_id": "m1", "author": "alice",
                                  "recipients": ["bob"], "text": "hello"}, "cmd1")
        self.assertTrue(reply["ok"])
        self.assertTrue(reply["committed"])
        for n, core in c.nodes.items():
            self.assertEqual(core.commit_index, reply["index"])
            self.assertIn("m1", core.sm.state["messages"])

    def test_no_commit_without_majority(self):
        c = SimCluster(["n1", "n2", "n3"], seed=17)
        c.run(1500)
        leader = c.leader()
        c.kill("n2")
        c.kill("n3")
        reply = c.submit(leader, {"type": "submit", "msg_id": "m1", "author": "alice",
                                  "text": "hi"}, "cmd1")
        self.assertFalse(reply["committed"])
        self.assertLess(c.nodes[leader].commit_index, reply["index"])
        self.assertEqual(c.nodes[leader].sm.state["applied"], {})

    def test_rejected_command_by_non_leader(self):
        c = SimCluster(["n1", "n2", "n3"], seed=23)
        c.run(1500)
        follower = [n for n in c.nodes if c.nodes[n].role != LEADER][0]
        reply = c.submit(follower, {"type": "submit", "msg_id": "m1", "author": "a", "text": "x"}, "c1")
        self.assertFalse(reply["ok"])
        self.assertEqual(reply.get("redirect"), c.leader())

    def test_client_retry_is_idempotent(self):
        c = SimCluster(["n1", "n2", "n3"], seed=29)
        c.run(1500)
        leader = c.leader()
        cmd = {"type": "submit", "msg_id": "m1", "author": "alice", "text": "hello"}
        r1 = c.submit(leader, cmd, "cmd-dup")
        r2 = c.submit(leader, cmd, "cmd-dup")
        self.assertEqual(r1["index"], r2["index"])
        self.assertTrue(r2.get("duplicate"))
        self.assertEqual(len(c.nodes[leader].sm.state["messages"]), 1)

    def test_conflicting_entry_is_truncated(self):
        c = SimCluster(["n1", "n2", "n3"], seed=31)
        c.run(1500)
        leader = c.leader()
        follower = [n for n in c.nodes if n != leader][0]
        core = c.nodes[follower]
        # a stale entry from a previous term at index 1 must be overwritten
        core.storage.append({"index": 1, "term": 99, "kind": "command",
                             "payload": {"type": "submit", "msg_id": "ghost"}, "command_id": None})
        reply = c.submit(leader, {"type": "submit", "msg_id": "m1", "author": "a", "text": "t"}, "c1")
        c.run(c.now + 400)
        self.assertTrue(reply["committed"])
        # the stale entry is gone and the follower prefix matches the leader's log
        self.assertEqual(core.storage.entry(1)["term"], c.nodes[leader].storage.entry(1)["term"])
        self.assertEqual(core.commit_index, c.nodes[leader].commit_index)
        self.assertIn("m1", core.sm.state["messages"])
        self.assertNotIn("ghost", core.sm.state["messages"])


class TestCompaction(unittest.TestCase):
    def test_snapshot_after_threshold(self):
        c = SimCluster(["n1", "n2", "n3"], seed=37, cfg={"snapshot_threshold": 4})
        c.run(1500)
        leader = c.leader()
        for i in range(10):
            c.submit(leader, {"type": "submit", "msg_id": f"m{i}", "author": "a", "text": "x"}, f"c{i}")
        for core in c.nodes.values():
            self.assertIsNotNone(core.storage.snapshot())
            self.assertLess(core.storage.log_size(), 10)

    def test_snapshot_install_on_lagging_node(self):
        c = SimCluster(["n1", "n2", "n3"], seed=41, cfg={"snapshot_threshold": 4})
        c.run(1500)
        leader = c.leader()
        for i in range(8):
            c.submit(leader, {"type": "submit", "msg_id": f"m{i}", "author": "a", "text": "x"}, f"c{i}")
        follower = [n for n in c.nodes if n != leader][0]
        core = c.nodes[follower]
        # force the follower back to an empty log: the leader must send a snapshot
        core.storage = type(core.storage)()
        core.storage.set_meta("current_term", core.term)
        core.sm.load({"configs": [], "messages": {}, "proposals": {}, "applied": {}, "history": []})
        core.last_applied = 0
        core.commit_index = 0
        c.run(c.now + 800)
        self.assertTrue(core.storage.snapshot())
        self.assertEqual(sorted(core.sm.state["messages"]), [f"m{i}" for i in range(8)])
        self.assertGreater(core.stats["snapshots_installed"], 0)


class TestMembership(unittest.TestCase):
    def test_joint_consensus_requires_both_configs(self):
        c = SimCluster(["n1", "n2", "n3"], seed=43)
        c.run(1500)
        leader = c.leader()
        # the new config majority cannot be reached: only the leader is alive in it
        reply = c.change_membership(leader, [leader, "n4", "n5"], joint=True)
        self.assertTrue(reply["ok"])
        cfgs = c.nodes[leader].configs
        self.assertTrue(cfgs[-1]["joint"])
        self.assertEqual(sorted(cfgs[-1]["cnew"]), sorted([leader, "n4", "n5"]))
        self.assertFalse(reply["committed"])
        self.assertEqual(sorted(c.nodes[leader].config_members()),
                         sorted(["n1", "n2", "n3", "n4", "n5"]))  # joint = union of both configs
        # once the new members join, the change can commit
        for n in ("n4", "n5"):
            c.join(n, initial_config=[leader, "n4", "n5", "n1", "n2", "n3"])
        c.run(c.now + 400)
        c.change_membership(leader, sorted([leader, "n4", "n5"]), joint=True)
        self.assertTrue(c.nodes[leader].commit_status(
            f"mc-{leader}-{'-'.join(sorted([leader, 'n4', 'n5']))}")["committed"])

    def test_join_completes_once_new_node_replicates(self):
        c = SimCluster(["n1", "n2", "n3"], seed=47)
        c.run(1500)
        leader = c.leader()
        core4 = c.join("n4", initial_config=["n1", "n2", "n3", "n4"])
        c.change_membership(leader, ["n1", "n2", "n3", "n4"], joint=True)
        c.run(c.now + 1200)
        self.assertIn("n4", c.nodes[leader].config_members())
        self.assertEqual(c.leader(), leader)
        # n4 is now part of the new config and must be able to accept entries
        reply = c.submit(leader, {"type": "submit", "msg_id": "m1", "author": "a", "text": "t"}, "c1")
        c.run(c.now + 300)
        self.assertTrue(reply["committed"])
        self.assertIn("m1", core4.sm.state["messages"])

    def test_leave_shrinks_config_and_commit_follows(self):
        c = SimCluster(["n1", "n2", "n3", "n4"], seed=53,
                       initial_config=["n1", "n2", "n3", "n4"])
        c.run(1500)
        leader = c.leader()
        c.submit(leader, {"type": "submit", "msg_id": "m1", "author": "a", "text": "t"}, "c1")
        victim = [n for n in c.node_ids if n != leader][0]
        remaining = [n for n in c.node_ids if n != victim]
        reply = c.change_membership(leader, remaining, joint=False)
        self.assertTrue(reply["committed"])
        self.assertEqual(sorted(c.nodes[leader].config_members()), sorted(remaining))
        self.assertEqual(c.leader(), leader)
        reply2 = c.submit(leader, {"type": "submit", "msg_id": "m2", "author": "a", "text": "t"}, "c2")
        self.assertTrue(reply2["committed"])
        # the removed node is no part of a quorum: it must be cut off from voting
        for n in remaining:
            self.assertIn(n, c.nodes[leader].config_members())
        self.assertNotIn(victim, c.nodes[leader].config_members())


class TestReads(unittest.TestCase):
    def test_leader_lease_and_read(self):
        c = SimCluster(["n1", "n2", "n3"], seed=61)
        c.run(1500)
        leader = c.leader()
        state = c.nodes[leader].read_state()
        self.assertTrue(state["lease_valid"])
        self.assertEqual(state["role"], LEADER)
        follower = [n for n in c.nodes if n != leader][0]
        self.assertFalse(c.nodes[follower].read_state()["lease_valid"])

    def test_check_confirms_leader(self):
        c = SimCluster(["n1", "n2", "n3"], seed=67)
        c.run(1500)
        leader = c.leader()
        follower = [n for n in c.nodes if n != leader][0]
        resp = c.nodes[follower].handle({"from": leader, "kind": "check",
                                         "payload": {"term": c.nodes[leader].term, "addr": "sim://x"}})
        self.assertFalse(resp[0]["payload"]["ok"])
        resp2 = c.nodes[leader].handle({"from": follower, "kind": "check",
                                        "payload": {"term": c.nodes[leader].term}})
        self.assertTrue(resp2[0]["payload"]["ok"])


if __name__ == "__main__":
    unittest.main()
