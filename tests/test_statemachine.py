"""Chat state machine + application-level quorum decision tests.

Deterministic: entries are applied in commit order, so a decision becomes
effective exactly when the quorum is reached in the log, identically on any node.
"""

import unittest

from raft.statemachine import ChatStateMachine, TOMBSTONE
from raft.storage import entry as mk_entry

CFG3 = [{"cold": ["n1", "n2", "n3"], "cnew": ["n1", "n2", "n3"], "joint": False, "index": 0}]


def sm3():
    sm = ChatStateMachine("n1")
    sm.load({"configs": CFG3, "messages": {}, "proposals": {}, "applied": {}, "history": []})
    return sm


def cmd(sm, index, payload, cid=None):
    return sm.apply(mk_entry(index, 1, "command", payload, cid))


class TestStateMachine(unittest.TestCase):
    def test_submit_and_direct_transitions(self):
        sm = sm3()
        r = cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                        "recipients": ["bob"], "text": "hi"}, "c1")
        self.assertTrue(r["ok"])
        self.assertEqual(r["state"], "committed")
        # read is only reachable after the cluster decided delivery
        r_bad = cmd(sm, 2, {"type": "transition", "msg_id": "m1", "to": "read", "actor": "bob"}, "c2")
        self.assertFalse(r_bad["ok"])
        cmd(sm, 3, {"type": "propose", "proposal_id": "p1", "msg_id": "m1",
                    "by": "alice", "to": "delivered"}, "c3")
        cmd(sm, 4, {"type": "vote", "proposal_id": "p1", "voter": "n1"}, "c4")
        cmd(sm, 5, {"type": "vote", "proposal_id": "p1", "voter": "n3"}, "c5")
        self.assertEqual(sm.state["messages"]["m1"]["state"], "delivered")
        r2 = cmd(sm, 6, {"type": "transition", "msg_id": "m1", "to": "read", "actor": "bob"}, "c6")
        self.assertTrue(r2["ok"])
        self.assertEqual(r2["state"], "read")
        self.assertEqual(sm.state["messages"]["m1"]["transitions"],
                         [{"index": 1, "to": "committed", "by": "alice", "via": "direct"},
                          {"index": 5, "to": "delivered", "by": "alice", "via": "quorum"},
                          {"index": 6, "to": "read", "by": "bob", "via": "direct"}])

    def test_read_by_non_recipient_rejected(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        r = cmd(sm, 2, {"type": "transition", "msg_id": "m1", "to": "read", "actor": "carol"}, "c2")
        self.assertFalse(r["ok"])
        self.assertEqual(sm.state["messages"]["m1"]["state"], "committed")

    def test_edit_only_by_author(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "v1"}, "c1")
        self.assertTrue(cmd(sm, 2, {"type": "transition", "msg_id": "m1", "to": "edit",
                                    "actor": "alice", "text": "v2"}, "c2")["ok"])
        self.assertFalse(cmd(sm, 3, {"type": "transition", "msg_id": "m1", "to": "edit",
                                     "actor": "bob", "text": "v3"}, "c3")["ok"])
        self.assertEqual(sm.state["messages"]["m1"]["revisions"], ["v1", "v2"])
        self.assertEqual(sm.state["messages"]["m1"]["state"], "committed")

    def test_contested_transition_needs_a_proposal(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        r = cmd(sm, 2, {"type": "transition", "msg_id": "m1", "to": "delivered", "actor": "alice"}, "c2")
        self.assertFalse(r["ok"])
        self.assertIn("quorum", r["error"])

    def test_author_can_tombstone_directly(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        r = cmd(sm, 2, {"type": "transition", "msg_id": "m1", "to": "delete", "actor": "alice"}, "c2")
        self.assertTrue(r["ok"])
        self.assertEqual(sm.state["messages"]["m1"]["state"], TOMBSTONE)
        self.assertTrue(sm.state["messages"]["m1"]["tombstone"])

    def test_decision_applies_at_quorum(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        cmd(sm, 2, {"type": "propose", "proposal_id": "p1", "msg_id": "m1",
                    "by": "alice", "to": "flagged"}, "c2")
        r1 = cmd(sm, 3, {"type": "vote", "proposal_id": "p1", "voter": "n1"}, "c3")
        self.assertFalse(r1["decided"])
        r2 = cmd(sm, 4, {"type": "vote", "proposal_id": "p1", "voter": "n2"}, "c2b")
        self.assertTrue(r2["decided"])
        self.assertEqual(sm.state["messages"]["m1"]["state"], "flagged")
        # a third vote must not change anything (already decided)
        r3 = cmd(sm, 5, {"type": "vote", "proposal_id": "p1", "voter": "n3"}, "c4")
        self.assertTrue(r3["decided"])
        self.assertEqual(sm.state["messages"]["m1"]["state"], "flagged")

    def test_decision_needs_quorum_of_every_config_in_joint_view(self):
        sm = sm3()
        sm.load({"configs": [{"cold": ["n1", "n2", "n3"], "cnew": ["n1", "n4", "n5"],
                              "joint": True, "index": 0}]})
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        cmd(sm, 2, {"type": "propose", "proposal_id": "p1", "msg_id": "m1",
                    "by": "alice", "to": "drop"}, "c2")
        # old config majority reached, new config majority not reached yet
        cmd(sm, 3, {"type": "vote", "proposal_id": "p1", "voter": "n1"}, "c3")
        self.assertFalse(sm.state["proposals"]["p1"]["decided"])
        cmd(sm, 4, {"type": "vote", "proposal_id": "p1", "voter": "n2"}, "c4")
        self.assertFalse(sm.state["proposals"]["p1"]["decided"])  # old set is full, new set is not
        cmd(sm, 5, {"type": "vote", "proposal_id": "p1", "voter": "n4"}, "c5")
        self.assertTrue(sm.state["proposals"]["p1"]["decided"])
        self.assertEqual(sm.state["proposals"]["p1"]["outcome"], "accepted")
        self.assertEqual(sm.state["messages"]["m1"]["state"], TOMBSTONE)

    def test_vote_from_non_member_rejected(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        cmd(sm, 2, {"type": "propose", "proposal_id": "p1", "msg_id": "m1",
                    "by": "alice", "to": "flagged"}, "c2")
        r = cmd(sm, 3, {"type": "vote", "proposal_id": "p1", "voter": "n9"}, "c3")
        self.assertFalse(r["ok"])
        self.assertIn("not a member", r["error"])

    def test_decision_with_illegal_transition_is_rejected(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        cmd(sm, 2, {"type": "propose", "proposal_id": "p1", "msg_id": "m1",
                    "by": "alice", "to": "ack"}, "c2")
        for i, v in enumerate(["n1", "n2"]):
            cmd(sm, 3 + i, {"type": "vote", "proposal_id": "p1", "voter": v}, f"v{i}")
        self.assertTrue(sm.state["proposals"]["p1"]["decided"])
        self.assertEqual(sm.state["proposals"]["p1"]["outcome"], "rejected")
        self.assertEqual(sm.state["messages"]["m1"]["state"], "committed")

    def test_membership_apply_is_not_a_duplicate_of_the_mirror(self):
        sm = ChatStateMachine("n1")
        base = {"cold": ["n1"], "cnew": ["n1"], "joint": False, "index": 0}
        entry = mk_entry(1, 1, "membership", {"cold": ["n1"], "cnew": ["n1", "n2"], "joint": True})
        # the core mirrors configs before the state machine applies the entry
        sm.load_configs([base, dict(entry["payload"], index=2, entry_index=1)])
        r = sm.apply(entry)
        self.assertTrue(r["ok"], r)
        self.assertFalse(r.get("duplicate"), f"first application must not be a duplicate: {r}")
        self.assertEqual(sorted(r["members"]), ["n1", "n2"])
        # a genuinely repeated change (another entry, same config) is a duplicate
        r2 = sm.apply(mk_entry(2, 1, "membership", {"cold": ["n1"], "cnew": ["n1", "n2"], "joint": True}))
        self.assertTrue(r2.get("duplicate"), r2)

    def test_replay_is_idempotent(self):
        sm = sm3()
        entries = [
            mk_entry(1, 1, "command", {"type": "submit", "msg_id": "m1", "author": "alice",
                                       "recipients": ["bob"], "text": "hi"}, "c1"),
            mk_entry(2, 1, "membership", {"cold": ["n1", "n2", "n3"], "cnew": ["n1", "n2", "n3"],
                                          "joint": False}, "c2"),
        ]
        for e in entries:
            sm.apply(e)
        snapshot = sm.to_dict()
        for e in entries:
            sm.apply(e)
        self.assertEqual(snapshot, sm.to_dict())
        other = ChatStateMachine("n2")
        other.load(snapshot)
        self.assertEqual(other.to_dict(), snapshot)

    def test_invalid_state_jump_rejected(self):
        sm = sm3()
        cmd(sm, 1, {"type": "submit", "msg_id": "m1", "author": "alice",
                    "recipients": ["bob"], "text": "hi"}, "c1")
        r = cmd(sm, 2, {"type": "transition", "msg_id": "m1", "to": "resolved", "actor": "alice"}, "c2")
        # resolved is contested AND not reachable from committed
        self.assertFalse(r["ok"])


if __name__ == "__main__":
    unittest.main()
