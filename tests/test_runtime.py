"""Integration tests over real HTTP (localhost, OS-assigned ports).

Deterministic inputs (seeded rng, injected clock), but real transport and real
asyncio event loop.
"""

import asyncio
import random
import unittest

from raft import net
from raft.runtime import NodeRuntime
from raft.statemachine import ChatStateMachine
from raft.storage import MemoryStorage

CFG = {"heartbeat_ms": 50, "election_min_ms": 150, "election_max_ms": 400,
       "snapshot_threshold": 64, "max_batch": 8}


def majority(n):
    return n // 2 + 1


def msg_of(read, msg_id):
    return {m["id"]: m for m in read["state"]["messages"]}[msg_id]


class TestRuntime(unittest.TestCase):
    def _cluster(self, ids):
        return [NodeRuntime(nid, MemoryStorage(), ChatStateMachine(nid),
                            initial_config=list(ids), cfg=CFG,
                            rng=random.Random(i + 1)) for i, nid in enumerate(ids)]

    async def _start(self, ids):
        nodes = self._cluster(ids)
        ports = await asyncio.gather(*[n.start() for n in nodes])
        addrs = {nid: net.base_url("127.0.0.1", p) for nid, p in zip(ids, ports)}
        for n in nodes:
            n.core.addrs = dict(addrs)
        return nodes

    async def _leader(self, nodes, timeout=5.0):
        end = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end:
            for n in nodes:
                if n.core.role == "leader":
                    return n
            await asyncio.sleep(0.02)
        return None

    async def _run(self):
        ids = ["n1", "n2", "n3"]
        nodes = await self._start(ids)
        try:
            leader = await self._leader(nodes)
            self.assertIsNotNone(leader, "no leader elected over HTTP")

            r = await leader.client_submit({"type": "submit", "msg_id": "m1",
                                           "author": "alice", "recipients": ["bob"],
                                           "text": "hello"}, "c1")
            self.assertTrue(r.get("ok"), r)
            self.assertTrue(r.get("committed"), r)

            # a linearizable read on a follower must be routed to the leader
            follower = [n for n in nodes if n is not leader][0]
            read = await follower.linearizable_read()
            self.assertFalse(read.get("ok"), "follower answered a linearizable read")
            read = await leader.linearizable_read()
            self.assertTrue(read.get("ok"), read)
            self.assertEqual(msg_of(read, "m1")["text"], "hello")

            # same command_id: must not create a second message
            r2 = await leader.client_submit({"type": "submit", "msg_id": "m2",
                                            "author": "alice", "recipients": ["bob"],
                                            "text": "dup"}, "c1")
            self.assertTrue(r2.get("duplicate"), r2)
            self.assertEqual(r2.get("index"), r.get("index"), "retry must map to the same log entry")
            self.assertEqual(len((await leader.linearizable_read())["state"]["messages"]), 1)

            # only the leader accepts writes
            rejected = await follower.client_submit({"type": "vote", "proposal_id": "p1",
                                                     "voter": follower.node_id}, "v-f")
            self.assertFalse(rejected.get("ok"), rejected)
            self.assertEqual(rejected.get("redirect"), leader.node_id, "write must redirect to the leader")

            # quorum decision through the cluster (leader write path)
            await leader.client_submit({"type": "propose", "proposal_id": "p1",
                                       "msg_id": "m1", "by": "alice", "to": "delivered"}, "c2")
            voters = [n.node_id for n in nodes][:2]
            for v in voters:
                await leader.client_submit({"type": "vote", "proposal_id": "p1", "voter": v}, f"v-{v}")
            read2 = await leader.linearizable_read()
            self.assertEqual(msg_of(read2, "m1")["state"], "delivered")
        finally:
            for n in nodes:
                await n.stop()

    def test_http_cluster(self):
        asyncio.run(self._run())

    async def _kill_and_reelect(self):
        ids = ["n1", "n2", "n3", "n4", "n5"]
        nodes = await self._start(ids)
        try:
            leader = await self._leader(nodes)
            self.assertIsNotNone(leader)
            self.assertEqual(majority(len(leader.core.quorum_sets()[0])), 3)

            r = await leader.client_membership(ids + ["n6"], joint=True, command_id="join-n6")
            self.assertTrue(r.get("committed"), r)
            self.assertEqual(sorted(leader.core.quorum_sets()[-1]), sorted(ids + ["n6"]))

            await leader.stop()
            rest = [n for n in nodes if n is not leader]
            new_leader = await self._leader(rest, timeout=6.0)
            self.assertIsNotNone(new_leader, "no re-election after leader death")
            self.assertNotEqual(new_leader.node_id, leader.node_id)
            sets = new_leader.core.quorum_sets()
            self.assertTrue(all(majority(len(s)) for s in sets), "joint quorum sets missing")
            self.assertEqual(len(sets), 2, "joint consensus must keep both configs")
        finally:
            for n in nodes:
                await n.stop()

    def test_membership_and_reelection(self):
        asyncio.run(self._kill_and_reelect())


if __name__ == "__main__":
    unittest.main()
