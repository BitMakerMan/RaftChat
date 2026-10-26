"""Supervisor tests: real processes, spawn/kill endpoints, dynamic discovery.

Every spawned node is a separate OS process writing its own SQLite file.
"""

import asyncio
import os
import shutil
import tempfile
import unittest

from raft import net
from raft.supervisor import Supervisor

T = 15.0


class TestSupervisor(unittest.TestCase):
    async def _run(self):
        d = tempfile.mkdtemp(prefix="raft-sup-")
        sup = Supervisor(port=0, data_dir=d)
        await sup.start()
        base = net.base_url("127.0.0.1", sup.port)
        try:
            for nid in ("n1", "n2", "n3"):
                r = await sup.spawn(nid)
                self.assertTrue(r.get("ok"), r)
                self.assertTrue(r.get("pid"), r)

            nodes = await sup.live_nodes()
            self.assertEqual(len(nodes), 3, "dynamic discovery must know all spawned nodes")
            for info in nodes.values():
                self.assertTrue(info["status"]["ok"], info)

            r = await net.post(base, "/api/command", {"type": "submit", "msg_id": "m1",
                                       "author": "alice", "recipients": ["bob"],
                                       "text": "hi", "command_id": "c1"}, timeout=T)
            self.assertTrue(r.get("ok") and r.get("committed"), r)

            read = await net.get(base, "/api/read", timeout=T)
            self.assertTrue(read.get("ok"), read)
            self.assertEqual(len(read["state"]["messages"]), 1)

            st = await net.get(base, "/api/status", timeout=T)
            self.assertIsNotNone(st["leader"])

            # kill through membership first (safe path)
            r2 = await net.post(base, "/admin/kill", {"node_id": "n3"}, timeout=T)
            self.assertTrue(r2.get("ok"), r2)

            # the two survivors must re-elect a leader (n3 is gone)
            end = asyncio.get_event_loop().time() + 10
            st = None
            while asyncio.get_event_loop().time() < end:
                st = await net.get(base, "/api/status", timeout=T)
                if st and st.get("leader"):
                    break
                await asyncio.sleep(0.3)
            self.assertTrue(st and st.get("leader"), st)
            cfg = st["nodes"][st["leader"]]["configs"][-1]
            self.assertEqual(sorted(cfg["cnew"]), ["n1", "n2"], "kill must remove the node from membership")
            self.assertNotEqual(st["leader"], "n3", "killed node must not be leader")

            # the remaining 2 nodes still form a quorum
            r3 = await net.post(base, "/api/command", {"type": "submit", "msg_id": "m2",
                                       "author": "alice", "recipients": ["bob"],
                                       "text": "again", "command_id": "c2"}, timeout=T)
            self.assertTrue(r3.get("committed"), r3)

            # disk files were created for every node
            files = sorted(f for f in os.listdir(d) if f.endswith(".db"))
            self.assertEqual(files, ["node-n1.db", "node-n2.db", "node-n3.db"], files)

            # a node that is gone must not be killable twice
            r4 = await net.post(base, "/admin/kill", {"node_id": "n3"}, timeout=T)
            self.assertFalse(r4.get("ok") and r4.get("killed"), r4)
        finally:
            for nid in list(sup.registry):
                await sup.kill(nid, force=True)
            shutil.rmtree(d, ignore_errors=True)

    def test_spawn_kill_and_discovery(self):
        asyncio.run(self._run())


if __name__ == "__main__":
    unittest.main()
