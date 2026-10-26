"""CLI end-to-end: the CLI talks to a live supervisor over HTTP.

The CLI is a blocking process, so calls run in a thread — otherwise the
supervisor's event loop would be blocked and could not answer the request.
"""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from raft import net
from raft.supervisor import Supervisor

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(HERE, "cli.py")


def cli(base, *args):
    out = subprocess.run([sys.executable, CLI, "--supervisor", base] + list(args),
                         capture_output=True, text=True, timeout=30)
    if out.returncode != 0:
        raise AssertionError(f"cli failed {args} rc={out.returncode} stdout={out.stdout!r} stderr={out.stderr!r}")
    return json.loads(out.stdout)


class TestCli(unittest.TestCase):
    async def _run(self):
        d = tempfile.mkdtemp(prefix="raft-cli-")
        sup = Supervisor(port=0, data_dir=d)
        await sup.start()
        base = net.base_url("127.0.0.1", sup.port)
        try:
            for nid in ("n1", "n2", "n3"):
                r = await sup.spawn(nid)
                self.assertTrue(r.get("ok"), r)

            st = await asyncio.to_thread(cli, base, "status")
            self.assertTrue(st.get("ok"), st)
            self.assertEqual(len(st["nodes"]), 3)

            r = await asyncio.to_thread(cli, base, "send", "--user", "alice", "--text", "hello", "--to", "bob", "--msg", "m1")
            self.assertTrue(r.get("ok") and r.get("committed"), r)
            self.assertTrue((r.get("result") or {}).get("ok"), r)

            r = await asyncio.to_thread(cli, base, "propose", "--user", "n1",
                                        "--msg", "m1", "--state", "delivered", "--proposal", "p1")
            self.assertTrue(r.get("ok") and r.get("committed"), r)
            self.assertTrue((r.get("result") or {}).get("ok"), r)

            # joint membership: a decision needs a majority in every active config
            r = await asyncio.to_thread(cli, base, "vote", "--voter", "n1", "--proposal", "p1")
            self.assertTrue(r.get("ok") and r.get("committed"), r)
            r = await asyncio.to_thread(cli, base, "vote", "--voter", "n2", "--proposal", "p1")
            self.assertTrue(r.get("ok") and r.get("committed"), r)
            self.assertTrue((r.get("result") or {}).get("ok"), r)

            r = await asyncio.to_thread(cli, base, "vote", "--voter", "n3", "--proposal", "p1")
            self.assertTrue(r.get("ok") and r.get("committed"), r)
            self.assertTrue((r.get("result") or {}).get("ok"), r)

            read = await asyncio.to_thread(cli, base, "read")
            self.assertTrue(read.get("ok"), read)
            msg = {m["id"]: m for m in read["state"]["messages"]}["m1"]
            self.assertEqual(msg["state"], "delivered", "quorum decision must be applied")

            r = await asyncio.to_thread(cli, base, "kill", "--id", "n3")
            self.assertTrue(r.get("ok"), r)
            st = await asyncio.to_thread(cli, base, "status")
            self.assertEqual(sorted(st["nodes"]), ["n1", "n2"], "killed node must be gone")
            cfg = st["nodes"][st["leader"]]["configs"][-1]
            self.assertEqual(sorted(cfg["cnew"]), ["n1", "n2"], "safe kill must leave membership first")

            r = await asyncio.to_thread(cli, base, "send", "--user", "alice", "--text", "still works", "--to", "bob",
                                        "--msg", "m2", "--cid", "c2")
            self.assertTrue(r.get("ok") and r.get("committed"), r)
            self.assertTrue((r.get("result") or {}).get("ok"), r)
        finally:
            for nid in list(sup.registry):
                await sup.kill(nid, force=True)
            shutil.rmtree(d, ignore_errors=True)

    def test_cli_over_http(self):
        asyncio.run(self._run())


if __name__ == "__main__":
    unittest.main()
