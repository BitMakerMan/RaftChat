"""Web UI guards: static delivery, UI/API contract, HTML/JS drift.

The UI is only allowed to call endpoints the supervisor actually serves, and
every element app.js touches must exist in index.html.
"""

import asyncio
import json
import os
import re
import shutil
import tempfile
import unittest
import urllib.request

from raft import net
from raft.supervisor import Supervisor

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(HERE, "web")


def fetch(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return r.headers.get("Content-Type", ""), r.read().decode("utf-8")


class TestWebUI(unittest.TestCase):
    def test_static_files_are_served_with_correct_types(self):
        async def run():
            sup = Supervisor(port=0)
            await sup.start()
            base = net.base_url("127.0.0.1", sup.port)
            try:
                ct, body = await asyncio.to_thread(fetch, base + "/")
                self.assertTrue(ct.startswith("text/html"), ct)
                self.assertIn("Raft Chatroom", body)
                ct, body = await asyncio.to_thread(fetch, base + "/app.js")
                self.assertTrue(ct.startswith("application/javascript"), ct)
                ct, body = await asyncio.to_thread(fetch, base + "/style.css")
                self.assertTrue(ct.startswith("text/css"), ct)
                ct, body = await asyncio.to_thread(fetch, base + "/nope.html")
                self.assertFalse(body.get("ok") if isinstance(body, dict) else False)
            finally:
                await sup.stop()
        asyncio.run(run())

    def test_ui_controls_are_wired(self):
        app = open(os.path.join(WEB, "app.js"), encoding="utf-8").read()
        html = open(os.path.join(WEB, "index.html"), encoding="utf-8").read()
        controls = set(re.findall(r'<button id="([A-Za-z0-9_-]+)"', html))
        self.assertTrue(controls)
        unwired = [c for c in controls if f'$("#{c}").onclick' not in app]
        self.assertEqual(unwired, [], f"buttons without handlers in app.js: {unwired}")

    def test_js_selectors_exist_in_html(self):
        app = open(os.path.join(WEB, "app.js"), encoding="utf-8").read()
        html = open(os.path.join(WEB, "index.html"), encoding="utf-8").read()
        sel = set(re.findall(r'\$\("#([A-Za-z0-9_-]+)"\)', app))
        self.assertTrue(sel, "app.js must reference elements")
        missing = [s for s in sel if f'id="{s}"' not in html]
        self.assertEqual(missing, [], f"app.js uses elements missing from index.html: {missing}")

    def test_ui_endpoints_are_all_served(self):
        app = open(os.path.join(WEB, "app.js"), encoding="utf-8").read()
        paths = set(re.findall(r'api\("(/[^"]+)"', app))
        self.assertEqual(paths, {"/api/status", "/api/read", "/api/command",
                                 "/admin/spawn", "/admin/kill"},
                         "the UI may only call endpoints the supervisor serves")

    def test_ui_flow_against_live_cluster(self):
        async def run():
            d = tempfile.mkdtemp(prefix="raft-ui-")
            sup = Supervisor(port=0, data_dir=d)
            await sup.start()
            base = net.base_url("127.0.0.1", sup.port)
            try:
                for nid in ("n1", "n2"):
                    r = await sup.spawn(nid)
                    self.assertTrue(r.get("ok"), r)

                st = await net.get(base, "/api/status", timeout=15)
                self.assertTrue(st.get("ok"), st)
                self.assertIn(st["leader"], ("n1", "n2"))

                # what the send button does
                r = await net.post(base, "/api/command", {"type": "submit", "author": "alice",
                                           "recipients": ["all"], "text": "ciao",
                                           "msg_id": "ui-1", "command_id": "ui-1"}, timeout=15)
                self.assertTrue(r.get("ok") and r.get("committed"), r)
                inner = r.get("result") or {}
                self.assertTrue(inner.get("ok"), r)
                self.assertEqual(inner.get("msg_id"), "ui-1")

                # app.js sends no explicit msg_id, so it falls back to the command id
                r = await net.post(base, "/api/command", {"type": "submit", "author": "bob",
                                           "recipients": ["all"], "text": "fallback",
                                           "command_id": "ui-fallback"}, timeout=15)
                self.assertTrue((r.get("result") or {}).get("ok"), r)
                self.assertEqual((r.get("result") or {}).get("msg_id"), "ui-fallback",
                                 "submit without msg_id must fall back to command_id")

                # what a contested button does
                r = await net.post(base, "/api/command", {"type": "propose", "by": "n1",
                                           "msg_id": "ui-1", "to": "delivered",
                                           "proposal_id": "p1", "command_id": "ui-2"}, timeout=15)
                self.assertTrue(r.get("ok"), r)

                # what the vote button does (needs a majority of every active config)
                for voter in ("n1", "n2"):
                    r = await net.post(base, "/api/command", {"type": "vote", "voter": voter,
                                               "proposal_id": "p1", "command_id": f"ui-v-{voter}"},
                                       timeout=15)
                    self.assertTrue(r.get("ok"), r)

                read = await net.get(base, "/api/read", timeout=15)
                self.assertTrue(read.get("ok"), read)
                msg = read["state"]["messages"][0]
                self.assertEqual(msg["id"], "ui-1")
                self.assertEqual(msg["state"], "delivered")
                self.assertEqual(msg["current_text"], "ciao", "UI renders current_text")
                prop = read["state"]["proposals"][0]
                self.assertIsInstance(prop["votes"], list, "votes must be a JSON list for the UI")
                self.assertEqual(sorted(prop["votes"]), ["n1", "n2"])
                self.assertEqual(sorted(read["state"]["config"]["members"]), ["n1", "n2"])

                # dynamic discovery: the spawn button
                r = await net.post(base, "/admin/spawn", {"node_id": "n3"}, timeout=20)
                self.assertTrue(r.get("ok"), r)
                st = await net.get(base, "/api/status", timeout=15)
                self.assertEqual(sorted(st["nodes"]), ["n1", "n2", "n3"])

                # the kill button
                r = await net.post(base, "/admin/kill", {"node_id": "n3"}, timeout=20)
                self.assertTrue(r.get("ok"), r)
                st = await net.get(base, "/api/status", timeout=15)
                self.assertEqual(sorted(st["nodes"]), ["n1", "n2"])
            finally:
                for nid in list(sup.registry):
                    await sup.kill(nid, force=True)
                shutil.rmtree(d, ignore_errors=True)
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
