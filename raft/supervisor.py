"""Supervisor: process registry + spawn/kill endpoints + dynamic discovery.

Endpoints (all JSON):
  GET  /                      -> web UI (static)
  GET  /admin/nodes           -> registered nodes and their live status
  POST /admin/register        -> a node announces itself (dynamic discovery)
  POST /admin/spawn           -> {"node_id": "n4", "port": 8304}
  POST /admin/kill            -> {"node_id": "n4", "force": false}
  POST /api/command           -> proxied to the current leader
  GET  /api/read              -> linearizable read from the leader
  POST /api/membership        -> membership change through the leader
  GET  /api/cluster           -> cluster view (members + leader + per-node status)

Kill is safe by default: the membership change is committed through Raft first,
then the process is terminated. `force: true` skips that step (documented risk:
the cluster may lose quorum).
"""

import asyncio
import json
import os
import subprocess
import sys
import urllib.parse

from . import net

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Supervisor:
    def __init__(self, host="127.0.0.1", port=8200, data_dir=None, web_dir=None):
        self.host = host
        self.port = port
        self.data_dir = data_dir or os.path.join(HERE, "data")
        self.web_dir = web_dir or os.path.join(HERE, "web")
        self.registry = {}   # node_id -> {"url": url, "pid": pid or None}
        self.server = None

    # ---------------- helpers ----------------

    async def node_status(self, url, timeout=1.5):
        return await net.post(url, "/node/status", {"from": "supervisor"}, timeout=timeout)

    async def live_nodes(self):
        out = {}
        for nid, info in list(self.registry.items()):
            st = await self.node_status(info["url"])
            if st and st.get("node_id"):
                out[nid] = {"url": info["url"], "pid": info.get("pid"), "status": st}
        return out

    async def find_leader(self, nodes):
        for nid, info in nodes.items():
            st = info.get("status") or {}
            if st.get("role") == "leader":
                return nid, info["url"]
            leader = st.get("leader")
            if leader and leader in nodes:
                return leader, nodes[leader]["url"]
        return None, None

    # ---------------- admin ----------------

    async def wait_leader(self, timeout=8.0):
        end = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end:
            nodes = await self.live_nodes()
            leader_id, leader_url = await self.find_leader(nodes)
            if leader_url:
                return leader_id, leader_url
            await asyncio.sleep(0.2)
        return None, None

    async def spawn(self, node_id, port=None, seeds=None, initial_config=None):
        if node_id in self.registry and self.registry[node_id].get("pid"):
            return {"ok": False, "error": f"{node_id} is already running"}
        nodes = await self.live_nodes()
        leader_id, leader_url = await self.find_leader(nodes)
        if not leader_url and self.registry:
            leader_id, leader_url = await self.wait_leader()
        # the new node starts as a non-member and learns membership from the log
        existing = sorted(set(self.registry) - {node_id})
        config = sorted(initial_config or (existing or [node_id]))
        peers = ",".join(f"{nid}={i['url']}" for nid, i in self.registry.items() if nid != node_id)
        args = [sys.executable, os.path.join(HERE, "run_node.py"),
                "--id", node_id, "--port", str(port or 0),
                "--seeds", ",".join(seeds or [i["url"] for i in self.registry.values()]),
                "--peers", peers,
                "--config", ",".join(config),
                "--data-dir", self.data_dir,
                "--supervisor", f"http://{self.host}:{self.port}"]
        env = dict(os.environ)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
        # read the first line: {"node_id":..., "port":...}
        loop = asyncio.get_event_loop()
        line = await asyncio.wait_for(loop.run_in_executor(None, proc.stdout.readline), timeout=20)
        info = json.loads(line.decode() or "{}")
        actual_port = info.get("port") or port
        url = net.base_url(self.host, actual_port)
        self.registry[node_id] = {"url": url, "pid": proc.pid, "config": config, "proc": proc}
        joined = None
        if existing:
            leader_id, leader_url = await self.wait_leader(timeout=6.0)
            if leader_url:
                members = (nodes.get(leader_id, {}).get("status") or {}).get("members") or config
                if node_id not in members:
                    joined = await net.post(leader_url, "/cluster/join",
                                            {"cnew": sorted(set(members) | {node_id}), "joint": True},
                                            timeout=10.0)
        return {"ok": True, "node_id": node_id, "url": url, "pid": proc.pid,
                "config": config, "join": joined}

    async def kill(self, node_id, force=False):
        info = self.registry.get(node_id)
        if not info:
            return {"ok": False, "error": f"{node_id} is not registered"}
        if not force:
            nodes = await self.live_nodes()
            leader_id, leader_url = await self.find_leader(nodes)
            members = (nodes.get(leader_id, {}).get("status") or {}).get("members") or list(nodes)
            cnew = [m for m in members if m != node_id]
            if leader_url and node_id in members and cnew:
                reply = await net.post(leader_url, "/cluster/leave",
                                       {"cnew": cnew, "joint": False}, timeout=5.0)
                if not reply or not reply.get("committed"):
                    return {"ok": False, "error": "membership change not committed",
                            "reply": reply, "hint": "use force:true to kill anyway"}
        pid = info.get("pid")
        if pid:
            try:
                def terminate():
                    if os.name == "nt":
                        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
                    else:
                        os.kill(pid, 15)
                    proc = info.get("proc")
                    if proc:
                        try:
                            proc.wait(5)
                        except subprocess.TimeoutExpired:
                            pass
                await asyncio.to_thread(terminate)
            except Exception as e:
                return {"ok": False, "error": str(e)}
        info["pid"] = None
        return {"ok": True, "node_id": node_id, "killed": bool(pid)}

    # ---------------- HTTP handler ----------------

    async def handler(self, path, payload):
        parsed = urllib.parse.urlsplit(path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        if path == "/" or path.startswith("/") and "." in path:
            name = "index.html" if path == "/" else path[1:]
            f = os.path.join(self.web_dir, name)
            if os.path.isfile(f):
                ctype = ("text/css" if name.endswith(".css") else
                         "application/javascript" if name.endswith(".js") else
                         "text/html; charset=utf-8")
                return (ctype, open(f, "rb").read())
            return {"ok": False, "error": "web UI not found"}

        if path == "/admin/nodes":
            nodes = await self.live_nodes()
            return {"ok": True, "nodes": nodes, "registry": self.registry}

        if path == "/admin/register":
            nid = payload.get("node_id") or query.get("node_id", [""])[0]
            url = payload.get("url") or query.get("url", [""])[0]
            if not nid or not url:
                return {"ok": False, "error": "node_id and url required"}
            self.registry[nid] = {"url": url, "pid": self.registry.get(nid, {}).get("pid")}
            return {"ok": True, "node_id": nid, "url": url}

        if path == "/admin/spawn":
            return await self.spawn(payload.get("node_id") or query.get("node_id", [""])[0],
                                   payload.get("port") or (int(query["port"][0]) if "port" in query else None),
                                   payload.get("seeds"), payload.get("config"))

        if path == "/admin/kill":
            return await self.kill(payload.get("node_id") or query.get("node_id", [""])[0],
                                   bool(payload.get("force")))

        nodes = await self.live_nodes()
        leader_id, leader_url = await self.find_leader(nodes)
        if not leader_url:
            return {"ok": False, "error": "no leader reachable", "nodes": nodes}
        if path in ("/api/read", "/client/read"):
            return await net.post(leader_url, "/client/read", {}, timeout=5.0)
        if path in ("/api/command", "/client/command"):
            return await net.post(leader_url, "/client/command", payload, timeout=8.0)
        if path in ("/api/membership", "/cluster/join", "/cluster/leave"):
            return await net.post(leader_url, "/cluster/join", payload, timeout=8.0)
        if path in ("/api/cluster", "/node/cluster"):
            return {"ok": True, "leader": leader_id, "members": (nodes[leader_id]["status"] or {}).get("members"),
                    "nodes": {n: i["status"] for n, i in nodes.items()}}
        if path == "/api/status":
            return {"ok": True, "leader": leader_id, "leader_url": leader_url,
                    "nodes": {n: i["status"] for n, i in nodes.items()}}
        return {"ok": False, "error": f"unknown path {path}"}

    async def start(self):
        self.server, self.port = await net.start_server(self.host, self.port, self.handler)
        return self.port

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None


async def main(port=8200):
    s = Supervisor(port=port)
    await s.start()
    print(json.dumps({"ok": True, "supervisor": f"http://{s.host}:{s.port}"}))
    import asyncio as _a
    await _a.Event().wait()


if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else 8200
    asyncio.run(main(p))
