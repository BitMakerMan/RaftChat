"""Asyncio runtime: wires the pure Raft core to real HTTP.

Single-threaded per node (one event loop, no shared mutable state across tasks),
so the core never sees concurrent mutations.
"""

import asyncio
import random
import time
import uuid

from . import net
from .core import LEADER, RaftCore
from .statemachine import ChatStateMachine


def now_ms():
    return time.monotonic() * 1000.0


class NodeRuntime:
    def __init__(self, node_id, storage, sm=None, *, addr="", initial_config=None,
                 cfg=None, commit_timeout=2.0, rng=None):
        self.node_id = node_id
        self.storage = storage
        self.sm = sm or ChatStateMachine(node_id)
        self.core = RaftCore(node_id, self.storage, self.sm, addr=addr,
                             initial_config=initial_config, cfg=cfg,
                             rng=rng or random.Random(node_id), clock=now_ms)
        self.addr = addr or self.core.addr
        self.commit_timeout = commit_timeout
        self.server = None
        self.port = None
        self._tasks = []
        self._sent = 0

    # ---------------- transport ----------------

    async def _send(self, m):
        target = self.core.addrs.get(m["to"])
        if not target or m["to"] == self.node_id:
            return None
        payload = dict(m["payload"])
        payload["from"] = self.node_id
        payload["addr"] = self.addr
        resp = await net.post(target, net.KIND_PATH[m["kind"]], payload,
                              timeout=self.core.cfg["request_timeout_ms"] / 1000.0)
        self._sent += 1
        if resp:
            self.core.handle({"from": m["to"], "kind": m["kind"] + "_ack", "payload": resp})
        return resp

    async def _flush(self, outbox):
        for m in outbox:
            await self._send(m)

    # ---------------- RPC (peer-to-peer) ----------------

    async def rpc(self, path, payload):
        frm = payload.get("from") or "?"
        if payload.get("addr"):
            self.core.addrs[frm] = payload["addr"]
        kind = net.PATH_KIND.get(path)
        if kind is None:
            return {"ok": False, "error": f"unknown path {path}"}
        outbox = self.core.handle({"from": frm, "kind": kind, "payload": payload})
        response, rest = None, []
        for m in outbox:
            if m["to"] == frm:
                response = m["payload"]
            else:
                rest.append(m)
        await self._flush(rest)
        return response or {"ok": True}

    # ---------------- client API ----------------

    async def client_submit(self, command, command_id=None):
        command_id = command_id or command.get("command_id")
        if not command_id:
            return {"ok": False, "error": "command_id required"}
        reply, outbox = self.core.submit(command, command_id)
        if not reply.get("ok"):
            return reply
        await self._flush(outbox)
        if reply.get("committed"):
            return reply
        return await self._wait_commit(command_id)

    async def _wait_commit(self, command_id, timeout=None):
        timeout = timeout or self.commit_timeout
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.core.commit_status(command_id)
            if status["committed"]:
                return status
            await self._flush(self.core.tick())
            await asyncio.sleep(0.01)
        return {**self.core.commit_status(command_id), "warning": "commit not reached in time"}

    async def client_membership(self, cnew, joint=True, command_id=None):
        command_id = command_id or str(uuid.uuid4())
        reply, outbox = self.core.change_membership(cnew, joint=joint, command_id=command_id)
        if not reply.get("ok"):
            return reply
        await self._flush(outbox)
        return await self._wait_commit(command_id)

    async def linearizable_read(self):
        if self.core.role != LEADER:
            return {"ok": False, "error": "not leader", "leader": self.core.leader,
                    "read_local": self.core.read_state()}
        verified = True
        if not self.core.lease_valid():
            answers = {}
            for n in self.core.quorum_nodes():
                resp = await self._send_quorum_check(n)
                if resp and resp.get("ok"):
                    answers[n] = True
            verified = False
            for s in self.core.quorum_sets():
                if len([n for n in s if answers.get(n)]) < len(s) // 2 + 1:
                    return {"ok": False, "error": "leader lease not verified",
                            "state": self.core.read_state()}
            verified = True
        return {"ok": True, "state": self.core.read_state(), "verified": verified}

    async def _send_quorum_check(self, node):
        target = self.core.addrs.get(node)
        if not target:
            return None
        payload = {"term": self.core.term, "from": self.node_id, "addr": self.addr}
        return await net.post(target, net.KIND_PATH["check"], payload,
                              timeout=self.core.commit_timeout)

    def status(self):
        return self.core.status()

    # ---------------- lifecycle ----------------

    async def start(self, host="127.0.0.1", port=0):
        async def handler(path, payload):
            if path.startswith("/raft/"):
                return await self.rpc(path, payload)
            if path in ("/client/command", "/client/submit"):
                return await self.client_submit(payload.get("command", payload),
                                                payload.get("command_id"))
            if path in ("/client/read", "/client/state"):
                return await self.linearizable_read()
            if path in ("/cluster/join", "/cluster/leave", "/cluster/members"):
                cnew = payload.get("cnew") or payload.get("members")
                if cnew is None:
                    return {"ok": True, "members": self.core.config_members(),
                            "configs": self.core.configs, "leader": self.core.leader}
                return await self.client_membership(cnew, joint=bool(payload.get("joint", True)))
            if path in ("/node/status", "/node/stats"):
                return self.status()
            if path == "/client/commit":
                cid = payload.get("command_id")
                return self.core.commit_status(cid) if cid else {"ok": False, "error": "command_id required"}
            return {"ok": False, "error": f"unknown path {path}"}

        self.server, self.port = await net.start_server(host, port, handler)
        self.addr = net.base_url(host, self.port)
        self.core.addr = self.addr
        self._tasks.append(asyncio.create_task(self._pump()))
        return self.port

    async def _pump(self):
        while True:
            try:
                outbox = self.core.tick()
                for m in outbox:
                    await self._send(m)
            except Exception as e:  # the node must stay alive
                self._errors = getattr(self, "_errors", 0) + 1
            await asyncio.sleep(self.core.cfg["heartbeat_ms"] / 1000.0 / 4)

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        if self.server:
            self.server.close()
