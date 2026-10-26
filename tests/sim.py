"""Deterministic simulated cluster used by the unit tests.

No wall clock, no real sockets, seeded RNG, zero-latency delivery: every run of
the same scenario produces the same trace, so a red test is a real bug and not a
flaky timing accident.
"""

import heapq
import random

from raft.core import LEADER, RaftCore
from raft.statemachine import ChatStateMachine
from raft.storage import MemoryStorage

DEFAULT_SIM_CFG = {"heartbeat_ms": 50, "election_min_ms": 150, "election_max_ms": 300}


class SimCluster:
    def __init__(self, node_ids, seed=7, cfg=None, initial_config=None):
        self.node_ids = list(node_ids)
        self.now = 0.0
        self._seq = 0
        self._events = []
        self.steps = 0
        self.down = set()
        self.partition = set()  # unordered node pairs that cannot talk
        self.cfg = dict(DEFAULT_SIM_CFG)
        if cfg:
            self.cfg.update(cfg)
        self.nodes = {}
        for i, n in enumerate(self.node_ids):
            self.nodes[n] = RaftCore(
                n, MemoryStorage(), ChatStateMachine(n),
                clock=self._clock, rng=random.Random(seed + i),
                initial_config=initial_config or self.node_ids,
                cfg=self.cfg, addr=f"sim://{n}",
            )

    def _clock(self):
        return self.now

    # ---- network control ----

    def cut(self, a, b):
        self.partition.add(frozenset([a, b]))

    def heal(self, a, b):
        self.partition.discard(frozenset([a, b]))

    def kill(self, node):
        self.down.add(node)

    def is_blocked(self, a, b):
        return a in self.down or b in self.down or frozenset([a, b]) in self.partition

    def enqueue(self, frm, msg):
        if self.is_blocked(frm, msg["to"]):
            return
        self._seq += 1
        heapq.heappush(self._events, (self.now, self._seq, frm, msg))

    # ---- execution ----

    def step(self):
        self.steps += 1
        if self._events:
            t, _, frm, msg = heapq.heappop(self._events)
            self.now = max(self.now, t)
            if self.is_blocked(frm, msg["to"]):
                return True
            if msg["to"] not in self.nodes:
                return True  # unreachable node: nothing to deliver
            outbox = self.nodes[msg["to"]].handle({"from": frm, "kind": msg["kind"], "payload": msg["payload"]})
            for m in outbox:
                self.enqueue(msg["to"], m)
            return True
        deadlines = []
        for n, c in self.nodes.items():
            if n in self.down:
                continue
            deadlines.append(c.heartbeat_deadline if c.role == LEADER else c.election_deadline)
        if not deadlines:
            return False
        self.now = max(self.now, min(deadlines))
        for n, c in self.nodes.items():
            if n in self.down:
                continue
            for m in c.tick(self.now):
                self.enqueue(n, m)
        return True

    def run(self, until_ms, max_steps=100000):
        while self.now < until_ms and self.steps < max_steps:
            if not self.step():
                break
        return self.now

    def drain(self):
        """Process everything queued at the current virtual time."""
        while self._events:
            self.step()

    def submit(self, node, command, command_id):
        reply, outbox = self.nodes[node].submit(command, command_id)
        for m in outbox:
            self.enqueue(node, m)
        self.drain()
        status = self.nodes[node].commit_status(command_id)
        return {**reply, "committed": status["committed"], "result": status["result"]}

    def change_membership(self, node, cnew, joint=True, command_id=None):
        command_id = command_id or f"mc-{node}-{'-'.join(sorted(cnew))}"
        reply, outbox = self.nodes[node].change_membership(cnew, joint=joint, command_id=command_id)
        for m in outbox:
            self.enqueue(node, m)
        self.drain()
        status = self.nodes[node].commit_status(command_id)
        return {**reply, "committed": status["committed"]}

    def join(self, node_id, initial_config=None):
        import random as _random
        from raft.core import RaftCore
        from raft.statemachine import ChatStateMachine
        from raft.storage import MemoryStorage
        core = RaftCore(node_id, MemoryStorage(), ChatStateMachine(node_id),
                        clock=self._clock, rng=_random.Random(hash(node_id) & 0xFFFF),
                        initial_config=initial_config or self.node_ids, cfg=self.cfg,
                        addr=f"sim://{node_id}")
        self.nodes[node_id] = core
        return core

    def leader(self):
        for n, c in self.nodes.items():
            if c.role == LEADER:
                return n
        return None

    def states(self):
        return {n: c.sm.to_dict() for n, c in self.nodes.items() if n not in self.down}
