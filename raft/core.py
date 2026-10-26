"""Pure Raft core.

Design rule (anti-regression): this module is *pure* — no sockets, no threads,
no wall-clock, no randomness other than the injected RNG. Every handler returns
an outbox of messages. That makes election/log/commit behaviour fully
deterministic in tests while the same code runs over real HTTP in the runtime.

Injected dependencies:
  storage  : Storage (MemoryStorage or SqliteStorage)
  sm       : ChatStateMachine
  clock    : callable -> milliseconds
  rng      : random.Random
"""

import random
import time

from .storage import entry as mk_entry

FOLLOWER, PRECANDIDATE, CANDIDATE, LEADER = "follower", "precandidate", "candidate", "leader"

K_COMMAND, K_MEMBERSHIP, K_NOOP = "command", "membership", "noop"

DEFAULT_CFG = {
    "heartbeat_ms": 100,
    "election_min_ms": 300,
    "election_max_ms": 600,
    "snapshot_threshold": 64,
    "max_batch": 32,
    "request_timeout_ms": 400,
    "pre_vote": True,
}


class RaftCore:
    def __init__(self, node_id, storage, sm, *, clock=None, rng=None, addr="",
                 initial_config=None, cfg=None):
        self.node_id = node_id
        self.storage = storage
        self.sm = sm
        self.clock = clock or (lambda: time.monotonic() * 1000.0)
        self.rng = rng or random.Random()
        self.cfg = dict(DEFAULT_CFG)
        if cfg:
            self.cfg.update(cfg)
        self.addr = addr
        self.initial_config = sorted(initial_config or [node_id])

        self.term = 0
        self.voted_for = None
        self.role = FOLLOWER
        self.leader = None
        self.commit_index = 0
        self.last_applied = 0
        self.configs = []           # membership as seen in the log (joint-aware)
        self.addrs = {node_id: addr}
        self.next = {}
        self.match = {}
        self.votes = set()
        self.prevotes = set()
        self.prevote_for = None   # pre-votes are tracked separately from real votes
        self.election_deadline = self._random_election()
        self.heartbeat_deadline = 0.0
        self.lease_expired = 0.0
        self.pending = {}          # command_id -> log index (client retry guard)
        self.stats = {"votes_won": 0, "appends_sent": 0, "commit_advanced": 0,
                      "snapshots_taken": 0, "snapshots_installed": 0, "rejected": 0,
                      "elections_started": 0, "prevotes_started": 0}
        self._load()

    # ---------------- recovery ----------------

    def _load(self):
        self.term = self.storage.meta("current_term", 0)
        self.voted_for = self.storage.meta("voted_for")
        self.commit_index = self.storage.meta("commit_index", 0)
        snap = self.storage.snapshot()
        if snap:
            self.sm.load(snap["data"])
            self.last_applied = snap["index"]
        i = self.last_applied + 1
        while i <= self.commit_index:
            e = self.storage.entry(i)
            if e is None:
                break
            self.sm.apply(e)
            self.last_applied = i
            i += 1
        self._sync_configs()

    # ---------------- membership ----------------

    def _add_config(self, e):
        p = e.get("payload") or {}
        self.configs.append({"cold": sorted(p.get("cold") or []), "cnew": sorted(p.get("cnew") or []),
                             "joint": bool(p.get("joint")), "index": e["index"], "entry_index": e["index"]})
        if hasattr(self.sm, "load_configs"):
            self.sm.load_configs(self.configs)

    def _sync_configs(self):
        self.configs = []
        if self.initial_config:
            base = sorted(self.initial_config)
            self.configs.append({"cold": base, "cnew": base, "joint": False, "index": 0})
        snap = self.storage.snapshot()
        if snap:
            for cfg in snap["data"].get("configs", []):
                self.configs.append(cfg)
        for e in self.storage.entries_from(1):
            if e["kind"] == K_MEMBERSHIP:
                self._add_config(e)
        # the state machine must see the same membership as the core
        if hasattr(self.sm, "load_configs"):
            self.sm.load_configs(self.configs)

    def quorum_sets(self):
        if not self.configs:
            return [set(self.initial_config)]
        cfg = self.configs[-1]
        sets = []
        if cfg.get("joint"):
            if cfg.get("cold"):
                sets.append(set(cfg["cold"]))
            if cfg.get("cnew"):
                sets.append(set(cfg["cnew"]))
        else:
            sets.append(set(cfg.get("cnew") or cfg.get("cold") or []))
        sets = [s for s in sets if s]
        return sets or [set(self.initial_config)]

    def config_members(self):
        return sorted(set().union(*self.quorum_sets()))

    def is_member(self, node_id):
        return any(node_id in s for s in self.quorum_sets())

    def peers(self):
        return [n for n in self.config_members() if n != self.node_id]

    def quorum_nodes(self):
        return self.config_members()

    def lease_valid(self, now=None):
        now = self.clock() if now is None else now
        return self.role == LEADER and now < self.lease_expired

    def _random_election(self):
        lo, hi = self.cfg["election_min_ms"], self.cfg["election_max_ms"]
        return self.clock() + lo + self.rng.random() * (hi - lo)

    # ---------------- log helpers ----------------

    def last_index(self):
        return self.storage.last_index()

    def last_term(self):
        return self.storage.last_term()

    def _up_to_date(self, other_term, other_index):
        lt, li = self.last_term(), self.last_index()
        if other_term != lt:
            return other_term > lt
        return other_index >= li

    def _entry(self, kind, payload, command_id=None):
        idx = self.last_index() + 1
        e = mk_entry(idx, self.term, kind, payload, command_id)
        self.storage.append(e)
        if kind == K_MEMBERSHIP:
            self._add_config(e)
        return e

    def _snapshot_payload(self):
        snap = self.storage.snapshot()
        if snap:
            return snap
        return {"index": self.last_applied, "term": self.storage.meta("snapshot_term", 0),
                "data": self.sm.to_dict()}

    def _compact(self):
        if self.storage.log_size() >= self.cfg["snapshot_threshold"]:
            idx = self.last_applied
            if idx <= 0:
                return
            self.storage.set_meta("snapshot_term", self.term)
            self.storage.save_snapshot(idx, self.term, self.sm.to_dict())
            self.stats["snapshots_taken"] += 1

    def _apply_upto(self, target):
        while self.last_applied < target:
            e = self.storage.entry(self.last_applied + 1)
            if e is None:
                break
            self.sm.apply(e)
            self.last_applied = e["index"]
        self._compact()

    def _advance_commit(self):
        if self.role != LEADER:
            return
        self.match[self.node_id] = self.last_index()
        best = 0
        for s in self.quorum_sets():
            vals = sorted((self.match.get(n, 0) for n in s), reverse=True)
            need = len(s) // 2 + 1
            if len(vals) < need:
                return
            best = min(best, vals[need - 1]) if best else vals[need - 1]
        if best <= self.commit_index:
            return
        e = self.storage.entry(best)
        # Raft safety: never commit an entry from a previous term directly.
        if e is None or e["term"] != self.term:
            return
        self.commit_index = best
        self.storage.set_meta("commit_index", best)
        self.stats["commit_advanced"] += 1
        self._apply_upto(best)

    def _step_down(self, new_term):
        self.term = new_term
        self.voted_for = None
        self.role = FOLLOWER
        self.leader = None
        self.votes = set()
        self.prevotes = set()
        self.prevote_for = None
        self.election_deadline = self._random_election()
        self.storage.set_meta("current_term", new_term)
        self.storage.set_meta("voted_for", None)

    # ---------------- outbox builders ----------------

    def _msg(self, to, kind, payload):
        return {"to": to, "kind": kind, "payload": payload}

    def _sync_peer(self, n, batch=None):
        """A peer behind the leader's snapshot must receive the snapshot, not entries."""
        snap = self.storage.snapshot()
        if snap and self.next.get(n, self.last_index() + 1) - 1 < snap["index"]:
            return self._msg(n, "snapshot", {"term": self.term, "snapshot": self._snapshot_payload()})
        return self._msg(n, "append", self._append_payload(n, batch))

    def _vote_payload(self, prevote):
        return {
            "term": self.term,
            "prevote": prevote,
            "last_index": self.last_index(),
            "last_term": self.last_term(),
            "candidate": self.node_id,
        }

    def _append_payload(self, to, batch=None):
        if batch:
            prev_index, batch_entries = batch[0]["index"] - 1, batch
        else:
            ni = self.next.get(to, self.last_index() + 1)
            prev_index, batch_entries = ni - 1, self.storage.entries_from(ni, self.cfg["max_batch"])
        prev = self.storage.entry(prev_index)
        return {
            "term": self.term,
            "leader": self.node_id,
            "addr": self.addr,
            "prev_index": prev_index,
            "prev_term": prev["term"] if prev else 0,
            "entries": batch_entries,
            "commit_index": self.commit_index,
        }

    # ---------------- main dispatch ----------------

    def handle(self, msg):
        """msg = {"from": node_id, "kind": str, "payload": dict} -> outbox list"""
        frm, kind, p = msg["from"], msg["kind"], msg.get("payload") or {}
        now = self.clock()
        if p.get("addr"):
            self.addrs[frm] = p["addr"]

        if kind == "vote":
            return [self._msg(frm, "vote_ack", self._handle_vote(frm, p))]
        if kind == "vote_ack":
            return self._handle_vote_ack(frm, p)
        if kind == "append":
            return [self._msg(frm, "append_ack", self._handle_append(frm, p))]
        if kind == "append_ack":
            return self._handle_append_ack(frm, p)
        if kind == "snapshot":
            return [self._msg(frm, "snapshot_ack", self._handle_snapshot(frm, p))]
        if kind == "snapshot_ack":
            return self._handle_snapshot_ack(frm, p)
        if kind == "check":
            return [self._msg(frm, "check_ack", self._handle_check(frm, p))]
        if kind == "check_ack":
            return []
        return []

    def tick(self, now=None):
        now = self.clock() if now is None else now
        if self.role == LEADER:
            if now >= self.heartbeat_deadline:
                self.heartbeat_deadline = now + self.cfg["heartbeat_ms"]
                out = []
                for n in self.peers():
                    out.append(self._sync_peer(n))
                    self.stats["appends_sent"] += 1
                return out
            return []
        if now >= self.election_deadline:
            prevote = bool(self.cfg.get("pre_vote")) and self.role == FOLLOWER
            return self._start_election(prevote=prevote or self.role == PRECANDIDATE)
        return []

    # ---------------- handlers ----------------

    def _handle_vote(self, frm, p):
        if not self.is_member(frm):
            self.stats["rejected"] += 1
            return {"term": self.term, "grant": False, "reason": "not member"}
        prevote = bool(p.get("prevote"))
        if p["term"] > self.term and not prevote:
            self._step_down(p["term"])
        if prevote:
            # pre-vote never changes term nor voted_for
            grant = p["term"] >= self.term and self._up_to_date(p["last_term"], p["last_index"])
            grant = grant and self.prevote_for in (None, frm)
            if grant:
                self.prevote_for = frm
            return {"term": self.term, "grant": bool(grant)}
        if p["term"] != self.term:
            return {"term": self.term, "grant": False, "reason": "term mismatch"}
        if self.role == LEADER:
            return {"term": self.term, "grant": False, "reason": "leader already elected"}
        if self.voted_for not in (None, frm):
            return {"term": self.term, "grant": False, "reason": "already voted"}
        if not self._up_to_date(p["last_term"], p["last_index"]):
            return {"term": self.term, "grant": False, "reason": "log not up to date"}
        self.voted_for = frm
        self.storage.set_meta("voted_for", frm)
        self.election_deadline = self._random_election()
        return {"term": self.term, "grant": True}

    def _handle_vote_ack(self, frm, p):
        if self.role == PRECANDIDATE:
            if p.get("grant") and p.get("term", 0) >= self.term:
                self.prevotes.add(frm)
                if self._reached(self.prevotes):
                    return self._start_election(prevote=False)
        elif self.role == CANDIDATE:
            if p.get("grant") and p.get("term") == self.term:
                self.votes.add(frm)
                if self._reached(self.votes):
                    return self._become_leader()
        return []

    def _handle_append(self, frm, p):
        if p["term"] < self.term:
            self.stats["rejected"] += 1
            return {"term": self.term, "success": False, "reason": "stale term"}
        if not self.is_member(frm) and frm not in self.initial_config:
            self.stats["rejected"] += 1
            return {"term": self.term, "success": False, "reason": "not member"}
        if p["term"] > self.term:
            self._step_down(p["term"])
        snap = self.storage.snapshot()
        if snap and p["prev_index"] < snap["index"]:
            return {"term": self.term, "success": False, "snapshot_required": True, "next_index": 1,
                    "conflict_index": p["prev_index"]}
        if p["prev_index"] > 0:
            prev = self.storage.entry(p["prev_index"])
            if prev is None:
                return {"term": self.term, "success": False, "next_index": 1,
                        "snapshot_required": bool(snap), "conflict_index": p["prev_index"]}
            if prev["term"] != p["prev_term"]:
                return {"term": self.term, "success": False, "conflict_term": prev["term"],
                        "conflict_index": p["prev_index"]}

        for e in p.get("entries") or []:
            idx = e["index"]
            existing = self.storage.entry(idx)
            truncated = False
            if existing is None:
                if idx != self.last_index() + 1:
                    continue
                self.storage.append(e)
                if e["kind"] == K_MEMBERSHIP:
                    self._add_config(e)
            elif existing["term"] != e["term"]:
                self.storage.truncate_from(idx)
                self.storage.append(e)
                truncated = True
            if truncated:
                self._sync_configs()
        self.leader = frm
        # a heartbeat is NOT a vote: voted_for must only change when we grant a vote
        self.election_deadline = self._random_election()
        if p.get("commit_index") is not None:
            target = min(p["commit_index"], self.last_index())
            if target > self.commit_index:
                self.commit_index = target
                self.storage.set_meta("commit_index", target)
                self._apply_upto(target)
        return {"term": self.term, "success": True, "next_index": self.last_index() + 1}

    def _handle_append_ack(self, frm, p):
        out = []
        if self.role != LEADER:
            return out
        if p.get("snapshot"):
            self._install_snapshot(frm, p["snapshot"])
            return out
        if p.get("term", 0) > self.term:
            self._step_down(p["term"])
            return out
        if p.get("success"):
            self.match[frm] = p.get("next_index", 0) - 1
            self.next[frm] = p.get("next_index", 0)
            before = self.commit_index
            self._advance_commit()
            if self.commit_index > before:
                for n in self.peers():
                    out.append(self._msg(n, "append", self._append_payload(n)))
            self.lease_expired = max(self.lease_expired, self.clock() + 2 * self.cfg["heartbeat_ms"])
        else:
            snap = self.storage.snapshot()
            if snap and (p.get("snapshot_required") or self.next.get(frm, self.last_index() + 1) - 1 < snap["index"]):
                out.append(self._msg(frm, "snapshot", {"term": self.term, "snapshot": self._snapshot_payload()}))
            else:
                self.next[frm] = max(1, self.next.get(frm, self.last_index() + 1) - 1)
        return out

    def _handle_snapshot(self, frm, p):
        if p.get("term", 0) > self.term:
            self._step_down(p["term"])
        snap = p.get("snapshot")
        if snap:
            self._install_snapshot(frm, snap)
        return {"term": self.term, "ok": True, "next_index": self.last_index() + 1}

    def _handle_snapshot_ack(self, frm, p):
        if self.role != LEADER or p.get("term", 0) > self.term:
            return []
        self.match[frm] = max(0, p.get("next_index", 0) - 1)
        self.next[frm] = p.get("next_index", self.last_index() + 1)
        before = self.commit_index
        self._advance_commit()
        out = []
        if self.commit_index > before:
            out.append(self._sync_peer(frm))
        return out

    def _install_snapshot(self, frm, snap):
        self.sm.load(snap["data"])
        self.configs = [dict(c) for c in snap["data"].get("configs", [])]
        self.last_applied = snap["index"]
        self.commit_index = max(self.commit_index, snap["index"])
        self.storage.set_meta("commit_index", self.commit_index)
        self.storage.save_snapshot(snap["index"], snap["term"], snap["data"])
        self.stats["snapshots_installed"] += 1
        self.match[frm] = self.last_index()
        self.next[frm] = self.last_index() + 1

    def _handle_check(self, frm, p):
        if p.get("term") != self.term:
            return {"term": self.term, "ok": False}
        return {"term": self.term, "ok": self.role == LEADER and self.leader == self.node_id}

    # ---------------- leader / election ----------------

    def _reached(self, votes):
        for s in self.quorum_sets():
            if len(votes & s) < len(s) // 2 + 1:
                return False
        return True

    def _start_election(self, prevote):
        out = []
        if prevote:
            if self.role != PRECANDIDATE:
                self.role = PRECANDIDATE
                self.stats["prevotes_started"] += 1
            self.prevotes.add(self.node_id)
            # pre-vote must not inflate the term, so it gets a short random window
            self.election_deadline = self._random_election()
        else:
            self.term += 1
            self.storage.set_meta("current_term", self.term)
            self.voted_for = self.node_id
            self.storage.set_meta("voted_for", self.node_id)
            self.role = CANDIDATE
            self.votes = {self.node_id}
            self.leader = None
            self.stats["elections_started"] += 1
            self.election_deadline = self.clock() + self._random_election() - self.clock()
        if self._reached(self.prevotes if prevote else self.votes):
            return self._become_leader()
        for n in self.peers():
            out.append(self._msg(n, "vote", self._vote_payload(prevote)))
        return out

    def _become_leader(self):
        self.role = LEADER
        self.leader = self.node_id
        self.stats["votes_won"] += 1
        self.next = {n: self.last_index() + 1 for n in self.config_members()}
        self.match = {n: 0 for n in self.config_members()}
        self.lease_expired = self.clock() + 2 * self.cfg["heartbeat_ms"]
        noop = self._entry(K_NOOP, {"leader": self.node_id})
        out = []
        for n in self.peers():
            out.append(self._msg(n, "append", self._append_payload(n, [noop])))
        self._advance_commit()
        return out

    # ---------------- client API ----------------

    def submit(self, command, command_id):
        """Append a command. Only the leader may do it; commit happens by quorum."""
        if self.role != LEADER:
            return {"ok": False, "error": "not leader", "leader": self.leader, "redirect": self.leader}, []
        if command_id in self.pending:
            idx = self.pending[command_id]
            result = self.sm.state["applied"].get(command_id)
            return {"ok": True, "index": idx, "committed": result is not None,
                    "result": result, "duplicate": True}, []
        e = self._entry(K_COMMAND, command, command_id)
        self.pending[command_id] = e["index"]
        self._advance_commit()
        out = [self._sync_peer(n, [e]) for n in self.peers()]
        return self._commit_status(command_id), out

    def change_membership(self, cnew, joint=True, command_id=None):
        if self.role != LEADER:
            return {"ok": False, "error": "not leader", "leader": self.leader, "redirect": self.leader}, []
        cold = sorted(self.config_members())
        cnew = sorted(set(cnew))
        if set(cold) == set(cnew) and not joint:
            return {"ok": True, "index": 0, "nochange": True}, []
        e = self._entry(K_MEMBERSHIP, {"cold": cold, "cnew": cnew, "joint": joint}, command_id)
        if command_id:
            self.pending[command_id] = e["index"]
        self._advance_commit()
        out = [self._sync_peer(n, [e]) for n in self.peers()]
        return self._commit_status(command_id), out

    def _commit_status(self, command_id):
        result = self.sm.state["applied"].get(command_id)
        return {"ok": True, "index": self.pending.get(command_id),
                "committed": result is not None, "result": result}

    def commit_status(self, command_id):
        result = self.sm.state["applied"].get(command_id)
        return {"ok": True, "committed": result is not None, "result": result,
                "index": self.pending.get(command_id)}

    def read_state(self, now=None):
        now = self.clock() if now is None else now
        return {
            "role": self.role,
            "term": self.term,
            "leader": self.leader,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "members": self.config_members(),
            "configs": list(self.configs),
            "config": self.sm.config_view() if hasattr(self.sm, "config_view") else {},
            "messages": self.sm.messages(),
            "proposals": self.sm.proposals(),
            "lease_valid": self.lease_valid(now),
        }

    def status(self, now=None):
        now = self.clock() if now is None else now
        return {
            "ok": True,
            "node_id": self.node_id,
            "addr": self.addr,
            "role": self.role,
            "term": self.term,
            "leader": self.leader,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_index": self.last_index(),
            "members": self.config_members(),
            "match": dict(self.match),
            "next": dict(self.next),
            "addrs": dict(self.addrs),
            "configs": list(self.configs),
            "stats": dict(self.stats),
            "lease_valid": self.lease_valid(now),
            "snapshot_index": (self.storage.snapshot() or {}).get("index", 0),
            "log_size": self.storage.log_size(),
        }
