"""Chat state machine.

Everything the cluster decides is a committed log entry, so every node applies
the same entries in the same order and converges on an identical state.

Entry kinds:
  membership -> config change (joint consensus)
  noop       -> leader's first-entry marker
  command    -> application command

Command types:
  submit    : create a message                       (direct, Raft-ordered)
  transition: move a message in the FSM              (direct when not contested)
  propose   : open a proposal for a contested decision
  vote      : a member votes on a proposal; when a quorum of *each* config of
              the current joint configuration has voted, the decision applies

Message FSM:

    committed -> delivered -> read -> ack
       |            |          |      |
       +------------+----------+------+--> flagged -> resolved
                                    (any alive state) -> dropped (tombstone)

`edit` appends a revision and does not change the state.
"""

ALIVE_STATES = ("committed", "delivered", "read", "ack", "flagged", "resolved")
TOMBSTONE = "dropped"

ALLOWED_TRANSITIONS = {
    "committed": {"delivered", "flagged", "dropped"},
    "delivered": {"read", "flagged", "dropped"},
    "read": {"ack", "flagged", "dropped"},
    "ack": {"flagged", "dropped"},
    "flagged": {"resolved", "dropped"},
    "resolved": {"dropped"},
    TOMBSTONE: set(),
}

# decisions that require an application-level quorum
CONTESTED = {"delivered", "ack", "flagged", "resolved", "dropped"}

# client-friendly verbs -> canonical target states
ALIAS = {"delete": "dropped", "drop": "dropped"}


class ChatStateMachine:
    def __init__(self, node_id=""):
        self.node_id = node_id
        self.state = {
            "configs": [],
            "messages": {},
            "proposals": {},
            "applied": {},
            "applied_entries": {},
            "history": [],
        }

    # ---------------- membership ----------------

    def load_configs(self, configs):
        self.state["configs"] = [dict(c) for c in configs]

    def quorum_sets(self):
        if not self.state["configs"]:
            return []
        cfg = self.state["configs"][-1]
        sets = []
        if cfg.get("joint"):
            if cfg.get("cold"):
                sets.append(set(cfg["cold"]))
            if cfg.get("cnew"):
                sets.append(set(cfg["cnew"]))
        else:
            sets.append(set(cfg.get("cnew") or cfg.get("cold") or []))
        return [s for s in sets if s]

    def active_config(self):
        sets = self.quorum_sets()
        return sorted(set().union(*sets)) if sets else []

    def is_member(self, node_id):
        return any(node_id in s for s in self.quorum_sets())

    # ---------------- apply ----------------

    def apply(self, e):
        """Apply one committed entry (idempotent). Returns the client-visible result."""
        cid = e.get("command_id")
        if cid and cid in self.state["applied"]:
            return self.state["applied"][cid]

        p = e.get("payload") or {}
        kind = e.get("kind")
        if kind == "membership":
            result = self._apply_membership(p, e)
        elif kind == "noop":
            result = None
        elif kind == "command":
            result = self._apply_command(e, p)
        else:
            result = None

        if cid:
            self.state["applied"][cid] = result
        self.state["history"].append({"index": e["index"], "kind": kind, "payload": p})
        return result

    def _apply_membership(self, p, e):
        applied = self.state.setdefault("applied_entries", {})
        if e["index"] in applied:
            return applied[e["index"]]
        cfg = {
            "cold": sorted(p.get("cold") or []),
            "cnew": sorted(p.get("cnew") or []),
            "joint": bool(p.get("joint")),
            "index": len(self.state["configs"]) + 1,
            "entry_index": e["index"],
        }
        prev = self.state["configs"]
        mirrored = next((c for c in prev if c.get("entry_index") == e["index"]), None)
        if mirrored is not None:
            # the core mirrored this entry's config before the sm applied it: first application
            result = {"ok": True, "config": mirrored["index"],
                      "members": sorted(set(mirrored["cold"]) | set(mirrored["cnew"]))}
        elif prev and set(prev[-1]["cold"]) == set(cfg["cold"]) and set(prev[-1]["cnew"]) == set(cfg["cnew"]) \
                and prev[-1]["joint"] == cfg["joint"]:
            result = {"ok": True, "duplicate": True, "config": prev[-1]["index"]}
        else:
            self.state["configs"].append(cfg)
            result = {"ok": True, "config": cfg["index"], "members": sorted(set(cfg["cold"]) | set(cfg["cnew"]))}
        applied[e["index"]] = result
        return result

    def _apply_command(self, e, p):
        ctype = p.get("type")
        if ctype == "submit":
            return self._submit(p, e)
        if ctype == "transition":
            return self._transition(p, e)
        if ctype == "propose":
            return self._propose(p, e)
        if ctype == "vote":
            return self._vote(p, e)
        return {"ok": False, "error": f"unknown command type {ctype}"}

    def _submit(self, p, e):
        # a submit without an explicit msg_id falls back to the command id
        mid = p.get("msg_id") or e.get("command_id")
        if not mid:
            return {"ok": False, "error": "msg_id (or command_id) required"}
        if mid in self.state["messages"]:
            return {"ok": False, "error": "message already exists", "msg_id": mid}
        self.state["messages"][mid] = {
            "id": mid,
            "author": p.get("author", "?"),
            "recipients": sorted(p.get("recipients") or []),
            "text": p.get("text", ""),
            "state": "committed",
            "created_index": e["index"],
            "revisions": [p.get("text", "")],
            "transitions": [{"index": e["index"], "to": "committed", "by": p.get("author"), "via": "direct"}],
            "tombstone": False,
            "dropped_by": None,
        }
        return {"ok": True, "msg_id": mid, "state": "committed", "index": e["index"]}

    def _transition(self, p, e):
        m = self.state["messages"].get(p.get("msg_id"))
        if m is None:
            return {"ok": False, "error": "no such message"}
        target = ALIAS.get(p.get("to"), p.get("to"))
        actor = p.get("actor")
        if target not in ALLOWED_TRANSITIONS and target != "edit":
            return {"ok": False, "error": f"invalid target state {target}"}
        if target in CONTESTED and not (target == "dropped" and actor == m["author"]):
            return {"ok": False, "error": f"{target} requires a quorum decision (proposal)"}
        if m["tombstone"]:
            return {"ok": False, "error": "tombstoned message cannot be changed"}
        if target == "edit":
            if actor != m["author"]:
                return {"ok": False, "error": "only the author can edit"}
            m["revisions"].append(p.get("text", ""))
            m["transitions"].append({"index": e["index"], "to": "edited", "by": actor, "via": "direct"})
            return {"ok": True, "msg_id": m["id"], "state": m["state"], "edited": True, "index": e["index"]}
        if target == "read" and actor not in m["recipients"]:
            return {"ok": False, "error": "only a recipient can mark read"}
        if target not in ALLOWED_TRANSITIONS[m["state"]]:
            return {"ok": False, "error": f"cannot go {m['state']} -> {target}"}
        if target == "dropped":
            m["tombstone"] = True
            m["dropped_by"] = actor
        m["state"] = target
        m["transitions"].append({"index": e["index"], "to": target, "by": actor, "via": "direct"})
        return {"ok": True, "msg_id": m["id"], "state": target, "index": e["index"]}

    def _propose(self, p, e):
        pid = p.get("proposal_id")
        mid = p.get("msg_id")
        if mid not in self.state["messages"]:
            return {"ok": False, "error": "no such message"}
        decision = ALIAS.get(p.get("to"), p.get("to"))
        if decision not in CONTESTED:
            return {"ok": False, "error": f"{decision} is not a contested decision"}
        self.state["proposals"][pid] = {
            "id": pid, "msg_id": mid, "type": decision, "by": p.get("by"),
            "votes": set(), "decided": False, "outcome": None, "index": e["index"],
        }
        return {"ok": True, "proposal_id": pid, "decided": False}

    def _vote(self, p, e):
        prop = self.state["proposals"].get(p.get("proposal_id"))
        if prop is None:
            return {"ok": False, "error": "no such proposal"}
        voter = p.get("voter")
        if not self.is_member(voter):
            return {"ok": False, "error": "voter is not a member of the cluster"}
        prop["votes"].add(voter)
        if self._has_decision(prop):
            m = self.state["messages"].get(prop["msg_id"])
            if m is not None and not m["tombstone"] and prop["type"] in ALLOWED_TRANSITIONS[m["state"]]:
                if prop["type"] == TOMBSTONE:
                    m["tombstone"] = True
                    m["dropped_by"] = prop["by"]
                    m["state"] = TOMBSTONE
                else:
                    m["state"] = prop["type"]
                m["transitions"].append({"index": e["index"], "to": prop["type"],
                                         "by": prop["by"], "via": "quorum"})
                prop["outcome"] = "accepted"
            else:
                prop["outcome"] = "rejected"
            prop["decided"] = True
            return {"ok": True, "proposal_id": prop["id"], "decided": True, "index": e["index"]}
        return {"ok": True, "proposal_id": prop["id"], "decided": False, "index": e["index"]}

    def _has_decision(self, prop):
        sets = self.quorum_sets()
        if not sets:
            return False
        for s in sets:
            if len(prop["votes"] & s) <= len(s) // 2:
                return False
        return True

    # ---------------- read views ----------------

    def messages(self):
        out = []
        for m in sorted(self.state["messages"].values(), key=lambda m: m["created_index"]):
            out.append(dict(m, current_text=(m["revisions"] or [""])[-1]))
        return out

    def proposals(self):
        # votes are exposed as a sorted list so the JSON API is stable
        return [dict(p, votes=sorted(p["votes"])) for p in
                sorted(self.state["proposals"].values(), key=lambda p: p["index"])]

    def config_view(self):
        cfg = self.state["configs"][-1] if self.state["configs"] else {}
        return dict(cfg, members=self.active_config())

    def to_dict(self):
        return {
            "configs": self.state["configs"],
            "messages": self.state["messages"],
            "proposals": {k: dict(v, votes=sorted(v["votes"])) for k, v in self.state["proposals"].items()},
            "applied": self.state["applied"],
            "applied_entries": self.state.get("applied_entries", {}),
            "history": self.state["history"],
        }

    def load(self, data):
        self.state = {
            "configs": data.get("configs", []),
            "messages": data.get("messages", {}),
            "proposals": {k: dict(v, votes=set(v.get("votes", []))) for k, v in data.get("proposals", {}).items()},
            "applied": data.get("applied", {}),
            "applied_entries": data.get("applied_entries", {}),
            "history": data.get("history", []),
        }
