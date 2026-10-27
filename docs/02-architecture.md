# 02 — Architecture

## Layers

```text
                 ┌───────────────────────────────────────────────┐
   client        │ cli.py            web/ (HTML + app.js)        │
                 └───────────────┬───────────────────────────────┘
                                 │ JSON over HTTP
                 ┌───────────────▼───────────────────────────────┐
   control       │ raft/supervisor.py   registry, spawn, kill,   │
                 │ dynamic discovery, static UI serving          │
                 └───────────────┬───────────────────────────────┘
                                 │
                 ┌───────────────▼───────────────────────────────┐
   runtime       │ raft/runtime.py      single asyncio loop      │
                 │ pump: tick -> outbox -> HTTP -> ack           │
                 └───────────────┬───────────────────────────────┘
                 ┌───────────────▼───────────────────────────────┐
   consensus     │ raft/core.py         pure, deterministic      │
                 └───────────────┬───────────────────────────────┘
        ┌────────────────────────┴───────────────────────────────┐
   state          │ statemachine.py            storage.py         │
                 │ chat FSM + proposals       Memory / SQLite     │
                 └────────────────────────────────────────────────┘
```

The arrows are one-directional on purpose: the consensus core never knows about HTTP, and the
state machine never knows about transport.

## Module responsibilities

| Module | Lines | Role |
|---|---|---|
| `raft/core.py` | 592 | Pure Raft: term, vote, pre-vote, election, heartbeat, replication, commit, snapshot, joint membership |
| `raft/statemachine.py` | 282 | Chat state, message FSM, proposals, decisions, JSON view |
| `raft/storage.py` | 199 | `Storage` interface + `MemoryStorage` + `SqliteStorage` |
| `raft/net.py` | 154 | Minimal HTTP client + path mapping |
| `raft/runtime.py` | 183 | Wires the core to a real asyncio HTTP server and pump loop |
| `raft/supervisor.py` | 225 | Process registry, spawn/kill, discovery, static UI |
| `tests/sim.py` | 139 | Deterministic simulated cluster for unit tests |
| `cli.py` | 137 | Client commands |
| `run_node.py` | 64 | Node entrypoint |
| `run_supervisor.py` | 40 | Supervisor entrypoint |
| `run_tests.py` | 22 | Anti-regression runner |

## Invariants

1. **The core is pure.** Handlers return an outbox of messages; they never call a socket, a
   thread, `time.time()`, or `random` directly. `clock`, `rng`, `storage` and `sm` are injected.
   This is what makes election tests deterministic.
2. **One event loop per node.** The runtime is single-threaded, so the core never sees
   concurrent mutations. Acks are processed by `handle()` on the same loop.
3. **Storage is versioned.** Entries carry `(index, term)`. Conflicting entries are truncated,
   never merged.
4. **Membership lives in the log.** The only source of truth for who is a member is the ordered
   list of `membership` entries. `initial_config` is only the bootstrap config (`index 0`).
5. **The state machine sees the same membership as the core** through
   `sm.load_configs(core.configs)`.
6. **Only the leader writes.** `submit()` and `change_membership()` return
   `{"ok": False, "error": "not leader", "leader": ..., "redirect": ...}` on followers.
7. **Reads are linearizable**: the leader serves reads only while its lease is valid
   (`lease_expired = clock + 2 * heartbeat_ms`).

## Data flow for one message

```text
client ─POST /api/command─▶ supervisor ─▶ leader /client/command
   leader: append entry (kind=command)
   leader: replicate to peers via /raft/append
   quorum stored it → commit_index advances → sm.apply(entry)
   leader returns {"committed": true, "index": N, "result": {...}}
```

## Data flow for a contested decision

```text
client ─propose─▶ leader ─▶ entry(kind=command, type=propose) replicated & applied
client ─vote (n1)─▶ entry replicated & applied → not decided yet
client ─vote (n2)─▶ entry applied → quorum reached → message state changes
```

The decision is taken **at apply time**, identically on every node, because the vote order comes
from the replicated log, not from the arrival order of HTTP requests.

## File tree

Relative to the git root, with no hardcoded checkout name:

```text
<git root>/
├── .gitignore
├── README.md
├── run_tests.py
├── run_supervisor.py
├── run_node.py
├── cli.py
├── raft/
│   ├── __init__.py
│   ├── core.py
│   ├── net.py
│   ├── runtime.py
│   ├── statemachine.py
│   ├── storage.py
│   └── supervisor.py
├── tests/
│   ├── __init__.py
│   ├── sim.py
│   ├── test_cli.py
│   ├── test_core.py
│   ├── test_docs.py
│   ├── test_runtime.py
│   ├── test_statemachine.py
│   ├── test_storage.py
│   ├── test_supervisor.py
│   └── test_webui.py
├── web/
│   ├── index.html
│   ├── app.js
│   └── style.css
└── docs/
    ├── 01-overview.md
    ├── 02-architecture.md
    ├── 03-raft-core.md
    ├── 04-state-machine.md
    ├── 05-storage.md
    ├── 06-network-and-runtime.md
    ├── 07-supervisor-and-api.md
    ├── 08-cli.md
    ├── 09-web-ui.md
    ├── 10-testing.md
    ├── 11-runbook.md
    ├── 12-design-decisions.md
    ├── 13-limitations.md
    └── 14-contributing.md
```

Generated at runtime (not part of the project): `data/node-<id>.db`.
