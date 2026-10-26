# Raft Chatroom

A chatroom where **every participating node is a Raft cluster node**. Nodes
collectively decide the state of messages: a message is only what the cluster
has agreed it is.

Python 3.13, standard library only. No third-party dependencies.

```
run_supervisor.py   process registry + spawn/kill + web UI + client API
run_node.py         one cluster node (its own SQLite file)
cli.py              client for the cluster
web/                HTML/JS UI served by the supervisor
raft/               core, runtime, storage, state machine, net, supervisor
tests/              anti-regression suite (run_tests.py)
```

## Run

```bash
python run_supervisor.py 8200          # UI + admin API at http://127.0.0.1:8200/
python cli.py --supervisor http://127.0.0.1:8200 spawn --id n1
python cli.py --supervisor http://127.0.0.1:8200 spawn --id n2
python cli.py --supervisor http://127.0.0.1:8200 spawn --id n3
python cli.py --supervisor http://127.0.0.1:8200 send --user alice --to bob --text "ciao" --msg m1 --cid c1
python cli.py --supervisor http://127.0.0.1:8200 propose --user n1 --msg m1 --state delivered --proposal p1
python cli.py --supervisor http://127.0.0.1:8200 vote --voter n1 --proposal p1
python cli.py --supervisor http://127.0.0.1:8200 vote --voter n2 --proposal p1
python cli.py --supervisor http://127.0.0.1:8200 read
python cli.py --supervisor http://127.0.0.1:8200 kill --id n3
```

Or start nodes manually (dynamic discovery works through self-registration and
seed/peer hints):

```bash
python run_node.py --id n1 --port 9001 --supervisor http://127.0.0.1:8200
python run_node.py --id n2 --port 9002 --peers n1=http://127.0.0.1:9001
python run_node.py --id n3 --port 9003 --peers n1=http://127.0.0.1:9001,n2=http://127.0.0.1:9002
```

Each node writes `data/node-<id>.db` (WAL mode): metadata, log entries, snapshots.

## Tests

```bash
python run_tests.py
```

39 tests: pure core (election, prevote, replication, commit, joint membership,
compaction, snapshot install, reads), state machine, SQLite storage, real HTTP
runtime, supervisor spawn/kill with real processes + dynamic discovery, CLI over
HTTP, web UI contract. **Tests are guards: a red test means the implementation
is wrong — never edit a test to make it pass.**

## Message state machine

```
committed -> delivered -> read -> ack
                    \-> flagged -> resolved
                    \-> dropped (tombstone)
edit: appends a revision, does not change state
```

- `committed` = the entry reached a Raft quorum.
- `delivered`, `read`, `ack`, `flagged`, `resolved` are application decisions.
- Contested transitions (`delivered`, `ack`, `flagged`, `resolved`, `dropped`)
  need a proposal plus a decision from a majority in **every active config**.
- Direct transitions: `read` only by a recipient, `edit` only by the author,
  `dropped` by the author without a proposal.
- Tombstoned messages cannot be changed. Replay of the same `command_id` is
  idempotent.

## API

Supervisor (`http://127.0.0.1:8200`):

```
GET  /                      web UI
GET  /admin/nodes           discovered nodes + registry
POST /admin/register        self-registration (node id + url)
POST /admin/spawn           {node_id, port?} -> new process
POST /admin/kill            {node_id, force?} -> safe kill (leave first)
POST /api/command           {type: submit|transition|propose|vote, ...}
GET  /api/read              linearizable read from the leader
POST /api/membership        {cnew: [...], joint: true}
GET  /api/cluster           leader, members, per-node status
GET  /api/status            per-node Raft status
```

Per-node Raft paths: `/raft/vote`, `/raft/append`, `/raft/snapshot`,
`/raft/check`; client paths: `/client/command`, `/client/read`,
`/client/commit`, `/cluster/join`, `/cluster/leave`, `/node/status`.

## Design decisions

- **Pure `RaftCore`**: no sockets, threads, wall clock or uncontrolled
  randomness; `clock`, `rng`, `storage`, `state machine` are injected, so the
  same core is testable deterministically (see `tests/sim.py`).
- **Writes only on the leader**; followers reject and redirect.
- **Linearizable reads** on the leader only, verified by a lease or a quorum `check`.
- **Pre-vote** is enabled and never inflates the term.
- **A heartbeat is not a vote**: `voted_for` changes only when a vote is granted.
- **Joint membership**: a change is committed when a majority of *both* configs
  acknowledges it; a new node joins as a non-member and learns membership from
  the log, so a join can complete only once the new node replicates.
- **Raft safety**: entries from a previous term are never committed directly.
- **Disk storage per node** (SQLite + WAL), snapshotting and compaction, with a
  `MemoryStorage` variant for deterministic tests.
- **Supervisor**: real OS processes, self-registration for dynamic discovery,
  safe kill = commit a membership leave through Raft before terminating.
  `force: true` skips the leave and can cost the cluster its quorum.
- **Single event loop per node**; no threads inside a node.
