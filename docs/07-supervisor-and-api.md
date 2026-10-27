# 07 — Supervisor and HTTP API

The supervisor is the control plane: it keeps the node registry, spawns and kills real
processes, discovers nodes, and proxies client requests to the current leader. It also serves
the web UI.

```bash
python run_supervisor.py 8200
python run_supervisor.py 8200 --data-dir /tmp/raftdata --web-dir web
```

Environment variables: `SUPERVISOR` / `RAFT_SUPERVISOR` set the default target for the CLI.

---

## Endpoints

| Method | Path | Behaviour |
|---|---|---|
| `GET` | `/` | web UI (`web/index.html`) |
| `GET` | `/<file>` | static file from `web/` (`.css`, `.js`, `.html`) |
| `GET` | `/admin/nodes` | `{"ok": true, "nodes": {id: {url, pid, status}}, "registry": {...}}` |
| `POST` | `/admin/register` | `{"node_id": "n1", "url": "http://127.0.0.1:8301"}` — dynamic discovery |
| `POST` | `/admin/spawn` | `{"node_id": "n4", "port": 8304}` |
| `POST` | `/admin/kill` | `{"node_id": "n4", "force": false}` |
| `POST` | `/api/command` | proxied to the leader's `/client/command` |
| `GET` | `/api/read` | proxied to the leader's `/client/read` |
| `POST` | `/api/membership` | proxied to the leader's `/cluster/join` |
| `GET` | `/api/cluster` | `{"ok": true, "leader": "n1", "members": [...], "nodes": {id: status}}` |
| `GET` | `/api/status` | `{"ok": true, "leader": "n1", "leader_url": "...", "nodes": {id: status}}` |

Aliases accepted by the handler: `/client/read`, `/client/command`, `/cluster/join`,
`/cluster/leave`, `/node/cluster`.

If no leader is reachable, the proxy endpoints return
`{"ok": false, "error": "no leader reachable", "nodes": {...}}`.

---

## Dynamic discovery

There is no static cluster file. A node announces itself:

```text
run_node.py --id n4 --supervisor http://127.0.0.1:8200
      └─ POST /admin/register {"node_id": "n4", "url": "http://127.0.0.1:8304"}
```

The supervisor keeps `registry[node_id] = {url, pid}`. `pid` is known only for processes it
spawned; externally started nodes appear with `pid: null` and are still fully usable.

Addresses also propagate inside RPC payloads (`payload.addr`), so every node learns the real URL
of every peer it talks to.

---

## Spawn

```text
POST /admin/spawn {"node_id": "n4"}
  1. refuse if node_id is already running
  2. start a real process: run_node.py --id n4 --port <port or 0> --config <existing members>
     --peers <id=url,...> --seeds <urls> --data-dir <supervisor data_dir> --supervisor <sup url>
  3. read the first stdout line: {"node_id","port","url","initial_config","pid"}
  4. register the node
  5. wait for a leader (up to 8 s), then POST /cluster/join with cnew = members + n4
```

The new node starts as a **non-member** (`--config` lists the existing nodes), so it cannot vote
before it replicates the membership entry. `join` is `null` when the node is the first one or when
no leader is available.

`--port 0` means the OS assigns the port; the real port comes back in the response.

## Kill

Default is **safe kill**:

```text
POST /admin/kill {"node_id": "n4"}
  1. find the leader, compute cnew = members - {n4}
  2. commit the membership change through Raft (POST /cluster/leave, joint=false)
     - if not committed: {"ok": false, "error": "membership change not committed",
                          "hint": "use force:true to kill anyway"}
     - if cnew would be empty: the membership step is skipped (last node)
  3. terminate the process (taskkill on Windows, SIGTERM elsewhere) and wait up to 5 s
  4. {"ok": true, "node_id": "n4", "killed": true}
```

`force: true` skips step 1. That is a documented risk: killing a member without committing the
leave can leave the cluster below quorum until the entry is decided.

Killing an unknown node returns `{"ok": false, "error": "n9 is not registered"}` — a second kill
of the same node is rejected, not silently accepted.

---

## Node status payload

Returned by `/node/status` and embedded in `/api/status` and `/api/cluster`:

```json
{"ok": true, "node_id": "n1", "addr": "http://127.0.0.1:8301", "role": "leader",
 "term": 2, "leader": "n1", "commit_index": 12, "last_applied": 12, "last_index": 12,
 "members": ["n1","n2","n3"], "match": {"n2": 12}, "next": {"n2": 13},
 "addrs": {"n1": "..."}, "configs": [{"cold":["n1"],"cnew":["n1"],"joint":false,"index":0}],
 "stats": {"votes_won": 1, "appends_sent": 40, "commit_advanced": 12, "snapshots_taken": 0,
           "snapshots_installed": 0, "rejected": 0, "elections_started": 1, "prevotes_started": 1},
 "lease_valid": true, "snapshot_index": 0, "log_size": 12}
```

## Read payload

`GET /api/read` returns the leader's read state:

```json
{"ok": true, "verified": true, "state": {
  "role": "leader", "term": 2, "leader": "n1", "commit_index": 12, "last_applied": 12,
  "members": ["n1","n2","n3"], "configs": [...], "config": {...},
  "messages": [...], "proposals": [...], "lease_valid": true}}
```
