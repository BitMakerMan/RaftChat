# 06 — Network and runtime

## Transport (`raft/net.py`)

Plain HTTP with JSON bodies and CORS headers. No framework, no connection pool, no retries — a
request either answers within the timeout or returns `None`.

```python
net.post(base, path, payload, timeout=1.0, host="127.0.0.1")   # -> dict | None
net.get(base, path, payload=None, timeout=1.0)                 # -> dict | None
net.start_server(host, port, handler)        # handler(path, payload) -> dict
net.base_url(host, port)                     # "http://host:port"
```

Raft message kinds map to fixed paths:

| Kind | Path |
|---|---|
| `vote` / `vote_ack` | `/raft/vote` |
| `append` / `append_ack` | `/raft/append` |
| `snapshot` / `snapshot_ack` | `/raft/snapshot` |
| `check` / `check_ack` | `/raft/check` |

`PATH_KIND` is derived only from the non-ack kinds, so an incoming request is recognised as the
request kind and the reply is returned directly to the caller.

## Node paths (`NodeRuntime.handler`)

| Path | Purpose |
|---|---|
| `/raft/vote`, `/raft/append`, `/raft/snapshot`, `/raft/check` | peer RPC |
| `/client/command`, `/client/submit` | execute a command (leader only) |
| `/client/read`, `/client/state` | linearizable read (leader only) |
| `/cluster/join`, `/cluster/leave`, `/cluster/members` | membership |
| `/node/status`, `/node/stats` | node status and counters |
| `/client/commit` | commit status for a `command_id` |

## The pump loop

```python
while True:
    outbox = core.tick()          # election timeout or heartbeat due
    for m in outbox: await send(m)
    await asyncio.sleep(heartbeat_ms / 1000 / 4)
```

One event loop per node. The core is never mutated from two places at once, so there are no
locks and no races in the consensus code. Exceptions inside the pump are counted and swallowed:
a node must not die because one request failed.

## Sending

`_send(m)` looks up `core.addrs[to]`, posts to `KIND_PATH[kind]`, adds `from` and `addr` to the
payload (this is how addresses are learned), and feeds the response back into the core as the
`*_ack` message. Unreachable or self-addressed targets return `None`.

## Write path

```text
client_submit(command, command_id)
  -> core.submit()            (rejects if not leader)
  -> flush replication
  -> if already committed: return
  -> otherwise _wait_commit(command_id) polls while pumping, up to commit_timeout (2 s)
```

If the commit does not arrive in time, the reply carries
`"warning": "commit not reached in time"` — the entry is still in the log and will be committed
later, so the client must retry with the same `command_id`.

## Read path

`linearizable_read()`:

- non-leader → `{"ok": False, "error": "not leader", "leader": ..., "read_local": ...}`;
- leader with valid lease → serve state directly (`verified: true`);
- leader with expired lease → re-verify leadership with a `check` to a majority of every quorum
  set before answering; if verification fails, the read is refused.

## Client API

```text
POST /client/command  {"command": {"type": "submit", "author": "alice", ...}, "command_id": "c1"}
POST /client/read     {}
GET  /client/commit?command_id=c1
POST /cluster/join    {"cnew": ["n1","n2","n4"], "joint": true}
POST /cluster/leave   {"cnew": ["n1","n2"], "joint": false}
POST /node/status     {}
```
