# 08 — CLI

```bash
python cli.py <command> [--supervisor URL] [--node URL]
```

## Global flags

| Flag | Default | Meaning |
|---|---|---|
| `--supervisor` | env `RAFT_SUPERVISOR`, else `http://127.0.0.1:8200` | control plane used for every command |
| `--node` | env `RAFT_NODE` | talk directly to one node URL instead of the supervisor |

When `--node` is set, the CLI calls that node's own endpoints (`/client/command`, `/client/read`,
`/node/status`), so a follower write is rejected with a redirect instead of being proxied.

## Commands

| Command | Flags | Endpoint |
|---|---|---|
| `send` | `--user` (required), `--text` (required), `--to` (default `all`), `--cid`, `--msg` | `POST /api/command` |
| `read` | — | `GET /api/read` |
| `status` | — | `GET /api/status` |
| `propose` | `--user` (required), `--msg` (required), `--state` (required), `--proposal` | `POST /api/command` |
| `vote` | `--voter` (required), `--proposal` (required) | `POST /api/command` |
| `spawn` | `--id` (required), `--port` (int, optional) | `POST /admin/spawn` |
| `kill` | `--id` (required), `--force` (flag) | `POST /admin/kill` |

Notes:

- `--cid` is the `command_id`. Without it the CLI generates a UUID; pass it explicitly to test
  client retry idempotency (same `--cid` twice must return the same result).
- `--msg` is the message id. Without it, `send` uses the `command_id` as the message id.
- `--state` on `propose` must be a contested target: `delivered`, `ack`, `flagged`, `resolved`,
  `dropped` (`delete`/`drop` are aliases of `dropped`).
- Direct transitions (`read` by a recipient, `edit` by the author) are not exposed by the CLI:
  the state machine rejects them as proposals. Use the API directly, e.g.
  `POST /api/command {"type": "transition", "msg_id": "m1", "to": "read", "actor": "bob"}`.

Every request uses `cli.call(..., timeout=10.0)`, which is `net.post`/`net.get` with
`timeout=10.0`. The transport default is `timeout=1.0` (see
[06 — Network and runtime](06-network-and-runtime.md)); a longer value is used here because a
command may wait for a commit.

## Exit codes

- `0` — the response is `ok` and the inner `result.ok` is not `false`.
- `1` — anything else (no leader, not leader, invalid transition, unknown node).

Output is pretty-printed JSON, keys sorted.

## Examples

```bash
python cli.py status
python cli.py send --user alice --to bob --text "hello" --msg m1 --cid c1
python cli.py propose --user n1 --msg m1 --state delivered --proposal p1
python cli.py vote --voter n1 --proposal p1
python cli.py vote --voter n2 --proposal p1
python cli.py read
python cli.py spawn --id n4 --port 8304
python cli.py kill --id n4
python cli.py kill --id n4 --force
python cli.py --node http://127.0.0.1:8301 read
python cli.py --supervisor http://127.0.0.1:8200 read
```

A full flow, from a clean cluster:

```bash
python cli.py spawn --id n1
python cli.py spawn --id n2
python cli.py spawn --id n3
python cli.py send --user alice --to bob --text "hello" --msg m1 --cid c1
python cli.py propose --user n1 --msg m1 --state delivered --proposal p1
python cli.py vote --voter n1 --proposal p1
python cli.py vote --voter n2 --proposal p1
python cli.py read
python cli.py kill --id n3
python cli.py read
```
