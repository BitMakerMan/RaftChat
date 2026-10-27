# 11 — Runbook

## Start

```bash
python run_supervisor.py 8200
```

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `port` (positional) | `8200` (env `RAFT_SUPERVISOR_PORT`) | listen port, `0` = OS assigned |
| `--host` | `127.0.0.1` | bind address |
| `--data-dir` | `data` (relative to the git root) | directory holding `node-<id>.db` |
| `--web-dir` | `web` (relative to the git root) | static UI directory |

## Start nodes

The supervisor can spawn nodes for you:

```bash
python cli.py spawn --id n1
python cli.py spawn --id n2 --port 8302
```

Or start them yourself (they self-register, so the supervisor discovers them):

```bash
python run_node.py --id n1 --port 8301 --supervisor http://127.0.0.1:8200
python run_node.py --id n2 --port 8302 --supervisor http://127.0.0.1:8200 --peers n1=http://127.0.0.1:8301
python run_node.py --id n3 --port 8303 --supervisor http://127.0.0.1:8200 --config n1,n2
```

Node flags:

| Flag | Default | Meaning |
|---|---|---|
| `--id` | required | node id |
| `--port` | `0` | listen port, `0` = OS assigned |
| `--seeds` | empty | comma separated node URLs used to bootstrap |
| `--peers` | empty | comma separated `id=url` pairs, seeds the address table |
| `--config` | `[--id]` | initial membership the node starts from |
| `--supervisor` | empty | supervisor URL for self-registration |
| `--data-dir` | `data` (relative to the git root) | where `node-<id>.db` lives |
| `--heartbeat-ms` | 100 | heartbeat interval |
| `--election-min-ms` | 300 | election window lower bound |
| `--election-max-ms` | 600 | election window upper bound |
| `--snapshot-threshold` | 64 | compaction threshold |

The node prints one JSON line on stdout before serving:

```json
{"node_id": "n1", "port": 8301, "url": "http://127.0.0.1:8301",
 "initial_config": ["n1"], "pid": 22928}
```

That line is what the supervisor reads, so a node started by hand and a node spawned by the
supervisor behave identically.

## Day-to-day

```bash
python cli.py status                      # who is leader, who is a member
python cli.py read                        # linearizable read from the leader
python cli.py send --user alice --to bob --text "hello" --msg m1 --cid c1
python cli.py propose --user n1 --msg m1 --state delivered --proposal p1
python cli.py vote --voter n1 --proposal p1
python cli.py kill --id n3
```

## Restarting a node

Stop the process and start it with the same `--id` and `--data-dir`. It recovers term, vote,
commit index, log, snapshot and chat state from disk. The port may change: peers learn the new
address from the next RPC payload.

## Partition and recovery

```bash
# simulate in the deterministic test harness
cluster.cut("n1", "n2")     # messages between n1 and n2 are dropped
cluster.heal("n1", "n2")
cluster.kill("n1")
cluster.join("n4")
```

What to expect:

- a minority side cannot elect, cannot commit, and cannot serve linearizable reads;
- pre-votes do not inflate the term, so when the partition heals the old leader can step down
  cleanly;
- once a quorum stores an entry, the commit advances on every node that has it;
- a lagging node behind the snapshot receives the snapshot, not a replay of the whole log.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `{"ok": false, "error": "not leader"}` | request hit a follower; the CLI answers with `leader`/`redirect` |
| `{"ok": false, "error": "no leader reachable"}` | no quorum, or every node is down |
| `"warning": "commit not reached in time"` | entry appended, quorum not yet reached; retry with the same `command_id` |
| `{"ok": false, "error": "leader lease not verified"}` | stale leader, quorum check failed |
| `membership change not committed` on kill | the leave was not decided; the supervisor refuses unless `force: true` |
| `{"ok": false, "error": "n4 is not registered"}` | node was never spawned or already killed |
| `snapshot_required` in an append reply | the follower is behind its snapshot |

## Test suite

```bash
python run_tests.py
```

The suite uses OS-assigned ports and temporary data directories; it never touches `data/` and
never needs a browser.
