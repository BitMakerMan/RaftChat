# Raft Chatroom

A chatroom where **every participant node is a Raft cluster node**. Nodes collectively
decide the state of every message through replicated entries and quorum decisions.

The project is a single, dependency-free Python 3 package: no frameworks, no third-party
libraries, no network calls in unit tests.

```text
nodes      : one process per cluster node, each with its own SQLite file
consensus  : leader election + log replication + commit + snapshots (Raft)
state      : chat state machine applied on every node, decided by quorum
interface  : supervisor (spawn/kill/discovery) + CLI + HTML web UI
tests      : 41 deterministic anti-regression tests + 16 docs-guard checks (57)
```

## Quick start

Every command and every path in this manual is relative to the **git root** (the folder you
clone). Do not hardcode the checkout name — start from the git root:

```bash
python run_supervisor.py 8200            # supervisor + web UI
# open http://127.0.0.1:8200/            (Spawn nodes, send messages, decide by quorum)

python cli.py send --user alice --to bob --text "hello" --msg m1 --cid c1
python cli.py propose --user n1 --msg m1 --state delivered --proposal p1
python cli.py vote --voter n1 --proposal p1
python cli.py vote --voter n2 --proposal p1
python cli.py read
python cli.py status
python cli.py spawn --id n4
python cli.py kill --id n4
```

Run the anti-regression suite:

```bash
python run_tests.py
```

## Manual

| Doc | What it covers |
|---|---|
| [docs/01-overview.md](docs/01-overview.md) | What the system is, guarantees, non-goals |
| [docs/02-architecture.md](docs/02-architecture.md) | Layers, module map, invariants, file tree |
| [docs/03-raft-core.md](docs/03-raft-core.md) | Terms, votes, pre-vote, election, commit, snapshots, membership |
| [docs/04-state-machine.md](docs/04-state-machine.md) | Chat FSM, rules, proposals, quorum decisions |
| [docs/05-storage.md](docs/05-storage.md) | Storage interface, MemoryStorage, SQLite schema, compaction |
| [docs/06-network-and-runtime.md](docs/06-network-and-runtime.md) | HTTP transport, RPC paths, runtime pump loop |
| [docs/07-supervisor-and-api.md](docs/07-supervisor-and-api.md) | Every endpoint, payloads, responses, error semantics |
| [docs/08-cli.md](docs/08-cli.md) | CLI commands, flags, exit codes |
| [docs/09-web-ui.md](docs/09-web-ui.md) | Web UI behaviour and wiring |
| [docs/10-testing.md](docs/10-testing.md) | Test policy, full inventory, how to run |
| [docs/11-runbook.md](docs/11-runbook.md) | Operating the cluster step by step |
| [docs/12-design-decisions.md](docs/12-design-decisions.md) | Why each design choice was made |
| [docs/13-limitations.md](docs/13-limitations.md) | Known limits and risks |
| [docs/14-contributing.md](docs/14-contributing.md) | Rules for changes, docs guard, checklist |

## Non-negotiable rules

1. Tests are never weakened to make code pass. A red test means the implementation is wrong.
2. `raft/core.py` is pure: no sockets, no threads, no wall clock, no uncontrolled randomness.
3. Only the leader accepts writes.
4. A heartbeat is not a vote.
5. A pre-vote never inflates the term.
6. The manual must match the code — `tests/test_docs.py` fails if it does not.
7. Instructions and paths in the manual are relative to the git root; no command may assume a
   particular checkout folder name.
