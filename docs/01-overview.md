# 01 — Overview

## What this is

A chatroom application where the chat is not stored on a server. Each participant runs a
**process that is a node of one Raft cluster**, and the cluster decides what the chat state is.

```text
        ┌──────────── one Raft cluster ────────────┐
        │  n1 (leader)      n2            n3        │
        │  SQLite node-n1.db  node-n2.db  node-n3.db│
        └────────────────────────────────────────────┘
                 ▲                    ▲
        CLI ─────┘                    └───── web UI
                 \                  /
                  supervisor (spawn / kill / discovery)
```

Every message, every state change and every membership change is a **log entry**. A message
becomes real for the system only when the entry is replicated to a quorum.

## The two layers

| Layer | Responsibility |
|---|---|
| Raft core (`raft/core.py`) | terms, votes, election, replication, commit, snapshots, membership |
| Chat state machine (`raft/statemachine.py`) | message records, state transitions, proposals, quorum decisions |

The core is generic and unaware of chat semantics. The state machine is unaware of transport.
They meet at `apply(entry)`: the core appends committed entries to the state machine.

## What "the cluster decides" means here

Two different decisions exist, and they must not be confused:

1. **Replication decision** — a quorum of cluster members stored the entry in its log, and the
   leader advanced `commit_index`. This makes the entry undeniable to every node.
2. **Application decision** — for contested message states (`delivered`, `ack`, `flagged`,
   `resolved`, `dropped`) the state machine requires a proposal plus a quorum of votes from
   cluster members. One node alone cannot mark a message as read or deleted.

Direct transitions (no proposal) are allowed only when the rule is unambiguous, for example the
recipient marking a message as `read`, or the author editing their own message. See
[04 — State machine](04-state-machine.md).

## Guarantees

- **Single writer**: only the leader accepts commands. Followers answer with a redirect.
- **Linearizable read**: a read is served only by the leader with a valid lease.
- **Idempotent client retry**: the same `command_id` never produces two state changes.
- **Deterministic tests**: same seed → same trace. Flakiness is treated as a bug.
- **Durable nodes**: every node has its own SQLite file; restarting a node recovers term, vote,
  commit index, log and snapshot.
- **Dynamic membership**: nodes can join and leave through joint-consensus config changes.
- **Dynamic discovery**: nodes self-register with the supervisor; the supervisor never needs a
  static config file.

## Non-goals

- Not a production-grade Raft: no disk fsync-per-entry guarantees, no async multi-threaded RPC,
  no storage of the leader's `next`/`match` beyond memory.
- No TLS, no authentication, no authorization beyond membership.
- No message ordering guarantees for the UI beyond commit order.
- No multi-datacenter or geo-replication.
