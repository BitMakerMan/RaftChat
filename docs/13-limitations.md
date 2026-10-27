# 13 — Limitations

Know these before using the system for anything real.

## Consensus and durability

- `next`, `match`, `votes`, `prevotes` and the leader lease are in memory. After a restart the
  leader rebuilds them; a follower that restarts before the leader notices may need a fresh
  round of appends.
- Entries are written to SQLite without an explicit `fsync` per entry. A crash right after a
  commit can lose a committed entry on that node — the cluster can still recover it from a
  quorum, but the guarantee is not the same as a WAL-first durable log.
- `command_id` idempotency survives a restart only through the snapshot's `applied` map. The
  `pending` map (command id → log index) is in memory, so after a restart the same `command_id`
  returns the stored result but with `index: null`.
- There is no reconfigurations timeout or leadership transfer. Membership changes take effect
  when the entry is committed.

## Transport

- Plain HTTP, no TLS, no authentication. Any process that can reach a port can send RPCs; trust
  is membership-based.
- Requests are fire-and-forget with a 400 ms RPC timeout and no retry. Liveness depends on the
  pump loop.
- Responses are not compressed and there is no backpressure: a slow peer slows the event loop of
  the sender.

## Application layer

- Linearizable reads cost a quorum `check` when the lease is expired.
- Proposal quorum is computed with the *current* quorum sets at apply time.
- There is no TTL, no retention policy, and no delivery guarantee beyond the state machine
  transitions — `delivered` is a decision, not a push notification.
- Chat state is per-cluster, not per-user: there is no private storage, no inbox, no search.

## Operations

- The supervisor is a single process. If it dies, running nodes keep working; only
  spawn/kill/discovery stop.
- Node discovery is voluntary: a node started without `--supervisor` is invisible to the
  supervisor until it registers or talks to it.
- Killing a node with `force: true` can leave the cluster below quorum until the membership entry
  is decided by the remaining members.
- On Windows the supervisor uses `taskkill /PID /T /F`; elsewhere `SIGTERM`. The supervisor waits
  up to 5 seconds for the process to exit.

## Tests

- Real-process tests take about 20–30 seconds for the full suite.
- The UI tests never launch a browser: they assert the HTML/JS contract and run the same calls
  the UI makes against a live cluster.
