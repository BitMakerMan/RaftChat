# 03 — Raft core

`raft/core.py` contains the whole consensus logic. It is **pure**: every handler returns an
outbox of messages and never touches sockets, threads, wall clock, or uncontrolled randomness.

```python
RaftCore(node_id, storage, sm, clock=None, rng=None, addr="", initial_config=None, cfg=None)
```

Everything the core needs from the outside is injected:

| Dependency | Meaning |
|---|---|
| `storage` | `MemoryStorage` (tests) or `SqliteStorage` (production) |
| `sm` | `ChatStateMachine` |
| `clock` | callable returning milliseconds |
| `rng` | `random.Random` instance, seeded by the caller |

## Defaults (`DEFAULT_CFG`)

| Key | Default | Meaning |
|---|---|---|
| `heartbeat_ms` | 100 | leader sends appends at most every 100 ms |
| `election_min_ms` | 300 | lower bound of the randomized election window |
| `election_max_ms` | 600 | upper bound of the randomized election window |
| `snapshot_threshold` | 64 | log entries applied before a snapshot is taken |
| `max_batch` | 32 | entries per append batch |
| `request_timeout_ms` | 400 | RPC HTTP timeout |
| `pre_vote` | `True` | pre-vote phase enabled |

## Roles

`follower`, `precandidate`, `candidate`, `leader`.

Node state: `term`, `voted_for`, `role`, `leader`, `commit_index`, `last_applied`, `configs`,
`addrs`, `next`, `match`, `votes`, `prevotes`, `prevote_for`, `election_deadline`,
`heartbeat_deadline`, `lease_expired`, `pending`, `stats`.

## Recovery (`_load`)

On construction the core restores, in order:

1. `current_term`, `voted_for`, `commit_index` from storage meta;
2. the last snapshot into the state machine, and `last_applied` from it;
3. entries from `last_applied + 1` to `commit_index` applied to the state machine;
4. membership from `initial_config` plus every `membership` entry in the log
   (`_sync_configs`), mirrored into the state machine through `sm.load_configs()`.

This is why a restarted node keeps its vote and cannot vote twice in the same term.

## Membership and quorum sets

Membership is a list of configs, each `{cold, cnew, joint, index, entry_index}`. The bootstrap
config has `index 0` and comes from `initial_config`.

`quorum_sets()` for the latest config:

- joint config → one set per non-empty side (`cold` and `cnew`);
- non-joint config → one set, `cnew` (falling back to `cold`).

`_reached(votes)` requires a strict majority **inside every set**. This is joint consensus:
during a transition both the old and the new configuration must agree.

`config_members()` is the union of all sets; `is_member(node)` is true if the node is in any set
(a joining node is not a member until its config entry is applied).

## Election

```text
follower --(election window expires)--> precandidate (pre-vote) --> candidate --> leader
```

**Pre-vote** (`_handle_vote`, `prevote=True`):

- never changes `term`, never changes `voted_for`;
- granted when `p.term >= self.term`, the sender's log is up to date, and `prevote_for` is free
  or already that sender;
- the window is a plain randomized `election_min_ms..election_max_ms`;
- when a majority in every quorum set grants the pre-vote, the node increments the term and
  becomes a real candidate.

**Real vote** (`_handle_vote`, `prevote=False`):

- the sender must be a member of some quorum set, otherwise the vote is rejected;
- `term > self.term` → step down;
- requires `term == self.term`, no existing vote for someone else, sender log up to date;
- grant sets `voted_for` and pushes the election deadline forward.

Winning the vote (`_handle_vote_ack`): the candidate must reach a majority in **every** quorum
set, then `_become_leader()`:

- `role = LEADER`, `leader = self`;
- `next[n] = last_index + 1`, `match[n] = 0` for every member;
- a `noop` entry is appended (needed so the leader can commit entries from its own term);
- `lease_expired = clock + 2 * heartbeat_ms`.

## Heartbeat and replication

`tick()` on the leader sends one append (or snapshot) per peer every `heartbeat_ms`.

**A heartbeat is not a vote.** `_handle_append` resets the election deadline but never touches
`voted_for` — this was a real bug caught by a test.

`_handle_append` on a follower:

1. stale term → reject;
2. sender not a member and not in `initial_config` → reject (a node must learn membership from
   the log before accepting writes from it);
3. `prev_index`/`prev_term` mismatch → reject with `conflict_term`/`conflict_index`, or with
   `snapshot_required` when `prev_index` is older than the stored snapshot;
4. append entries in order; if an existing entry at the same index has a different term,
   truncate from that index and re-sync configs;
5. accept `commit_index` and apply up to it.

Commit advances on the leader in `_advance_commit()`:

1. take, for every quorum set, the median `match[n]` (the leader counts itself through
   `match[self] = last_index`);
2. the candidate commit point is the minimum of those medians;
3. the entry at that index must belong to the **current term** — an entry from a previous term is
   never committed directly, it is committed later through an entry of the current term.

This is the standard Raft safety rule, and `test_command_commits_on_quorum` plus
`test_no_commit_without_majority` keep it enforced.

## Snapshots

- `_compact()` runs after each apply batch: when `log_size() >= snapshot_threshold` and
  `last_applied > 0`, the state machine dict is stored as a snapshot with the current term.
- `_sync_peer()` sends a `snapshot` message instead of entries when the peer's `next_index - 1`
  is older than the stored snapshot.
- `_install_snapshot()` loads the state, restores configs from the snapshot data, sets
  `last_applied`, raises `commit_index`, persists the snapshot, and updates `next`/`match`.

## Linearizable read and `check`

- `read_state()` returns the state; the runtime's `linearizable_read()` refuses to serve it on a
  follower, and re-verifies leadership with `check` when the lease is expired.
- `check` confirms leadership: the answer is `ok` only if the term matches and the node is the
  leader it believes itself to be. This is the guard against a stale leader in a partition.

## Status

`status()` returns `ok`, `node_id`, `addr`, `role`, `term`, `leader`, `commit_index`,
`last_applied`, `last_index`, `members`, `match`, `next`, `addrs`, `configs`, `stats`,
`lease_valid`, `snapshot_index`, `log_size`.

`stats` keys: `votes_won`, `appends_sent`, `commit_advanced`, `snapshots_taken`,
`snapshots_installed`, `rejected`, `elections_started`, `prevotes_started`.
