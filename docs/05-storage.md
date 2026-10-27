# 05 — Storage

`raft/storage.py` defines one interface and two implementations. The core talks only to the
interface, so tests use memory and production uses disk with zero code change.

## Entry shape

```python
entry(index, term, kind, payload, command_id=None)
# {"index": int, "term": int, "kind": str, "payload": dict, "command_id": str|None}
```

`(index, term)` is the version. Conflict resolution is version-based, never merge-based.

## Interface

| Method | Meaning |
|---|---|
| `meta(key, default=None)` / `set_meta(key, value)` | persistent node metadata (`current_term`, `voted_for`, `commit_index`, `snapshot_term`) |
| `append(e)` | append one entry |
| `entry(index)` | entry at index; returns a synthetic `kind: "snapshot"` entry when the index is the snapshot index |
| `entries_from(start, limit=None)` | entries from `start` |
| `truncate_from(index)` | delete entries from `index` (conflicting term) |
| `save_snapshot(index, term, data)` | store the state machine dict as the snapshot |
| `snapshot()` | latest snapshot or `None` |
| `last_index()` / `last_term()` | last entry, snapshot-aware |
| `log_size()` | entries after the snapshot |
| `compact(threshold)` | hook, no-op in the base class |
| `close()` | release resources |

`last_index()` and `log_size()` are snapshot-aware: after a snapshot the "log" is only what came
after it.

## `MemoryStorage`

Plain dict + list. Used by the deterministic tests. Snapshots are stored in a dict and entries
are not pruned.

## `SqliteStorage`

One file per node: `data/node-<id>.db`. `PRAGMA journal_mode=WAL`.

```sql
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE log (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, kind TEXT NOT NULL,
                  payload TEXT NOT NULL, command_id TEXT);
CREATE TABLE snapshot (id INTEGER PRIMARY KEY, idx INTEGER NOT NULL, term INTEGER NOT NULL,
                       data TEXT NOT NULL);
```

`save_snapshot` replaces the single snapshot row and deletes log entries up to the snapshot
index, so disk usage stays bounded.

## Compaction path

The core owns compaction:

```text
_apply_upto(target) -> sm.apply(entry) -> _compact()
_compact(): if log_size() >= snapshot_threshold and last_applied > 0:
              set_meta("snapshot_term", term); save_snapshot(last_applied, term, sm.to_dict())
```

`sm.to_dict()` includes `configs`, `messages`, `proposals`, `applied`, `applied_entries`,
`history` — so a node that restores from a snapshot keeps its idempotency guards.

## What a node keeps on disk

| Thing | Where | Why |
|---|---|---|
| current term | `meta.current_term` | a restarted node cannot vote twice in the same term |
| vote granted | `meta.voted_for` | same rule after restart |
| commit index | `meta.commit_index` | avoids re-applying and keeps read correct |
| log | `log` table | authoritative replicated history |
| snapshot | `snapshot` table | compacted state machine |
| state machine | snapshot data | chat state |

`next`, `match`, `votes`, `prevotes` and the leader lease are in memory only — they are rebuilt
after a restart or re-election.
