"""Per-node storage. Same interface, two implementations:

- MemoryStorage: used by deterministic tests (no disk side effects).
- SqliteStorage: durable per-node disk storage (WAL, atomic commits).

Entries are plain dicts so they survive JSON transport and snapshots:
    {"index": int, "term": int, "kind": str, "payload": dict, "command_id": str|None}

`entry(index)` also answers for the snapshot index with a pseudo-entry, so log
conflict checks keep working after compaction.
"""

import json
import os
import sqlite3


def entry(index, term, kind, payload, command_id=None):
    return {
        "index": int(index),
        "term": int(term),
        "kind": kind,
        "payload": payload or {},
        "command_id": command_id,
    }


class Storage:
    def meta(self, key, default=None):
        raise NotImplementedError

    def set_meta(self, key, value):
        raise NotImplementedError

    def append(self, e):
        raise NotImplementedError

    def entries_from(self, start, limit=None):
        raise NotImplementedError

    def truncate_from(self, index):
        raise NotImplementedError

    def save_snapshot(self, index, term, data):
        raise NotImplementedError

    def snapshot(self):
        raise NotImplementedError

    def _entry_impl(self, index):
        raise NotImplementedError

    def _last_impl(self):
        raise NotImplementedError

    def entry(self, index):
        snap = self.snapshot()
        if snap and int(index) == snap["index"]:
            return {"index": snap["index"], "term": snap["term"], "kind": "snapshot",
                    "payload": {}, "command_id": None}
        return self._entry_impl(int(index))

    def last_index(self):
        snap = self.snapshot()
        return max(self._last_impl(), snap["index"] if snap else 0)

    def last_term(self):
        e = self.entry(self.last_index())
        return e["term"] if e else 0

    def log_size(self):
        snap = self.snapshot()
        return self._last_impl() - (snap["index"] if snap else 0)

    def compact(self, threshold):
        return None

    def close(self):
        pass


class MemoryStorage(Storage):
    """Index-preserving in-memory storage (mirrors SQLite semantics exactly)."""

    def __init__(self):
        self._meta = {}
        self._entries = {}
        self._snap = None

    def meta(self, key, default=None):
        return self._meta.get(key, default)

    def set_meta(self, key, value):
        self._meta[key] = value

    def append(self, e):
        e = dict(e)
        e["index"] = int(e["index"])
        self._entries[e["index"]] = e
        return e["index"]

    def _entry_impl(self, index):
        return self._entries.get(index)

    def _last_impl(self):
        return max(self._entries) if self._entries else 0

    def entries_from(self, start, limit=None):
        out = [self._entries[i] for i in sorted(self._entries) if i >= int(start)]
        return out[:limit] if limit else out

    def truncate_from(self, index):
        for i in [i for i in self._entries if i >= int(index)]:
            del self._entries[i]

    def save_snapshot(self, index, term, data):
        self._snap = {"index": int(index), "term": int(term), "data": data}
        for i in [i for i in self._entries if i <= int(index)]:
            del self._entries[i]

    def snapshot(self):
        return self._snap


class SqliteStorage(Storage):
    def __init__(self, path, node_id):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.path = path
        self.node_id = node_id
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS log (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, command_id TEXT)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshot (id INTEGER PRIMARY KEY, idx INTEGER NOT NULL, "
            "term INTEGER NOT NULL, data TEXT NOT NULL)"
        )

    def meta(self, key, default=None):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )

    def append(self, e):
        self.conn.execute(
            "INSERT INTO log (idx, term, kind, payload, command_id) VALUES (?,?,?,?,?)",
            (int(e["index"]), int(e["term"]), e["kind"],
             json.dumps(e["payload"], sort_keys=True), e["command_id"]),
        )
        return int(e["index"])

    def _entry_impl(self, index):
        row = self.conn.execute(
            "SELECT idx, term, kind, payload, command_id FROM log WHERE idx=?", (index,)
        ).fetchone()
        if row is None:
            return None
        return {"index": row[0], "term": row[1], "kind": row[2],
                "payload": json.loads(row[3]), "command_id": row[4]}

    def _last_impl(self):
        return self.conn.execute("SELECT COALESCE(MAX(idx),0) FROM log").fetchone()[0]

    def entries_from(self, start, limit=None):
        if limit:
            rows = self.conn.execute(
                "SELECT idx, term, kind, payload, command_id FROM log WHERE idx>=? ORDER BY idx LIMIT ?",
                (int(start), int(limit))).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT idx, term, kind, payload, command_id FROM log WHERE idx>=? ORDER BY idx",
                (int(start),)).fetchall()
        return [{"index": r[0], "term": r[1], "kind": r[2], "payload": json.loads(r[3]),
                 "command_id": r[4]} for r in rows]

    def truncate_from(self, index):
        self.conn.execute("DELETE FROM log WHERE idx>=?", (int(index),))

    def save_snapshot(self, index, term, data):
        self.conn.execute("DELETE FROM snapshot")
        self.conn.execute("INSERT INTO snapshot (id, idx, term, data) VALUES (1,?,?,?)",
                          (int(index), int(term), json.dumps(data, sort_keys=True)))
        self.conn.execute("DELETE FROM log WHERE idx<=?", (int(index),))

    def snapshot(self):
        row = self.conn.execute("SELECT idx, term, data FROM snapshot WHERE id=1").fetchone()
        return {"index": row[0], "term": row[1], "data": json.loads(row[2])} if row else None

    def close(self):
        self.conn.close()
