# 04 — Chat state machine

`raft/statemachine.py` holds the chat state. It is deterministic and pure: the same entry order
produces the same state on every node.

## Entry kinds

| Kind | Meaning |
|---|---|
| `membership` | config change `{cold, cnew, joint}` |
| `noop` | leader's first entry in a term, nothing to apply |
| `command` | chat command, dispatched by `payload.type` |

Command types: `submit`, `transition`, `propose`, `vote`.

## States

```text
committed ──▶ delivered ──▶ read ──▶ ack
                  │            │
                  ▼            ▼
               flagged ──▶ resolved
                  │            │
                  ▼            ▼
               dropped  (tombstone, terminal)
```

Allowed transitions (exact, from `ALLOWED_TRANSITIONS`):

| state | allowed transitions |
|---|---|
| committed | delivered, flagged, dropped |
| delivered | read, flagged, dropped |
| read | ack, flagged, dropped |
| ack | flagged, dropped |
| flagged | resolved, dropped |
| resolved | dropped |
| dropped | (none, tombstone) |

Contested decisions (require a proposal + quorum): `delivered`, `ack`, `flagged`, `resolved`,
`dropped`.

Aliases accepted from clients: `delete` → `dropped`, `drop` → `dropped`.

Machine-readable contract (checked by `tests/test_docs.py`):

```text
CONTESTED: delivered, ack, flagged, resolved, dropped
ALIAS: delete -> dropped, drop -> dropped
```

## Message record

```json
{
  "id": "m1",
  "author": "alice",
  "recipients": ["bob"],
  "text": "hello",
  "state": "committed",
  "created_index": 4,
  "revisions": ["hello"],
  "transitions": [{"index": 4, "to": "committed", "by": "alice", "via": "direct"}],
  "tombstone": false,
  "dropped_by": null,
  "current_text": "hello"
}
```

`current_text` is the last revision, added by the read view (the stored record keeps
`revisions`).

## Rules

1. A message starts in `committed` only when the entry exists — it cannot be created in another
   state.
2. `submit` without `msg_id` falls back to `command_id`, so a plain CLI send is still addressable.
3. Only a recipient can mark `read`.
4. Only the author can `edit`; an edit appends a revision and does not change state.
5. Contested transitions cannot be done directly by one node — they need a proposal.
6. The author may tombstone (`dropped`) directly, because deleting one's own message is not a
   dispute.
7. A tombstoned message cannot be changed any further.
8. Illegal jumps are rejected: the target must be in `ALLOWED_TRANSITIONS[current_state]`.

## Proposals and decisions

```json
{
  "id": "p1",
  "msg_id": "m1",
  "type": "delivered",
  "by": "n1",
  "votes": ["n1", "n2"],
  "decided": true,
  "outcome": "accepted",
  "index": 5
}
```

- `propose` is itself a replicated entry, so proposals are not local guesses.
- Only cluster members can vote.
- A decision is reached when the votes hold a majority in **every** quorum set (joint configs
  included), evaluated at apply time.
- If the transition is no longer legal when the quorum is reached, the proposal is recorded as
  `outcome: "rejected"` — the decision is still recorded, the state does not change.
- Votes are exposed as sorted lists so JSON output is stable.

## Idempotency

Two guards, both persisted:

- `applied[command_id]` — a client retry of the same `command_id` returns the stored result and
  never re-applies the command.
- `applied_entries[entry_index]` — a membership entry is applied once per entry. The core mirrors
  configs into the state machine, so without this guard a fresh join would be misreported as a
  duplicate.

## Read views

| Method | Returns |
|---|---|
| `messages()` | messages sorted by `created_index`, with `current_text` |
| `proposals()` | proposals sorted by `index`, votes as lists |
| `config_view()` | latest config plus its active members |
| `active_config()` | union of the current quorum sets |
| `is_member(node)` | membership in any quorum set |
