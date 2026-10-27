# 10 — Testing

## Policy

1. **Tests are never weakened.** If a test fails, the implementation is wrong. A test may only be
   changed when the intended behaviour itself changes, and that must be written down here.
2. **Deterministic unit tests.** `tests/sim.py` runs the real `RaftCore` with a virtual clock, a
   seeded RNG, `MemoryStorage` and zero-latency delivery. Same seed → same trace.
3. **Real-process integration tests.** `test_runtime`, `test_supervisor`, `test_cli` and
   `test_webui` use real HTTP on OS-assigned ports (port 0) and real subprocesses, so they test
   the code the user actually runs.
4. **The manual is guarded.** `tests/test_docs.py` fails if the documentation drifts from the
   code (endpoints, flags, defaults, FSM table, file tree, test inventory, runnable examples).

Run everything:

```bash
python run_tests.py
```

Run one module:

```bash
python -m unittest tests.test_core
```

## Inventory

| Module | Tests | Scope |
|---|---|---|
| `tests/test_core.py` | 16 | election, pre-vote, commit, partitions, truncation, snapshots, membership, lease, check |
| `tests/test_statemachine.py` | 12 | chat FSM, rules, proposals, quorum decisions, idempotency |
| `tests/test_storage.py` | 3 | SQLite roundtrip, recovery, compaction |
| `tests/test_runtime.py` | 2 | real HTTP cluster, membership, re-election |
| `tests/test_supervisor.py` | 1 | real processes, discovery, spawn, safe kill, disk files |
| `tests/test_cli.py` | 1 | CLI commands against a live cluster |
| `tests/test_webui.py` | 6 | static serving, UI wiring, UI/API contract, live UI flow |
| **Total (excluding the docs guard)** | **41** | |
| `tests/test_docs.py` | 16 | the manual itself, compared with the code |
| **Grand total** | **57** | |

## `tests/test_core.py`

```text
test_check_confirms_leader
test_client_retry_is_idempotent
test_command_commits_on_quorum
test_conflicting_entry_is_truncated
test_election_requires_real_majority
test_join_completes_once_new_node_replicates
test_joint_consensus_requires_both_configs
test_leader_lease_and_read
test_leave_shrinks_config_and_commit_follows
test_no_commit_without_majority
test_prevote_does_not_inflate_term_in_partition
test_rejected_command_by_non_leader
test_single_node_elects_itself
test_snapshot_after_threshold
test_snapshot_install_on_lagging_node
test_three_nodes_exactly_one_leader```

## `tests/test_statemachine.py`

```text
test_submit_and_direct_transitions
test_read_by_non_recipient_rejected
test_edit_only_by_author
test_contested_transition_needs_a_proposal
test_author_can_tombstone_directly
test_decision_applies_at_quorum
test_decision_needs_quorum_of_every_config_in_joint_view
test_vote_from_non_member_rejected
test_decision_with_illegal_transition_is_rejected
test_membership_apply_is_not_a_duplicate_of_the_mirror
test_replay_is_idempotent
test_invalid_state_jump_rejected
```

## `tests/test_storage.py`

```text
test_roundtrip_and_persistence
test_node_recovers_state_from_disk
test_compaction_on_disk_keeps_state_readable
```

## `tests/test_runtime.py`

```text
test_http_cluster
test_membership_and_reelection
```

## `tests/test_supervisor.py`

```text
test_spawn_kill_and_discovery
```

Covers: spawn three nodes, discovery through `/admin/register`, leader election, client command
and read, status, safe kill through membership, the killed node leaves the config, survivors
reach quorum, each node owns exactly one `.db` file (`node-n1.db`, `node-n2.db`, `node-n3.db`),
and killing the same node twice is rejected.

## `tests/test_cli.py`

```text
test_cli_over_http
```

Runs `cli.py` as a subprocess through `asyncio.to_thread` so the supervisor loop is never
blocked. Covers: status, send, propose, vote, read, kill, post-kill status, post-kill command,
and the exit-code contract the manual promises — a rejected decision (`--state read`, which is not
contested) and an unknown node must both exit `1` while printing a JSON body.

## `tests/test_webui.py`

```text
test_static_files_are_served_with_correct_types
test_ui_controls_are_wired
test_js_selectors_exist_in_html
test_ui_endpoints_are_all_served
test_js_reads_only_fields_the_api_returns
test_ui_flow_against_live_cluster
```

## `tests/test_docs.py`

```text
test_cli_commands_and_flags_documented
test_contested_and_aliases_match
test_defaults_documented
test_documented_checks_match_guard_methods
test_documented_paths_exist_in_code
test_examples_are_runnable
test_file_tree_matches_repo
test_instructions_start_from_the_git_root
test_net_client_defaults_documented
test_node_flags_documented
test_readme_links_resolve
test_stats_keys_documented
test_storage_tables_documented
test_supervisor_flags_documented
test_test_inventory_matches
test_transition_table_matches_code```

## What the suite has already caught

These are the regressions the tests prevented, kept here so they are never re-introduced:

- `_handle_append` treated heartbeats as votes, so `voted_for` changed on a heartbeat.
- Pre-votes inflated the term, breaking the "pre-vote must not change term" rule.
- Commit advanced from the wrong quorum set during a joint config.
- A joining node could vote before it replicated the membership entry.
- `entries_from(start, limit)` passed the wrong number of SQLite bindings.
- `submit()` without `msg_id` produced an unusable message id.
- The state machine reported a fresh join as a duplicate of its own config mirror.
- The supervisor returned stale status after a kill because the process was not waited for.
- The CLI reported exit code 0 when the inner command result was `ok: false`.
- The UI rendered `n.commit` and `n.url`, fields that do not exist in the payloads.
