# 12 — Design decisions

Each decision below is enforced by at least one test. The "guard" column names it.

| # | Decision | Why | Guard |
|---|---|---|---|
| 1 | Python 3.13 standard library only | no dependency drift, tests run anywhere | `run_tests.py` uses no third-party import |
| 2 | Pure `RaftCore` + separate runtime | elections and commit logic must be testable without sockets or wall clock | `tests/test_core.py` |
| 3 | Injected `clock`, `rng`, `storage`, `sm` | determinism in tests, real services in production | `tests/sim.py`, `tests/test_storage.py` |
| 4 | One asyncio event loop per node | the core is never mutated concurrently, so no locks | `tests/test_runtime.py` |
| 5 | HTTP + JSON transport | trivial to debug, CORS lets the static UI call the supervisor | `tests/test_webui.py` |
| 6 | Only the leader accepts writes | single writer, no split-brain writes | `test_rejected_command_by_non_leader` |
| 7 | Redirect instead of proxy on followers | clients learn the leader explicitly | `tests/test_runtime.py` |
| 8 | Linearizable reads only from the leader with a valid lease, re-verified by `check` when the lease expired | a stale leader must not answer reads | `test_leader_lease_and_read`, `test_check_confirms_leader` |
| 9 | Client idempotency keyed by `command_id` | retries are normal in a distributed system | `test_client_retry_is_idempotent`, `test_replay_is_idempotent` |
| 10 | Commit advances only when a majority of **every** quorum set holds the entry | joint consensus is what makes membership changes safe | `test_joint_consensus_requires_both_configs`, `test_no_commit_without_majority` |
| 11 | A joining node starts as a non-member and learns membership from the log | it cannot vote before it replicates | `test_join_completes_once_new_node_replicates` |
| 12 | Pre-vote enabled by default, and a pre-vote never changes `term` or `voted_for` | a partitioned minority must not inflate terms | `test_prevote_does_not_inflate_term_in_partition` |
| 13 | A heartbeat is never a vote | `voted_for` may only change when a vote is granted | regression caught by `test_three_nodes_exactly_one_leader` |
| 14 | Conflicting entries are truncated, never merged | versioned log, term is the version | `test_conflicting_entry_is_truncated` |
| 15 | Snapshots taken at apply time; a lagging peer receives the snapshot | bounded disk and bounded catch-up cost | `test_snapshot_after_threshold`, `test_snapshot_install_on_lagging_node` |
| 16 | Membership lives in the log; the state machine mirrors it through `load_configs` | core and state machine must agree on who votes | `test_membership_apply_is_not_a_duplicate_of_the_mirror` |
| 17 | Contested message states need a proposal + application quorum; direct transitions are only `read` by recipient, `edit` by author, and the author's own tombstone | one node must not unilaterally decide chat semantics | `tests/test_statemachine.py` |
| 18 | Application decisions are evaluated at apply time from the replicated order | two nodes cannot disagree about which vote decided the proposal | `test_decision_applies_at_quorum`, `test_decision_needs_quorum_of_every_config_in_joint_view` |
| 19 | SQLite per node, WAL | durable per-node storage with zero configuration | `tests/test_storage.py` |
| 20 | Supervisor kill is safe by default (commit the leave, then terminate); `force` is an explicit opt-in | killing a member without a decided leave can break quorum | `tests/test_supervisor.py` |
| 21 | Dynamic discovery by self-registration, no static cluster file | nodes can join at runtime with no config edit | `tests/test_supervisor.py` |
| 22 | CLI exit code accounts for the nested command result | automation must not see success on a failed decision | `tests/test_cli.py` |
| 23 | The UI may only call endpoints the supervisor serves, and may only render fields that exist | the UI is a contract, not a decoration | `tests/test_webui.py` |
| 24 | The manual is tested | documentation drift is a regression | `tests/test_docs.py` |
