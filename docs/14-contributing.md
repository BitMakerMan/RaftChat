# 14 — Contributing

## The two rules

1. **Never weaken a test to make code pass.** If a test is red, fix the implementation. Changing
   a test's expectation requires writing down why in the commit message and in
   [10 — Testing](10-testing.md).
2. **The manual is code.** `tests/test_docs.py` compares the documentation with the
   implementation. If you change an endpoint, a CLI flag, a default, the FSM, the storage schema,
   the file tree, or the test inventory, the docs must change in the same commit.

## What the docs guard checks

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

It works on the real artifacts, not on intentions:

| Check | Source of truth |
|---|---|
| README links | files on disk |
| HTTP paths | string literals in `raft/supervisor.py`, `raft/runtime.py`, `raft/net.py` |
| CLI commands and flags | `cli.build_parser()` |
| Node flags | `run_node.build_parser()` |
| Supervisor flags | `run_supervisor.build_parser()` |
| Transition table | `statemachine.ALLOWED_TRANSITIONS` |
| Contested set and aliases | `statemachine.CONTESTED`, `statemachine.ALIAS` |
| Defaults | `core.DEFAULT_CFG` |
| Stats keys | `RaftCore.stats` |
| Storage tables | `CREATE TABLE` statements in `storage.py` |
| File tree | the repository |
| Test inventory | `unittest` discovery |
| Runnable examples | every `python cli.py ...`, `python run_node.py ...`, `python run_supervisor.py ...` line in the docs is parsed by the real parser |
| Network defaults | `inspect.signature(net.post)` / `inspect.signature(net.get)` and `inspect.signature(cli.call)` |
| Instructions | never change directory, never name the checkout folder, never use an absolute path; every `python ...` line is a git-root command |

## Checklist for a change

1. `python run_tests.py` must be green before and after.
2. New behaviour needs a new test; the test name goes into `docs/10-testing.md`.
3. New endpoint, flag, default, state, or file goes into the matching doc page.
4. Example commands in the docs must be runnable exactly as written — they are parsed by the real
   parsers in the guard test.
5. No new dependency. Standard library only.
6. Generated directories and logs are not committed.
7. Write instructions from the git root: never change directory, never name the checkout
   folder, never use an absolute path.

## Style that has proved useful

- Keep `raft/core.py` pure: no socket, no thread, no wall clock, no uncontrolled randomness.
- Keep the state machine pure: no transport, no time.
- Return JSON-shaped dicts with `ok`, and put the reason in `error`.
- Prefer a deterministic failure over a retry that hides it.
- Comment only what is not obvious from the code, and never comment a rule that a test already
  enforces twice.
