# Git branches

Updated 2026-10-11.

## Conventions

- `main` is the only long-lived branch. Everything else is a topic branch cut from the current `main`.
- Branch names: `feat/…` new capability, `fix/…` bug fix, `test/…` tests only, `docs/…`, `chore/…` repo hygiene.
- One concern per branch, small commits with a subject line and a "why" body.
- Merge into `main` with a merge commit (`Merge branch '<name>'`), after the full test suite passes on the merged result.
- If a branch depends on another unmerged branch, say so in its commit message and merge the dependency first.
- Never commit generated runtime state, logs or large data (see `.gitignore`).
- Delete a topic branch once it is merged and pushed. Mark milestones with tags, not long-lived branches.

## Merged into main (2026-10-11, in order)

| Branch | What it added | Merge commit |
|---|---|---|
| `local/data-flow-detector` | Data quality alerts resolve when the check passes (bootstrap, replay, freshness checks) | `d6a2c25` |
| `fix/etl-hardening` | API-Football error payloads fail the run; results catch up for 14 days after a season ends; league tier saved in `dim_leagues`; `int_fixtures` no longer duplicates fixtures for multi-season teams | `d9f3d28` |
| `engine-v2` | Consensus engine v2: Dixon-Coles, per-market calibration and Goal Bounds 1-4, Bayesian k/n and venue context, fixture analysis and shortlists, forecast history and API, `engine_predict` waits for dbt and verifies shortlists, diagnostics | `a881cfc` |
| `test/rolling-standouts-timezones` | DST window test runs in Los Angeles; new test pins BC at UTC-7 after 2026-11-01 (tzdata 2026c) | `03d2d1d` |
| `feat/xg-dbt-lineage` | Daily xG ingestion with coverage tracking; `raw.fixture_xg` → `stg_raw_fixture_xg` → `fact_fixture_xg`, which Dixon-Coles reads | `a597f2d` |
| `chore/gitignore-local-state` | Ignore `logs/`, Claude worktrees, `big_dog_leagues_state.json`, experiment `.gz` data | see `git log` |
| `docs/git-branches` | This file | see `git log` |

These branches were deleted after merging. Test suite on `a597f2d`: 236 passed, 0 failed.

## Tags

| Tag | Points at | Meaning |
|---|---|---|
| `engine-v1` | `5afcee1` | Consensus engine v1 snapshot (three-model system + calibration) |
| `engine-v2` | `da5ea47` | Last commit of the engine v2 branch before it merged |

Tags exist locally; push them from your machine with `git push origin --tags`.

## Open

| Branch | Content | Notes |
|---|---|---|
| `docs/product-design` | Product docs: design brief, decisions (to D104), match detail contract, interaction tests, mocks, experiment reports | Experiment `.gz` files are not committed (largest 41 MB) |

Uncommitted in the working tree: refreshed `engine/eval/calibration_maps.json` and `calibration_report.json` (from a calibration run after 2026-10-10), and `.dbt/profiles.example.yml` (locked on disk, could not be read).

## Deleted as stale (2026-10-11)

Local branches, all fully contained in `main`: `engine-v1` (now a tag), `list`, `codex/data-flow-detector`, `claude/elastic-hawking-c31c84`, `local/data-detector-skipped-runs`.

Still on GitHub (also contained in `main`), delete from your machine:

```bash
git push origin --delete local/data-detector-skipped-runs local/data-flow-detector
```

Worktree records for `.claude/worktrees/elastic-hawking-c31c84` (no uncommitted work) and four `~/.codex/worktrees/` folders were pruned. Those folders are no longer git worktrees; delete them if unused.

## Safety refs

Created during the 2026-10-10/11 reorganisation; delete once you are happy with `main`:

- `refs/backup/engine-v2-before-split` — `engine-v2` before the uncommitted work was split into branches
- `refs/backup/main-before-dfd-merge` — `main` before the 2026-10-11 merges
- `refs/backup/fix-etl-hardening-before-move`, `refs/backup/xg-before-move`, `refs/backup/xg-before-rebase`, `refs/backup/chore-gitignore-before-rebase` — intermediate states

```bash
git for-each-ref --format='%(refname)' refs/backup | xargs -n1 git update-ref -d
```

## Known repo quirks

- Some files under `.git/logs/` were locked on the Mac ("Resource deadlock avoided") and were renamed `*.unreadable-bak`. Only reflog history is affected; they can be deleted.
- `test_extract_metadata_tiers.py` errors when run in the same session as `test_data_detector.py` (that test stubs modules). It passes on its own.
