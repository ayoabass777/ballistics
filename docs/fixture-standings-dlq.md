# Fixture and standings quarantine

Implementation checked locally on 2026-10-20. Isolated PostgreSQL integration verification is complete; live S3 verification remains pending.

- Bootstrap and full-season replay retain malformed fixture payloads with their rejection reason. Rejected rows prevent pruning the previous good fixtures.
- New-fixture discovery validates before filtering existing IDs and parks rejected records.
- Daily fixture updates validate before deduplication, park bad records, and load valid records. Existing league quarantine is preserved because an incremental payload is not a complete season. Partial updates do not resolve entries; full replay repairs them.
- Standings quarantine covers missing team IDs, malformed nested records, invalid integer fields and integer overflow. Valid standings still load without pruning when any row is rejected.
- Fixture and standings replay isolate per-entry fetch/load failures, record retry state, and continue to the next league. Failure to record retry state is logged, leaving the entry open for another run.
- Records whose league/season cannot be recovered from payload or fixture ID are retained under `dlq/fixtures/unassigned/<uuid>.json` with `requires_manual_review: true`. They are not automatically replayed: an operator must establish their identity. Existing automated DLQ counters do not include this prefix.

Regression tests: `tests/test_dlq_remaining_paths.py`, plus existing fixture/standings DLQ suites. External alerts are mocked during local tests. PostgreSQL tests require an isolated `FLOW_TEST_DSN`; they must never target the application database because their setup replaces test tables.

## Verification follow-up — 2026-10-20

- Ran the fixture, standings and discovery suites against a fresh temporary PostgreSQL instance on port 55439: **76 passed, zero skipped**. This includes all 12 previously skipped database tests. External alert delivery was mocked; S3 uses the test fake. The instance was stopped after testing.
- Added a full `run_consensus` regression with mocked model/DB inputs but real signal conversion, scoring and output assembly: NS survives; PST, CANC, ORPHAN, 1H and FT are absent from both pool and fixture analysis. Engine guard suite: **4 passed**.
- Read-only live verification of logical run `2026-10-20`: complete, 750 match analyses (all currently NS); 1,834 pool rows across 733 fixtures (all currently NS). Bayesian/Dixon-Coles/Mismatch runs are `ok`, with 2,945/3,632/671 picks respectively. No Mismatch fixture lacks a home or away Elo-history entry. These are current warehouse checks, not a reconstruction of status at publication time.
