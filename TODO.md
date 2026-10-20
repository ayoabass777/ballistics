# To-do

One list for open work across the pipeline, engine and product. Detailed
history stays where it was recorded (linked); this file is the index.
Updated 2026-10-20.

## Now

- [ ] **Merge the 2026-10-20 branches into `main`** (merge commits, per
  [docs/git-branches.md](docs/git-branches.md)): `feat/dlq-hourly-retry`,
  `feat/model-result-reuse`, `chore/calibration-refresh-2026-10-18`,
  `docs/product-log-2026-10-20`. They touch separate files and merge in any order.
- [ ] **Run the database-backed tests** (16 skipped without `FLOW_TEST_DSN`):
  create a throwaway database in `ballistics_postgres`, run
  `tests/test_dlq.py`, `tests/test_dlq_fixtures_monitoring.py`,
  `tests/test_daily_flow.py` against it, drop it.
- [ ] **Fix the two failing tests** (both predate the 2026-10-20 work):
  - `tests/test_rolling_standouts.py::test_vancouver_stays_on_utc_minus_7_after_november_2026`
    needs tzdata ≥ 2026c (local Python has 2025.2): upgrade `tzdata`.
  - `tests/test_extract_metadata_tiers.py` errors at collection in a full-suite
    run but passes alone: find the import/name clash.

## Pipeline (DLQ, D147/D148)

- [ ] A replay that first discovers bad rows creates the entry at 1 attempt,
  not 0, so it dies one retry early (`etl/src/fixtures_dlq.py` and
  `etl/src/standings_dlq.py`, `park_*_rows` with `source="replay"`). Low.
- [ ] A clean fixture load resolves the S3 entry and sets the status to `ok`
  before the caller commits; if that commit then fails the entry is lost
  (`etl/src/fixtures_dlq.py`, `load_league_fixtures`). Low.
- [ ] Incremental fixture updates only touch existing rows: fixtures created
  after bootstrap (e.g. playoffs) are never inserted (CLAUDE.md, Known Gaps).
- [ ] ~110 fixtures stuck at NS from postponements (Egyptian league, USL Tormenta).
- [ ] `engine/eval/diagnostics/out/` is generated output: add it to `.gitignore`.

## Engine

- [ ] Learned stacker to replace tier voting (logistic regression per market
  family on the four models' probabilities + league base rate); also fixes the
  double-counted Bayesian signals in `avg_conf`.
- [ ] Gate Bayesian picks on edge over the league base rate, not an absolute 0.65.
- [ ] Align backtest and live Bayesian base rates (current season vs all
  history before the cutoff).
- [ ] Smaller: unused `threshold` CLI param in `predict_upcoming`; `int(NaN)` on
  missing goals in calibration scripts; empty `IN ()` SQL edge cases.
- [ ] Recheck Poisson bias at each calibration refresh.

Done since July: Dixon-Coles feeds consensus; real SportyBet odds and EV in
slips; engine Airflow DAGs (`engine_predict`, `engine_calibration`);
dependency-aware model result reuse (2026-10-19).

## Product and frontend

Details: [docs/product/implementation-status.md](docs/product/implementation-status.md).

- [ ] Saved forecasts: Saved-account and sidebar work is local only, excluded
  from the 2026-10-18 frontend release.
- [ ] Figma: Settings (timezone, followed leagues) not yet designed.
- [ ] Show the API's new `data_quality` block (D148) on the match page.
- [ ] D146 evidence-depth bar (Focused / More context / Full view): owner review.
- [ ] Deployment: nothing is deployed; frontend is on `codex/frontend-redesign`.
- [ ] Desktop Match details: production implementation; sidebar and H2H
  prototype navigation regression
  ([docs/product/desktop-match-details-todo.md](docs/product/desktop-match-details-todo.md)).
- [ ] Design backlog: onboarding / first run; real Strength content (settle
  the Naples label and Big dog threshold rule first); consistent mock/live labels.
