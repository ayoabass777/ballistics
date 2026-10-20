# To-do

One list for open work across the pipeline, engine and product. Detailed
history stays where it was recorded (linked); this file is the index.
Updated 2026-10-20 (after the betting workflow removal, D149).

## Now

- [ ] **Check tonight's engine run** (first with model reuse and card versions):
  `raw.model_processing_state` should fill, the run should start cold, and
  `raw.card_versions` should gain rows beyond the 5,265 backfilled on 2026-10-20.
- [ ] **Local Python has stale tz data** (Anaconda `share/zoneinfo` is 2025b), so
  `test_vancouver_stays_on_utc_minus_7_after_november_2026` fails and the local
  API on port 8000 would show BC kickoffs an hour off after 2026-11-01. The
  engine image (Debian tzdata 2026b) is correct. `conda install tzdata=2026c`
  fails on an inconsistent base env (`pluggy`); `PYTHONTZPATH=/usr/share/zoneinfo`
  (macOS data, 2026c) fixes it.

## Pipeline (DLQ, D147/D148)

- [ ] A clean fixture-row DLQ replay reloads the whole league season
  (`load_league_fixtures`: DELETE + COPY), which gives every row a new
  `fixture_id`; forecasts, card versions, consensus pool and facts key on it.
  Replay should replace only the parked fixtures, keeping existing ids.
- [ ] ~110 fixtures stuck at NS from postponements (Egyptian league, USL Tormenta).

## Engine

- [ ] Decide whether to drop the dead betting data: `raw.sportybet_odds` and the
  `est_odds/real_odds/odds/ev/has_odds` columns of `raw.consensus_pool` (kept, D149).
- [ ] Rename `sportybet_market` / `sportybet_selection` to neutral market names
  (pool rows, `raw.consensus_pool`, grading, dashboard, tests). Deferred in D149.

- [ ] Learned stacker to replace tier voting (logistic regression per market
  family on the models' probabilities + league base rate); also fixes the
  double-counted Bayesian signals in `avg_conf`.
- [ ] Gate Bayesian picks on edge over the league base rate, not an absolute 0.65.
- [ ] Align backtest and live Bayesian base rates (current season vs all
  history before the cutoff).
- [ ] Smaller: unused `threshold` CLI param in `predict_upcoming`; `int(NaN)` on
  missing goals in calibration scripts; empty `IN ()` SQL edge cases.
- [ ] Recheck Poisson bias at each calibration refresh.

Done since July: Dixon-Coles feeds consensus; engine Airflow DAGs
(`engine_predict`, `engine_calibration`); dependency-aware model result reuse
(2026-10-19). SportyBet odds, EV and slips were built and then removed (D149).

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
