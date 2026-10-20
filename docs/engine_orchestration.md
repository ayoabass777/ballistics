# Engine Orchestration — Design (not yet built)

> **2026-10-20:** the SportyBet odds scraper (`odds_scrape` DAG), EV and betting
> slips were removed. References to them below are historical.

2026-10-11 — **Poisson GLM retired.** It fed nothing in the product and the slips are retired. `engine_predict` runs mismatch, bayesian and dixon_coles (quorum 2 of 3); `calibration_wf` no longer fits a Poisson map. Consensus goal rules: Match Over 1.5 and Match Under now need Dixon-Coles alone (were DC + Poisson), which feeds the Over 1.5 shortlist. `predict_upcoming.py` stays as a library (shared loaders and `_tier`).

2026-10-08 — `engine_predict` gate moved from `fixture_update_dag.extract_xg_task` to `fixture_update_dag.run_dbt` (task `wait_for_dbt_refresh`). The engine reads dbt facts (`raw.fact_head_to_head*`, `raw.fact_fixture_xg`); Airflow logs for Oct 5–7 showed every model finishing before `run_dbt` started, i.e. training on the previous night's facts. Side effect: if `run_dbt` fails, the engine does not run that night (previously it ran on stale facts).

Status: **design only** (July 3 2026). Decision: run engine code via **DockerOperator +
a dedicated `ballistics-engine` image**, mirroring how dbt already runs. This keeps the
Airflow image slim (it lacks scipy/statsmodels today, which Poisson and Dixon-Coles need).

Only the odds scraper is on Airflow so far (`odds_scrape` DAG). This doc plans bringing
prediction, consensus, and calibration under Airflow too, with a fan-out on the models.

## Target architecture

Three DAGs on three cadences — coupled only through durable artifacts (a DB table and
`calibration_maps.json`), never through XCom payloads or in-process calls:

```
  calibration DAG  (weekly / manual)
     calibration_wf ─► engine/eval/calibration_maps.json  (PAVA maps)
                              │  loaded by every predictor at runtime
                              ▼
  engine DAG  (daily)
     ┌─ poisson ──┐   each task, on finish, writes:
     ├─ mismatch ─┤     • its picks → raw.model_picks
     ├─ bayesian ─┤     • a readiness marker → raw.model_runs
     └─ dixon ────┘
            ┆  (no hard fan-in — see below)
            ▼
     quorum sensor  ──► fires when ≥N models ready OR grace window elapsed
            ▼
        consensus  ──►  reads raw.model_picks for run_ds (+ latest raw.sportybet_odds)
            │           writes raw.consensus_pool (history) + consensus_slips_<ds>.json (UI)
            ▼
        publish (optional: land slips JSON to S3 / notify)

  odds_scrape DAG  (every 4h — already built, independent)
     fixtures_from_db ─► scrape ─► raw.sportybet_odds (+ S3)
```

### Scheduling model — event-based, not a hard fan-in

A DAG/task starts either on a **schedule** (a clock: `@daily`, `0 */4 * * *`) or on an
**event** (`TriggerDagRunOperator`, Airflow **Datasets**/data-aware scheduling, or a
**readiness marker + sensor**). Consensus must NOT wait on all four models — a slow or
failed model should never block the slip. So consensus is **event-gated**:

- Each model task upserts `raw.model_runs (run_ds, model, status, n_picks, finished_at)`
  the moment it finishes — a durable readiness marker (not XCom).
- A **reschedule-mode quorum sensor** gates consensus: `poke` returns True when
  `count(distinct model where status='ok' and run_ds=…) >= QUORUM` **OR** the grace window
  since the first marker has passed. Reschedule mode frees the worker slot between pokes.
- Consensus then builds from **whatever picks landed** for `run_ds`. It records which
  models contributed, so a 3-of-4 run is first-class, not an error.
- **Odds stay decoupled**: the 4h scraper runs on its own clock; consensus reads the
  *latest* `raw.sportybet_odds` and records each price's capture age — it never blocks on a
  fresh scrape. (Datasets are the fancier alternative but fire on *any* update, which
  doesn't give quorum/timeout semantics — hence the marker table.)

Why three DAGs, not one:
- **Calibration is expensive** (walk-forward over ~60k preds, minutes) and only needs to
  re-run when model logic or the underlying data materially changes — not per prediction.
- **Odds drift** and books open ~T-3, so the scraper wants a tight ~4h loop independent of
  the daily prediction run.
- **Prediction** is the daily driver; it should consume the latest maps + latest odds.

## The `ballistics-engine` image

Mirror `dbt/Dockerfile` → `ballistics-dbt`. New `engine/Dockerfile` → `ballistics-engine`:

- `FROM python:3.12-slim`
- Install the heavy stack Airflow lacks: `numpy pandas scipy statsmodels psycopg2-binary
  python-dotenv boto3 pyyaml`.
- `COPY engine/ etl/` (engine imports `etl.src.logger` / `etl.src.s3_landing`).
- Entrypoint pattern: `python -m engine.eval.<module>` chosen per task via DockerOperator
  `command`.

DockerOperator config (per task), copied from the dbt tasks in `fixture_update.py`:
- `image="ballistics-engine:latest"`, `auto_remove=True`, `network_mode="etl_network"`
  (so it resolves `ballistics_postgres`), `mount_tmp_dir=False`.
- Mounts: bind `${PROJECT_ROOT}/engine` → `/opt/engine` so `calibration_maps.json` and the
  output `consensus_slips_<ds>.json` persist on the host across containers (same trick the
  odds mount uses).
- Env: `DB_NAME/DB_USER/DB_PASSWORD/DB_PORT` + `ENGINE_DB_HOST=ballistics_postgres`
  (already the pattern from `db.py::get_conn`).

## Required refactor (the crux)

Today the four models are **in-process calls inside `run_consensus()`**
(`consensus_slips.py:464–489`): `predict_upcoming()`, `predict_mismatch()`,
`get_fixture_beliefs()`, `predict_dixon_coles()`. To fan them out they must become
independent steps that persist their signals:

1. **Each model → a runnable module that writes to `raw.model_picks`.** Proposed schema:
   ```sql
   raw.model_picks (
     run_ds        date,          -- prediction run date
     model         text,          -- 'poisson'|'mismatch'|'bayesian'|'dixon_coles'
     fixture_id    bigint,
     unified_market text,         -- already the consensus join key
     probability   numeric,       -- calibrated
     raw_probability numeric,
     detail        jsonb,         -- model-specific (gap, streak, lambdas, ...)
     PRIMARY KEY (run_ds, model, fixture_id, unified_market)
   )
   ```
   Most models already produce these fields; this is mostly moving the `_normalise_market`
   mapping to write-time.

   Plus a **readiness marker** each model upserts on completion (drives the quorum sensor):
   ```sql
   raw.model_runs (
     run_ds      date,
     model       text,
     status      text,        -- 'ok' | 'empty' | 'error'
     n_picks     int,
     finished_at timestamptz,
     PRIMARY KEY (run_ds, model)
   )
   ```

2. **Split `run_consensus()`** into:
   - `collect_signals()` — the per-model calls (now replaced by reading `raw.model_picks`).
   - `build_consensus_from_signals(signals, odds)` — the existing merge / tier / goal-model
     rules / EV join / slip build. This becomes the `consensus` task and needs **no** model
     imports (so a lighter consensus container is possible).

   Consensus **writes its pool to the DB** (not just the dashboard JSON):
   ```sql
   raw.consensus_pool (
     run_ds        date,
     fixture_id    bigint,
     unified_market text,
     sportybet_market text, sportybet_selection text, line numeric,
     n_models      int,   models text[],
     avg_confidence numeric, max_confidence numeric,
     real_odds numeric, ev numeric, has_odds bool, est_odds numeric,
     market_tier text, strength_rank int,
     league text, country text, kickoff timestamptz,
     odds_captured_at timestamptz,   -- price staleness
     generated_at timestamptz,
     PRIMARY KEY (run_ds, fixture_id, unified_market)
   )
   ```
   This gives queryable slip history for hit-rate / ROI tracking (ties into the betting
   log); `consensus_slips_<ds>.json` stays as the dashboard feed.

3. **Calibration output location.** `calibration_wf` already writes
   `engine/eval/calibration_maps.json`; with the engine bind-mount, the calibration
   container writes it and the prediction containers read it via the same host path. No
   change needed beyond the mount. (Alt: land maps to S3 and load from there — cleaner for
   a multi-host setup, optional.)

## Task graphs

**`engine_calibration`** — `schedule="@weekly"`, catchup=False:
```
calibration_wf  (DockerOperator: python -m engine.eval.calibration_wf)
```

**`engine_predict`** — `schedule="@daily"`, catchup=False, `max_active_runs=1`:
```
wait_for_xg_refresh ─┬─► refresh_elo ─► mismatch ─┐
                     └─► [poisson] [bayesian] [dixon] ─► wait_for_quorum ─► consensus
                                                                          │
                                                                          ▼
                                                               verify_daily_shortlists
```
- `refresh_elo` processes newly landed completed fixtures before mismatch reads
  team ratings. The other three models remain parallel because they do not use Elo.
- Each model task: `python -m engine.eval.<model> --write-picks --run-ds {{ ds }}` — writes
  `raw.model_picks` and a `raw.model_runs` marker.
- `wait_for_quorum`: reschedule-mode sensor, `trigger_rule="all_done"` (runs even if some
  model tasks failed). Fires on `ready_models >= QUORUM` **or** grace-window timeout.
- `consensus`: `python -m engine.eval.consensus_slips --from-picks --run-ds {{ ds }}` —
  reads `raw.model_picks` for `run_ds`, joins latest `raw.sportybet_odds`, writes
  `raw.consensus_pool` + JSON.
- `verify_daily_shortlists`: checks that the JSON belongs to the current `run_ds`, was
  generated within six hours, and contains freshly stamped 1–4, Over 1.5, and Elo-gap
  sections. Empty lists are valid when no events qualify; missing or stale sections fail
  the daily DAG instead of leaving the dashboard silently outdated.

**`odds_scrape`** — already built, `schedule="0 */4 * * *"`, fully independent.

## Decisions (resolved July 3 2026)
- **Fan-in = event-based, never blocking.** Readiness markers (`raw.model_runs`) + a
  reschedule quorum sensor. Consensus proceeds on `QUORUM` (start: 3 of 4) or after a grace
  window, using whatever picks landed. A failed/slow model degrades gracefully.
- **Consensus pool → DB.** `raw.consensus_pool` (schema above) for queryable history +
  ROI/hit-rate tracking; JSON kept for the dashboard.
- **Consensus is event-gated, odds are clock-scheduled and decoupled.** Consensus is
  triggered by model readiness, not a fixed time; it uses the latest odds and stamps their
  capture age rather than waiting on a fresh scrape.
- **Idempotency yes; backfill is bounded.** `run_ds`-keyed upserts make re-runs of a day
  safe and deterministic. True historical backfill is limited: live prediction is
  forward-looking (upcoming NS fixtures), so a past `run_ds` can only be reconstructed where
  the fixtures'/odds' as-of state still exists. Design each model query to scope "upcoming
  as of `run_ds`"; treat idempotent same-day re-runs as the primary goal, deep backfill as
  best-effort. (Calibration is already leak-free walk-forward.)

## Still open (smaller)
- `QUORUM` value and grace-window length (start 3 / ~30–45 min; tune once live).
- Whether `publish` also lands the slips JSON to S3 and/or notifies.
- Optional `raw.consensus_slips` table (assembled accumulators) alongside `raw.consensus_pool`
  (per-leg pool).

## Not doing yet
No code changes in this pass — this is the plan. Building it means: `engine/Dockerfile`
+ image build, the `raw.model_picks` table + `--write-picks` on each model, the
`run_consensus` split, and the two new DAGs.
