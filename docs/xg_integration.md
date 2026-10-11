# Expected Goals (xG) Integration

2026-10-08 — **Lineage: the engine reads xG through dbt.** `raw.fixture_xg` (ETL, unchanged) is a dbt source → `stg_raw_fixture_xg` (view, rename/cast) → `fact_fixture_xg` (table in `raw`, fixture grain, played fixtures with both sides' xG, warehouse team ids from `int_fixtures`). `predict_dixon_coles.blend_xg` (also used by `calibration_wf`) reads `raw.fact_fixture_xg` (`XG_TABLE`). The fact is a full rebuild, not incremental: xG lands after the result without touching `raw_fixtures.updated_at`. Tests: unique/not-null `fixture_id`, relationship to `stg_raw_fixtures`, non-negative xG, source freshness warn at 72 h on `captured_at`. `engine_predict` now waits for `fixture_update_dag.run_dbt` (was `extract_xg_task`), so xG and all dbt facts are same-night. Switch check: `python -m engine.eval.diagnostics.xg_lineage_check`.

Status: **scheduled** (updated August 6 2026). xG is ingested from API-Football after
the daily fixture-result update and blended into the Dixon-Coles goals model for the
leagues that have verified coverage.

## Why (and why it's narrow)

xG is a lower-variance signal than raw scorelines, so xG-informed attack/defence rates
predict better than goals alone. But a coverage probe over the pool's leagues showed
**only ~18% of historical fixtures have xG** — it exists for top-flight leagues and is
absent for the lower-tier bulk of the pool. Free alternatives don't help: Understat is
top-5 only; FBref/StatsBomb would add a couple more but not the smallest divisions.

So xG is a **per-fixture, opt-in enhancement for a handful of leagues**, not a
foundation. Non-covered fixtures are untouched and stay on actual goals.

### Coverage (API-Football, re-probed August 6 2026)

| League group | Ten-fixture probe |
|---|---|
| 100% complete | Premier League and Championship (England), Ligue 1, Brazil Serie A/B, Bundesliga and 2. Bundesliga, Primeira Liga, Eliteserien, Ekstraklasa, Danish Superliga, Italy Serie A/B, La Liga, Jupiler Pro League, Scottish Premiership, Süper Lig, Swiss Super League, Egypt Premier League, Czech Liga, Ireland Premier Division |
| 80% complete | Segunda Division (Spain), Major League Soccer |
| 50% complete | Eredivisie recent sample; full-season warehouse coverage is 89–98% across 2023–2025, and provider delivery can lag on the newest fixtures |
| Historical/intermittent | Allsvenskan and Chinese Super League retain useful 2025–2026 xG already in the warehouse, although the latest three-fixture probe returned none |
| No xG in sample | Remaining 25 tracked leagues; they stay on actual goals |

The 26 verified or historically useful league ids are the explicit `XG_LEAGUES`
allowlist. A league is not promoted solely because the endpoint returns other match
statistics.

## Data ingestion — `etl/src/extract_xg.py`

Pulls `expected_goals` from API-Football's `fixtures/statistics` endpoint and stores it
keyed by the **shared `fixture_id`** (same across `raw_fixtures` and `fact_head_to_head`).

```sql
raw.fixture_xg (
    fixture_id     BIGINT PRIMARY KEY,   -- shared id, the DC join key
    api_fixture_id BIGINT,
    home_xg        NUMERIC,
    away_xg        NUMERIC,
    captured_at    TIMESTAMPTZ DEFAULT now()
)
```

- Home/away mapping: the API returns stats per team id; we match `team.id` to
  `raw_fixtures.home_team_id` (both API-team-id space) to assign home vs away xG.
- Only FT fixtures in `XG_LEAGUES`, and by default only those **missing** xG (idempotent
  backfill). `--all` re-fetches; `--limit N` caps a batch.
- **Recent seasons by default** (`RECENT_SEASONS = [2025, 2026]`). Dixon-Coles time-decays
  history (half-life ~107 days), so older seasons contribute ~nothing to the fit and just
  burn API calls — no reason to pull 2020–2024. Override with `--seasons 2024,2025,2026`.
- Must run where the API and DB are reachable (the Airflow container):

```bash
  docker exec -w /opt/airflow ballistics_airflow_scheduler \
    python -m etl.src.extract_xg          # backfill missing, recent seasons
```

Eredivisie was added after a direct provider check found xG for PSV's 2023/24,
2024/25, and 2025/26 opening fixtures. The two older opening fixtures returned
statistics but no `expected_goals` values. To run that historical backfill only:

```bash
python -m etl.src.extract_xg --leagues 88 --seasons 2023,2024,2025
```

## Coverage discovery and monitoring

The production allowlist stays explicit, but provider support can be rediscovered
without editing it first:

```bash
# Sample three recent completed fixtures from every tracked league.
python -m etl.src.extract_xg --probe-all-leagues --probe-sample-size 3

# Optionally keep valid xG returned by those probe calls.
python -m etl.src.extract_xg --probe-all-leagues --probe-sample-size 3 --store-probe-xg

# Report warehouse completeness at league-season grain.
python -m etl.src.extract_xg --coverage-report
```

Probe results are stored in `raw.xg_league_probes`. Current warehouse coverage is
exposed by `raw.xg_league_coverage`, including completed fixtures, complete xG rows,
coverage percentage, missing/error attempts, and the last xG capture/attempt time for
every league-season. `raw.fixture_xg_attempts` prevents fixtures with no provider xG
from consuming the same API calls every day: missing values retry after 30 days and
errors retry after one day.

## Model blending — `predict_dixon_coles.blend_xg()`

```python
blend_xg(matches, conn, weight=XG_WEIGHT)   # XG_WEIGHT = 0.5
```

LEFT-joins `raw.fixture_xg` onto the historical matches and, **per fixture where xG
exists**, replaces the observed goals with `(1 - weight)*goals + weight*xg` (each side
independently; a fixture with only one side's xG blends only that side). Fixtures with no
xG are unchanged. No-op if the table is absent or `weight <= 0`.

Wired into **both** paths so fit and eval stay consistent:
- **Live**: `predict_dixon_coles()` blends right after `load_dc_historical()`.
- **Walk-forward**: `calibration_wf._dixon_coles_fold()` blends the pre-cutoff training
  matches. Because it only touches matches already played before the fold cutoff, the xG
  (a post-match stat known by then) introduces **no leakage**.

### Effect (sanity check)

For Eliteserien the blend shifts the fit sensibly: `home_adv 0.469 → 0.419`, mean
`|Δ attack rate|` across teams ≈ 0.032.

## Caveat — the Dixon-Coles rho correction softens

DC's `rho` low-score correction keys off integer scorelines (0-0, 1-0, 0-1, 1-1). Blended
goals are continuous, so those exact-integer masks rarely fire on covered-league rows and
the correction weakens there. This is an accepted tradeoff: the better rate signal from xG
outweighs a small low-score dependence tweak on ~5 leagues. If it ever proves harmful,
options are: lower `XG_WEIGHT`, gate the blend to only the highest-coverage leagues, or
keep integer goals for the rho mask while using blended goals only for the Poisson mean.

## Tuning knobs

- `XG_WEIGHT` (`predict_dixon_coles.py`) — blend strength; 0 disables, 1 = pure xG.
- `XG_LEAGUES` (`extract_xg.py`) — which leagues to ingest xG for.

## Operations

- **Re-run calibration after any material historical xG backfill**
  (`python -m engine.eval.calibration_wf`):
  the blend affects DC's fitted probabilities, so its PAVA map must be refit on the blended
  signal. Live DC auto-loads the refreshed `calibration_maps.json`.
- **Scheduled daily.** `fixture_update_dag.extract_xg_task` runs after fixture results and
  corrections load, then records a `raw.fixture_xg` table snapshot and logs configured
  league-season coverage. The task processes at most 250 missing fixtures per run by
  default (`XG_INGEST_LIMIT`) so a historical backlog cannot monopolize the daily DAG.
- **Safe partial coverage.** Fixtures without xG remain on actual goals because the
  Dixon-Coles blend is a LEFT JOIN and only changes rows with available xG.

## Validation

Re-ran `calibration_wf` (walk-forward, folds 5-9) after wiring the blend, on the complete
recent-season xG set (1,282 fixtures, seasons 2025-26):

| | DC Brier | DC pred | DC actual | gap |
|---|---|---|---|---|
| baseline (no xG) | 0.1849 | 74.0% | 74.8% | −0.8% |
| **xG blend (w=0.5)** | **0.1837** | 73.6% | 75.0% | −1.4% |

**Net effect: a small improvement** (0.1849 → 0.1837, ~0.6% relative) with DC staying
well-calibrated. The gain is modest because the blend touches only the ~5 covered leagues,
a slice of DC's ~4,800 walk-forward predictions — so a real per-league improvement is
diluted in the overall number. Importantly the rho-softening caveat did **not** hurt.
(An earlier run on partial xG data — only ~300 fixtures ingested mid-backfill — showed
0.1850, i.e. neutral; coverage matters.)

Maps were refreshed on the blended fits, so live DC and the calibration map are consistent.
To push further, measure DC Brier **restricted to the covered leagues** (isolates the
signal from the average) before raising `XG_WEIGHT` above 0.5.
