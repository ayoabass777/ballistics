# Expected Goals (xG) Integration

Status: **built** (July 4 2026). xG is ingested from API-Football and blended into the
Dixon-Coles goals model for the leagues that have coverage.

## Why (and why it's narrow)

xG is a lower-variance signal than raw scorelines, so xG-informed attack/defence rates
predict better than goals alone. But a coverage probe over the pool's leagues showed
**only ~18% of historical fixtures have xG** — it exists for top-flight leagues and is
absent for the lower-tier bulk of the pool. Free alternatives don't help: Understat is
top-5 only; FBref/StatsBomb would add a couple more but not the smallest divisions.

So xG is a **per-fixture, opt-in enhancement for a handful of leagues**, not a
foundation. Non-covered fixtures are untouched and stay on actual goals.

### Coverage (API-Football, probe July 4 2026)

| League (api_league_id) | xG coverage |
|---|---|
| Eliteserien (103) | ~100% |
| Brazil Serie B (72) | ~88% |
| Chinese Super League (169) | ~75% |
| Allsvenskan (113) | ~38% |
| Ireland Premier Division (357) | ~38% |
| everything else in the pool | ~0% (often no stats at all) |

These five are the `XG_LEAGUES` allowlist. See memory `xg-data-coverage`.

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
- **Recent seasons only** (`RECENT_SEASONS = [2025, 2026]`). Dixon-Coles time-decays
  history (half-life ~107 days), so older seasons contribute ~nothing to the fit and just
  burn API calls — no reason to pull 2020–2024. Override with `--seasons 2024,2025,2026`.
- Must run where the API and DB are reachable (the Airflow container):

```bash
docker exec -w /opt/airflow ballistics_airflow_scheduler \
    python -m etl.src.extract_xg          # backfill missing, recent seasons
```

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

- **Re-run calibration after any xG change** (`python -m engine.eval.calibration_wf`):
  the blend affects DC's fitted probabilities, so its PAVA map must be refit on the blended
  signal. Live DC auto-loads the refreshed `calibration_maps.json`.
- **Not yet on a schedule.** Ingestion is manual/one-off today. To keep xG fresh, add an
  `extract_xg` step to the fixture pipeline (after results load) or a small standalone DAG,
  the same way `odds_scrape` runs.

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
