# Dependency-aware model processing

Implemented locally 2026-10-19. The daily `run_model` entry point now combines
reused and recalculated results into the same complete `raw.model_picks` output.
Bayesian full analysis remains in `raw.model_analysis`. Model readiness and
consensus selection remain separate; settlement is unchanged.

## Dependencies actually checked

| Scope | Inputs fingerprinted before reuse |
|---|---|
| Mismatch fixture | Fixture identity/names/kickoff, both current Elo ratings, minimum gap, relevant league **and global fallback** hit-rate tables, pair H2H context and both team strength profiles |
| Bayesian team/context | Ordered current-season event history, relevant league/event base rates, prior strength, context |
| Bayesian fixture | Fixture metadata, both teams' computed overall/venue beliefs and below-threshold inclusion setting |
| Dixon–Coles league | Actual xG-blended history, actual integer age-in-days vector used by the fitter and decay coefficient |
| Dixon–Coles fixture | Fixture metadata, fitted league parameters and prediction threshold |

Shared rate fingerprints are calculated once per league/context in a run. League
changes invalidate affected teams/fixtures; Mismatch's global fallback can also
legitimately invalidate other leagues. Source corrections are detected from
content, not just row count, maximum ID or update timestamp. Fixture loaders still
select the current prediction window; absent/finished fixtures do not reappear
just because an old cache entry exists.

Dixon–Coles floors age to whole days. The exact vector is included rather than
ignoring the clock or changing model semantics. A new age vector can require a
new league fit with no new results. Reusing a fit is allowed only when those
weights and history match. Even after refitting, unchanged fitted parameters
allow fixture-level reuse.

Implementation/calibration file hashes and Python/NumPy/pandas/SciPy versions
are also included. Source invalidation is deliberately conservative across the
engine: a code change can invalidate more scopes than necessary. Daily run date
and processing timestamp are not dependencies. Odds are handled in fresh consensus
assembly; these three model computations do not consume odds.

## Storage and success boundaries

`raw.model_processing_state(scope, entity_key)` holds the current input hash,
result ID and last successful processing time. `raw.model_result_versions` holds
append-only-by-writer output versions, their hashes and creation time. Input
changes with identical output update processing state without adding an output
version. A → B → A creates three version events. Empty results are cacheable;
a missing cache is different from a cached empty result.

The cache uses typed JSON, never pickle, preserving numeric team keys and
timestamps. A per-model advisory lock serializes normal model runners. New state
and versions remain in a transaction until the model finishes and its picks are
written. Failures roll back pending cache updates; the existing runner marks the
model failed, so failed computations are not advertised as ready. Source reads
remain necessary to establish current dependencies; this does not eliminate all
shared SQL/feature preparation work.

Run bootstrap is idempotent and serialized. Tables are created before use; no
backfill of fabricated historic processing fingerprints occurs. First execution
is cold. Engine source is already bind-mounted by the existing Airflow model
DockerOperators, so their unchanged `run_model` commands use this behavior on the
next run. No prediction feed was regenerated during verification.

## Verification and operations

```sh
# Normal daily run: reuse matching computations
python -m engine.eval.run_model --model mismatch --run-ds YYYY-MM-DD --days 7
# Force calculations but still avoid identical output versions
python -m engine.eval.run_model --model mismatch --run-ds YYYY-MM-DD --days 7 --full-recompute
# Read live inputs; compare cold/warm/forced; rollback test cache writes; no publication
python -m scripts.verify_model_reuse --model mismatch --days 7
python -m scripts.verify_model_reuse --model bayesian --days 7
python -m scripts.verify_model_reuse --model dixon_coles --days 7
```

The verifier freezes the DC clock across all three calculations. Runner signal
ordering is deterministic so query tie ordering cannot masquerade as an output
change. Unit tests cover result codec and storage tests cover reuse, league
invalidation, same-output version preservation, A→B→A, empty results, scope
isolation and failed-computation retry. PostgreSQL tests use synthetic scopes and
roll back all test versions/state.

## Relationship to user-facing versions

These are **model-computation versions**, not new forecast IDs or user saves.
Existing `forecast.snapshots` and `forecast.entries` retain their original meaning.
The daily feed still records its actual run metadata, so a new engine snapshot
can contain reused model outputs. S3 incremental archival and settlement IDs do
not change. Connecting comparable card-content versions to Saved's “updated”
indicator and current Upcoming/Finished status remains separate frontend/account
work; this change does not claim those UI features are delivered.


## Verified 2026-10-19

Live seven-day-window cold/warm/forced comparisons passed for all three models:
Mismatch 246 computations reused; Bayesian 4,088 team/context + fixture computations
reused; Dixon–Coles 684 league + fixture computations reused across 137,123 historical
matches. All returned signals/audit data matched exactly after deterministic signal
ordering and a history fixture-ID tie-breaker; DC reference time was frozen. Test
cache writes rolled back, no model picks/forecast snapshots/feed were published.
37 focused model/consensus/history/store tests passed (three opt-in pre-existing
forecast database tests skipped); three explicit cache PostgreSQL/codec tests passed.
The existing daily Docker tasks bind-mount the engine and call this updated runner;
the next production task starts cold because validation did not seed its state.
