# Prospective forecast history

Implemented locally 2026-10-02. No live database migration, deployment, history backfill or frontend integration was performed. FigJam generation awaits team/organisation selection.

## Capture and settlement, in plain language

**Capture records what we forecast and when.** The engine saves the forecast output, its probability, supporting context and the database receipt time before publishing the mutable feed. Eligibility checks decide whether an entry can join the prospective record. Capture does not train or rerun a model, and does not prove the forecast was displayed to a user.

**Settlement records what happened to that forecast.** It checks the selected latest eligible pre-kickoff forecast against the current fixture status and regulation-time score, then appends an outcome observation. It can record `pending` before a result is available, not just `hit` or `miss` after full time. Here settlement means forecast grading, not a payment or betting transaction.

Illustrative example: an eligible capture at 10:00 saves “Over 2.5 goals — 72%” for an 18:00 kickoff. An eligible 17:00 capture of 74% is a separate snapshot and becomes the evaluated forecast, provided no later eligible version arrives before kickoff. Both probabilities remain stored. A final score of 2–1 produces a `hit` observation against the selected 74%. If the source later corrects that score to 1–1, settlement appends a `miss` observation. Neither probability is rewritten.

## Contract

- Policy `latest_pre_kickoff_dc_goals_v1` (2026-10-03, supersedes first-capture selection): one latest eligible captured DC goal forecast per fixture/event. Selection advances with eligible pre-match captures, never based on which probability performed better. Home/away identities are normalised independently of team display names. Each event is selected independently; omission from a later payload does not erase an earlier event.
- Coverage is **exported DC goal events**, not every potential model forecast, not the consensus pool, and not only winning or Discover-selected records. Model output thresholds still affect this population. Win/strength signals and historical pattern frequencies are not graded as goal predictions.
- Capture uses the database clock, not `run_ds`, file timestamps or a caller-supplied timestamp. Both the exported kickoff and current database kickoff must be strictly later than capture. Database status must be `NS` or `TBD`. Other attempts remain archived with an ineligibility reason.
- A fixture cannot reopen for eligible captures after any eligible capture's exported or database kickoff under this policy has passed. New attempts are retained as `previously_captured_kickoff_reached`. This conservative cutoff also applies to reschedules: moving kickoff later does not extend selection beyond an already captured earlier cutoff. Source corrections can exclude a selected forecast, but never cause fallback to a more favourable older version.
- Exact-payload retries return the original snapshot. Changed payloads append. Each snapshot preserves the complete consensus export, including patterns, strength context, thresholds and available calibration metadata. These non-DC fields are audit context, not extra graded forecasts.
- The CLI captures before replacing its mutable JSON feed. An archive failure fails publication. A later filesystem-publication failure can leave an archived calculation that was never displayed; call these **captured forecasts**, not proof of user-visible publication.
- Existing daily files are **legacy saved outputs**. No import path certifies them as pre-match forecasts. `results_review.py` and its old dashboard results remain legacy and are not the new track record.

## Data model

| Relation | Grain / identity | Purpose |
| --- | --- | --- |
| `forecast.snapshots` | One content-hashed captured payload | Actual receipt timestamp, logical run date, complete output and capture-time file hashes |
| `forecast.entries` | Snapshot + fixture + goal event | Probability, captured kickoff, eligibility, policy and exact DC/fixture context |
| `forecast.latest_forecasts` | Fixture + event + policy | Active read-only view selecting the latest eligible snapshot; descending timestamp and ID tie-breakers |
| `forecast.settlements` | One changed observation of a selected entry | Append-only outcome, source score/status/kickoff, grader version and observation timestamp |

Snapshots have many entries. Each selected entry has many settlement observations. The report uses the latest observation but older observations remain queryable. A score changing A → B → A creates three observations; only consecutive identical observations are deduplicated.

Before kickoff, selection is provisional and can advance to a newer eligible entry. Any pending observations for the replaced entry remain stored; they are not transferred to its replacement. The report exposes outcome history for the currently selected entry. The older `forecast.first_forecasts` view remains available for historical inspection, but is no longer used by the active report or settlement writer.

## Keys, transactions and overwrite protection

The implementation uses a combination of keys, transactional writers, selection rules and database mutation guards—not a key alone.

| Protection | Current implementation |
| --- | --- |
| Snapshot retry identity | `UNIQUE(content_hash)`, using SHA-256 of the complete canonically encoded JSON payload. An identical retry returns the existing snapshot ID. A changed payload, including changed metadata, creates another snapshot. This hash is a deduplication key, not a signature or proof against tampering. |
| Entry identity | `UNIQUE(snapshot_id, fixture_id, event_key)`. This prevents duplicates within a snapshot while allowing later snapshots of the same fixture/event. |
| Atomic capture | The snapshot and all its entries are inserted within one PostgreSQL transaction: successful completion commits; an exception rolls back the transaction. This relies on the normal non-autocommit connection used by the writer. |
| Cooperating writer concurrency | Capture, settlement and schema initialisation acquire transaction-scoped advisory lock `734120260`. This serializes these writers and releases the lock when their transaction ends. Other writers must follow the same protocol; an advisory lock does not automatically block unrelated SQL. |
| Forecast selection | `forecast.latest_forecasts` selects the latest eligible entry for each fixture/event/policy, ordered by capture time, snapshot ID and entry ID descending. Exact retries keep their original IDs/timestamps and cannot promote an older forecast. Late captures remain ineligible and cannot replace the selection. |
| Atomic settlement | Each settlement run appends its changed observations in one transaction. It compares each observation's evidence hash with that entry's latest observation and skips consecutive identical evidence. This is writer-level deduplication under the lock, not a unique constraint on settlement hashes. |
| Mutation prevention | Database triggers reject `UPDATE`, `DELETE` and `TRUNCATE` on snapshots, entries and settlements. Corrections are new observations, never edits to the forecast probability. |

The approved capture writer obtains `recorded_at` from `clock_timestamp()` and does not accept a caller-supplied capture timestamp. The schema does not independently prevent an alternative authorised SQL writer from inserting a fabricated timestamp. Restricted runtime roles and control over all insert paths are therefore part of deployment hardening, not an already-established guarantee.

The database transaction and JSON-file publication are **not one atomic transaction**. Capture commits first; publication follows. Publication failure can therefore leave a captured but undisplayed forecast, while capture failure must stop publication.

Documentation checked against `engine/eval/forecast_history.py` on 2026-10-03. These are local implementation properties, not evidence of live deployment; database-owner limits and outstanding hardening are listed below.

## Settlement policy

Grader `regulation_goals_v1` supports match/team half-goal totals, both-teams-to-score and inclusive match goals 1–4. Integer/quarter lines are not silently interpreted.

| Fixture state | Record status |
| --- | --- |
| `FT` with valid regulation score | `hit` or `miss` |
| Scheduled, postponed, suspended or in play | `pending` |
| `CANC` | `void` |
| Abandoned, awarded, walkover, extra-time/penalty final, unknown state or missing score | `unavailable`, requiring a future explicit rule/manual review |
| Corrected kickoff at/before original capture | `excluded`; retain entry and reason rather than substitute a later forecast |

Postponement retains the same fixture/event and captured versions, subject to the conservative capture cutoff above. A new provider fixture ID is a separate identity; no automatic reschedule matching. Cancellation may be revised if source state changes; the observation history preserves both states. Pending/void/unavailable/excluded are not hits or misses. No pooled accuracy badge is generated across correlated markets.

## Integration and operation

`consensus_slips.py` CLI calls `save_slips(..., record_history=True)`. Direct library/offline calls default to no capture to keep previews/tests from writing live history; production wrappers must opt in. Schema initialisation is idempotent and serialized with capture/settlement. The existing mutable `raw.model_*`, `raw.consensus_pool` and JSON feed remain working stores.

Policy transition: run schema initialisation before using the new writer/report. It adds `forecast.latest_forecasts` without rewriting entries, settlements or the legacy view. Old `first_pre_kickoff_dc_goals_v1` rows remain under their original policy and are excluded from the active latest-policy report. An exact retry of an old payload still returns its old snapshot, not a backdated new-policy entry; a changed payload can be captured under the new policy at its actual new receipt time. No policy backfill or live migration was performed.

The daily engine DAG adds schema initialisation and settlement after its xG-refresh gate. Settlement is independent of model/consensus success, so old results can refresh when prediction tasks fail. It still waits on the upstream refresh and is not a live minute-by-minute results service. First-time newly captured entries may read pending/not-yet-checked until the next settlement.

After deploying through the normal project process:

```sh
python -m engine.eval.forecast_history init
python -m engine.eval.forecast_history settle
python -m engine.eval.forecast_history report
```

`report` is read-only JSON containing the selection policy, population, capture period, status counts, selected probability, captured context, latest result evidence, observation timestamps and ordered outcome_history. A local [read-only HTTP adapter](forecast_api.md) was added on 2026-10-03 with SQL-side filters, fixture pagination and separately paginated correction history. No frontend integration or deployment has been performed. The frontend must expose unobserved results, capture time, event, probability, source outcome, all status categories and clearly bounded event/league/date cohorts. True stale-result warnings need a last-check/source-freshness record: settlement timestamps only mark changed observations, not every successful poll. Use “Latest captured forecast” while selection is provisional and “Last forecast before kickoff” afterward; always show capture time. These UI labels are requirements, not implemented frontend/Figma changes.

## Guarantees and limits

- Database triggers reject UPDATE, DELETE and TRUNCATE on all three ledger tables; application code only inserts. This prevents routine overwrite, **not** tampering by a privileged database owner who can disable triggers. Separate migration/admin and restricted runtime roles plus external backups remain deployment hardening work.
- Captured probability and available metadata are preserved. Capture-time Python/calibration-file hashes are stored; these do not prove which files a model loaded earlier in a separate task. Training-data cutoff, trained model version and exact calibration application remain unavailable unless supplied upstream. Do not claim leak-free evaluation or independently verified performance from this ledger alone.
- Current prediction scheduling waits for `extract_xg_task`, not the downstream `run_dbt` task. Latest warehouse features are consumed but same-run dbt completion is not enforced here. This pre-existing scheduling gap is documented, not fixed as part of the ledger.
- Starting deployment creates a prospective record from that point onward. Storage retention/backups and full source snapshots are not implemented; append-only output can grow over time.

## Verification

2026-10-03 latest-policy verification: all nine forecast-history tests passed using `/opt/anaconda3/bin/python -m unittest discover -s tests -p test_forecast_history.py -v`, including three tests on a fresh isolated PostgreSQL database. Coverage includes latest selection, unchanged older probabilities, retry non-promotion, strict kickoff eligibility, late-capture rejection, conservative no-reopening after kickoff, policy isolation, per-event selection, deterministic timestamp ties, repeated schema setup, correction history and mutation guards. Neighbouring suites also passed: 11 consensus, five daily-shortlist and 14 goal-bounds tests (39 total). No application database migration, live DAG run or deployment was performed.

Historical verification (2026-10-02, superseded first policy): seven tests passed, including real isolated PostgreSQL integration covering retry deduplication, first-probability selection, pending/hit/miss revisions, correction reversal, backdated logical-date rejection and UPDATE/DELETE/TRUNCATE protection. Existing regressions passed: five daily-shortlist, eleven consensus and fourteen goal-bounds tests. DAG syntax was parsed; no live Airflow task execution was claimed.

Integration tests require a fresh empty database whose name starts with `forecast_test`, supplied via `FORECAST_TEST_DSN`. Never point these tests at the application database.

## Engine flow

See [rendered data flow](engine-data-flow.svg), [editable Graphviz source](engine-data-flow.dot), and [Mermaid source](engine-data-flow.mmd). Solid arrows describe data/control movement; dashed notes identify limitations and pending work. The diagram is a logical engine pipeline, not a deployment/network topology.
