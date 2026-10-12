# Bayesian prior comparison — 2026-10-05

## Result

Keep the fixed prior weight of 15 for now. It was selected on 2024 validation data and beat the half-games prior on the 2025–2026 holdout. This is a raw-smoothing experiment, not a validation of the complete calibrated and boosted engine. No production parameter was changed.

## Holdout results

Lower Brier score and log loss are better. Rows are correlated team/event observations, not independent matches.

| Prior | Brier | Log loss |
|---|---:|---:|
| fixed_3 | 0.225777 | 0.648644 |
| fixed_5 | 0.222874 | 0.639757 |
| fixed_10 | 0.219763 | 0.631234 |
| fixed_15 | 0.218663 | 0.628401 |
| half_n | 0.224506 | 0.644985 |
| league_only | 0.219250 | 0.629402 |
| team_only | 0.236972 | 0.877665 |

Half-games increased Brier error by 0.005843 (2.67%) relative to 15. A paired league-season cluster bootstrap (2,000 resamples; 154 clusters) gives a 95% interval of +0.005507 to +0.006205. This interval captures between-cluster variation under that resampling assumption; it does not account for all recurring-team or cross-season dependence.

Fixed 15 also beat half-games in all 62 individual league aggregates. Equal-weight league Brier means: 0.219029 versus 0.224995. Removing exact complementary event pairs preserved the result (0.218500 versus 0.224304). The remaining events are still correlated.

| Slice | Fixed 15 Brier | Half-games Brier |
|---|---:|---:|
| Scored 2+ goals | 0.232575 | 0.237829 |
| Unbeaten | 0.226868 | 0.231333 |
| 3–5 prior matches | 0.221618 | 0.239382 |
| 6–9 | 0.219267 | 0.226347 |
| 10–19 | 0.217510 | 0.220199 |
| 20+ | 0.222124 | 0.222772 |

Ten-bin expected calibration error on the holdout: fixed 15 = 0.0136; half-games = 0.0385. This is bin-dependent and pooled across events. The league-only reference was better calibrated by this summary (0.0086), but had slightly worse Brier/log loss than fixed 15. The Brier gain of fixed 15 over the league-only reference is small (about 0.27%).

## Design and evidence

- Source: read-only raw.raw_fixtures, FT with both full-time scores, 2020-01-01 through 2026-10-04 UTC. 113,950 unique fixtures across 62 leagues. Current database results, not archived point-in-time inputs.
- Warm-up history precedes 2024. Candidate selection uses 2024; the subsequent holdout is 2025-01-01 through 2026-10-04. Fixed candidates 3, 5, 10, 15 and n/2 were specified before scoring. Select by validation Brier, with log loss as tie-breaker. League-only and team-only are reference models.
- Validation: 16,448 fixtures, 32,755 eligible team contexts. Holdout: 28,383 fixtures, 56,517 eligible team contexts, 734,721 event predictions across 13 events. One or both teams may qualify in a fixture.
- Team counts and league base rates are rebuilt per league and season using strictly earlier UTC dates; no same-day result is allowed into predictions. Both venue and overall histories are accumulated. Use venue when at least six previous venue matches exist, otherwise overall; require at least three team observations and 20 league/team observations in the selected context.
- League baseline includes the target team’s prior records, as in the existing engine. For overall match-total events, each fixture contributes both team perspectives; exact duplication does not change the mean but prediction rows are dependent.
- Events match the engine BETTING_EVENTS set and dbt full-time event definitions. This study evaluates primary contexts only and does not filter predictions using a 75% rating gate.
- Formula: (k + s × league_rate)/(n + s). Half-games means s=n/2. No fitted calibration, current calibration maps, streak boosts, odds or profit measures are used. Log loss clips probabilities at 1e-6 and 1−1e-6, materially relevant to the unsmoothed team-only baseline.
- Independent verifier reconstructs 130 randomly sampled predictions using dataframe filters, checks counts, context, outcomes and all prior formulas, plus full-output uniqueness/event completeness. See verification.json.

## Interpretation and limits

Reducing shrinkage just to make Barcelona’s 7/7 event clear the display gate is not supported by this experiment. Half-games gives sparse team records too much influence in this dataset. Retain 15 while separating historical pattern visibility from forecast-rating eligibility.

Fifteen is the largest tested fixed candidate, so this does not prove it is optimal. Larger or event-specific weights require a newly designed validation study; this holdout has now been inspected and cannot be reused as untouched confirmation. Calibration and boosting would need training-only refitting before any complete-engine comparison or production change.

Historical data may have been corrected after the fixtures; historical ingestion availability and result-publication times are unavailable. Day batching is conservative but is not a complete simulation of known-at-the-time availability. Coverage is limited to loaded league seasons, excludes missing-score/non-FT records and early contexts below the sample gates. Team strength, opponents, promotion effects and changing league coverage are not separately modeled.

## Reproduction

Run from repository root with Python providing numpy, pandas and psycopg2:

```sh
python -m engine.eval.diagnostics.bayesian_prior_check
python -m engine.eval.diagnostics.verify_bayesian_prior_check
```

Credentials are read locally and are not stored in outputs. The first command queries the database read-only. Source and prediction CSVs, query, source hash, model summaries, slices and calibration bins are saved alongside this report. Re-running against a changed database can change the source snapshot.
