# Standout cutoff comparison — 6 October 2026

Decision update: D78 selects the inclusive 15-point cutoff; applied locally after this comparison, not deployed. The experiment below preserves the pre-decision comparison.

Pattern ranking now uses a 15-match adjustment and does not require the prediction display gate. No cutoff has been selected or installed. This is a selection-volume comparison, not an accuracy backtest.

## Comparison

567 unique saved fixtures, kickoff dates 6–15 October UTC; export generated 6 October 00:13 UTC. One best event per fixture, no section quotas or count ceiling. Counts below are after cross-section selection, not the total candidate events.

| Minimum adjusted gap | Fixtures selected | Share of 567 | Strength | Goals | Patterns |
|---|---:|---:|---:|---:|---:|
| 5 pp | 536 | 94.5% | 18 | 93 | 425 |
| 10 pp | 450 | 79.4% | 18 | 69 | 363 |
| 15 pp | 206 | 36.3% | 18 | 42 | 146 |

On 10 October (231 available fixtures), the cutoffs select **216 / 178 / 80** respectively. On 6 October (8 available fixtures), they select **8 / 6 / 1**. Missing calendar dates in the export are not treated as observed empty days. Per-date counts are in results.json.

## Meaning of the score

- Goals: (calibrated DC probability − matching saved league rate) × 0.76; probability must be at least 65%, valid league comparison required, win markets excluded.
- Patterns: (observed k/n − matching saved league base rate) × n/(n+15); n≥6 and valid comparison required. No display_eligible requirement. The final Bayesian prediction rating is not used to order historical patterns. Display actual k/n, not the adjusted ranking rate.
- Strength: D74-qualified league Big dog lower-bound win rate − league win rate at the favourite's venue, with all-games fallback. Existing export shortlist supplies candidates; this analysis does not add new strength fixtures.
- Thresholds are inclusive percentage-point differences after adjustment, not relative percentage improvements or raw observed differences. Rank using unrounded scores. Existing export winner scores round to four decimals; variant sensitivity below uses those export values.

## Example historical patterns near the boundaries

| Fixture | Pattern | Record | League rate | Adjusted gap |
|---|---|---:|---:|---:|
| Goias vs Athletic Club | Athletic Club · draw (away) | 7/15 | 26.8% | 9.95 pp |
| Nyköping vs Karlslund | Nyköping · halftime_win (home) | 8/12 | 32.9% | 14.99 pp |
| Gwangju FC vs Ulsan Hyundai FC | Gwangju Fc · winless (all) | 29/30 | 66.9% | 19.82 pp |

## What changed

At the 10-point cutoff: old 20-match adjustment plus prediction gate selected 245 fixtures; changing only to 15 selected 308; removing the gate selected 450. At 15 points, those counts are 88 / 113 / 206. The expanded historical candidate pool drives much of the increase.

## Interpretation

Of the tested values, 15 points is the most selective starting point for review, but still admits 36.3% of fixtures and 80 on the busiest day. Ten admits 79.4%, so it is broad for a section called Biggest standouts. No evidence here establishes an optimal cutoff or justifies claiming better future results. Section-specific thresholds remain possible; this comparison holds the threshold equal across methods for clarity. No quota is applied: the top 12 overall are Strength, even though Patterns supply most qualifying cards across the full list.

## Source and verification

Frozen source.json.gz copies the relevant fields of engine/eval/consensus_slips_2026-10-05.json. Strength-context.json contains a read-only local database snapshot retrieved 6 October: league records and matching win baselines. Saved forecasts/patterns were not refreshed. This mixes a saved forecast export with current strength context and is not point-in-time performance evidence. Existing primary and secondary pattern contexts remain eligible; the scope policy was not changed by this task. Pattern league baseline sample counts are not available in the saved rows.

compare.py replays the frozen inputs through discover_candidates. All fixture IDs unique; all pattern scores independently reconstructed from observed gaps and sample weights; goal scores checked against saved probabilities/baselines allowing rounding; selected totals reconcile and fall monotonically with higher cutoffs. Focused engine tests: 24 passed. Full ranked-fixtures.csv and candidates.json.gz retain inspectable selections.
