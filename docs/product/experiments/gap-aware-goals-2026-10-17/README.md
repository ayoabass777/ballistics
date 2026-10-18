# Proposal: gap-aware Goals outlook calibration (2026-10-17)

Status: **Implemented 2026-10-17 as D127** (engine code; takes effect on the next engine run). Internal document; not user-facing copy.

Implementation: `engine/eval/goals_league_blend.py` (blend + walk-forward fit), weights in `engine/eval/goals_league_blend.json`, applied in `attach_league_comparisons()` (`engine/eval/consensus_slips.py`). Tests: `tests/test_goals_league_blend.py`. Rows keep `probability_pre_blend` and gain `league_blend: true` in the export; the match-detail API does not pass these fields through.

## Owner hypothesis
The league average is latent to the user. Goals outlook should not display it; the engine uses it (with team attack/defence) to predict and to identify unusual events.

## Where the league rate comes from today
- Goals outlook league rate = `dc_league_rate()` in `engine/eval/consensus_slips.py`, read from the same table as Team patterns base rates (`compute_league_base_rates()` in `engine/eval/bayesian_streaks.py`): current season only, ≥ 20 league fixtures.
- The forecast itself is fitted per league (league scoring level built in), plus a league adjustment at calibration for Goals 1–4.
- Mismatch: forecast uses multi-season, recency-weighted history; the league rate is a raw current-season count.

## Test 1 — does the gap add information beyond the chance?
`python -m engine.eval.diagnostics.goals_gap_check` — 16,355 Goals forecasts ≥ 65%, Jun–2 Oct, walk-forward calibration, league rate from prior games only.

Hit rate minus claimed chance (points), same chance split by gap:

| Chance | Gap < 5 | Gap 5–15 | Gap 15+ |
|---|---|---|---|
| 65–70% | +0.5 | −1.4 | −6.7 |
| 70–75% | +0.3 | −1.8 | −8.7 |
| 75–80% | −0.2 | −0.8 | −1.5 |
| 80–85% | −1.4 | −1.7 | −3.0 |
| 85%+ | −0.3 | −1.3 | −2.1 |

Logistic hit ~ chance + gap: gap coefficient −1.06 (90% CI −1.46 to −0.64).
Gap 15+: claimed +21.9 pts above league, realised +16.8. Unusual picks are real but overstated; the model under-uses the league level. Discover ranks by gap, so its top picks are the most overstated.

By market at 15+ gap (residual): Over 1.5 +2.5, Team 1+ −3.0, Goals 1–4 −3.9, Under 3.5 −7.5, Team 2+ −8.3, Over 2.5 −8.8.

## Test 2 — gap-aware calibration
`python -m engine.eval.diagnostics.goals_gap_calibration` — corrected chance = sigmoid(a + b·logit(chance) + c·logit(league rate)), fitted per market on earlier folds only, scored on folds 5–9 (14,269 forecasts).

- Overall Brier 0.1804 → 0.1805, log loss 0.5447 → 0.5448 (no average gain; error moves out of the tails).
- Residual by gap: <5 +0.3 → +1.1; 5–15 −0.8 → +1.8; 15+ −4.7 → −0.2.
- Discover-style top 10% (one pick per fixture, by gap): at 65% claimed 73.1 / hit 66.4 → claimed 74.3 / hit 73.9; at 75% claimed 82.0 / hit 79.5 → claimed 82.7 / hit 82.8. Realised gap above league stays ~+18–22.
- By market at 15+ gap (residual current → gap-aware): Team 2+ −7.4 → −1.2; Under 3.5 −8.4 → −2.5; Over 2.5 −12.0 → −6.0; Team 1+ −2.7 → −0.3; Over 1.5 +2.6 → +5.7 (over-corrected); Goals 1–4 −0.2 → +4.9 (over-corrected).

## Proposed change (pending owner approval)
1. Add a post-calibration league blend for Team 2+, Team 1+, Under 3.5, Over 2.5; leave Over 1.5 and Goals 1–4 on current calibration.
2. Refit blend weights alongside the calibration maps; Discover ranks on the corrected chance; displayed chance = corrected chance.
3. Over 2.5 needs a separate calibration review (still ~6 pts overconfident).

Caveats: backtest dump ends 2 Oct; 5 evaluation folds; small-market weights noisy; current-season league rate is noisy early in a season.

## Related open items from the same discussion (not decided unless stated)
- Owner direction: Goals outlook does not show the league average (Discover and match page); gap stays internal for selection/ranking. Team patterns keeps the league comparison.
- Pending: Discover Goals outlook bar 75% plus excluding "typical" picks (14 Oct run: 498 → ~303 cards; Team 2+ never reaches 70%+ because its map tops out ~69.5%).
- Revisit: Team 2+ top calibration bin split once raw ≥ 85% exceeds 150 games (88 now; hits ~77% vs 69.5% shown; 75–85% raw band hits ~62%).
- Figma review: Matches market filter (Over 1.5 · 75%+, percentages, ties broken by gap) on board `1388:63299`.


## Threshold follow-up — 2026-10-17

The owner approved the previously pending Discover Goals section 75% floor and exclusion of typical picks. Implemented and verified locally; see implementation-status.md. Historical experiment notes above remain unchanged. This threshold delivery does not re-verify the separately reported calibration implementation.
