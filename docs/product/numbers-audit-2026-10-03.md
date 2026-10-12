# Numbers audit — 2026-10-03

**Reproduce:** inputs are `data/raw_preds.json` (owner's walk-forward dump, 3 Oct, 9 folds), `engine/eval/calibration_report.json` and `calibration_maps.json` (refit 4 Oct 01:30 UTC), and `engine/eval/consensus_slips_2026-10-02.json`. Scripts:

| Section | Script |
| --- | --- |
| §1 top band | `python -m engine.eval.top_band_check data/raw_preds.json` |
| §4 per-market vs shared | `python -m engine.eval.compare_dc_calibration data/raw_preds.json` |
| §4 league test | `python -m engine.eval.diagnostics.league_sensitivity` |
| §5 wins edge | `python -m engine.eval.diagnostics.win_edge` |
| §5 Mismatch by market | `python -m engine.eval.diagnostics.mismatch_markets` |
| §5 Mismatch win map | `python -m engine.eval.diagnostics.mismatch_win_lofo` |

§2–3 (goal estimates and pattern lift vs league) were one-off reads of the Oct 2 export; their figures are recorded below. Unit tests: `python -m pytest tests`. Latest saved run: `docs/product/test-runs/pytest-2026-10-04.txt` (184 passed, 10 skipped: all need an isolated test database).

Source: `engine/eval/consensus_slips_2026-10-02.json` (run 2 Oct), `calibration_maps.json` (generated 28 Sep), `calibration_report.json`. League baselines are current-season rates from the export's pattern records (early-season samples are small).

## 1. The 83.87% ceiling

**Cause (confirmed in code):** `calibration_maps.apply_points` uses `np.interp`, which clamps outside the map. The Dixon–Coles map's last point is `[raw 0.9113 → 0.8387]`: the top 10% of raw predictions, averaged into one point. Any raw ≥ 0.9113 becomes exactly 0.8387.

- 2 Oct run: 136 of 2,750 DC forecasts (5%) hit the ceiling. Raw values ranged 91.2%–98.9% (median 93.6%). By market: team to score 73, over 1.5 38, under 3.5 20, other 5.
- The pull-down itself is justified: historically raw ~91% predictions hit ~84%, so DC is overconfident at the top. The held-out check still shows the 85–90% band slightly over (predicted 86.3%, actual 81.8%, n=696).
- The ceiling value is unstable: the deployed map says 83.9%, the latest report's refit says 86.8%. It rests on one bin.
- Ceiling forecasts are not weak: median +5.2 points above league, 71% above league.
- **Unknown:** whether raw 97% actually hits more often than raw 92%. If it does, the clamp hides real information. Needs `top_band_check.py` (below).
- Goals 1–4 has a second flat zone: its map pools the top 4 bins at 81.07% (raw ≥ 0.797). That's 200 of 605 Goals 1–4 forecasts. Their final value is then decided by the league adjustment, which is effectively the league rate. That's why the Goals 1–4 shortlist tracks league averages.

**Top-band test result (2026-10-03, walk-forward 5 Jun–2 Oct, 30,239 DC predictions, owner's local run):**

| Market | Raw ≥ 0.9113 | Actual hit rate | Shown | Shown minus actual |
| --- | --- | --- | --- | --- |
| Team to score | 548 | 90.3% | 83.9% | −6.5 (understated) |
| Match Over 1.5 | 253 | 88.5% | 83.9% | −4.7 (understated) |
| Match Under 3.5 | 172 | 79.7% | 83.9% | +4.2 (overstated) |
| Team to win | 29 | 79.3% | 83.9% | +4.6 (small n) |
| Over/Under 2.5, Team 2+ | 21–30 each | 67–77% | 83.9% | +7 to +17 (small n) |
| All markets | 1,092 | 86.2% | 83.9% | −2.3 |

Pooled bands rise from 78.9% (raw 0.80–0.85) to ~86–87% (raw ≥ 0.91); above 0.94 it flattens. **Diagnosis:** the bigger problem isn't the clamp. It's one shared DC map for every market type. Markets that over-deliver at the top (team to score, Over 1.5) are dragged down; markets that under-deliver (Under 3.5, 2.5 lines, team 2+) are pushed up to the same 83.9%. **Proposed fix:** per-market-family DC maps (team to score, Over 1.5, Under 3.5; others pooled or uncalibrated-and-flagged) with a top bin built from ≥ 150 predictions. Goals 1–4 raw never exceeds 0.85 (its flat zone is the separate issue above).

Note: Poisson was skipped in this run (missing `statsmodels` locally); it doesn't affect DC results.

**Test to run locally (needs DB):**
```
python -m engine.eval.calibration_wf --no-save-maps --output /tmp/cal_report.json --dump-raw /tmp/raw_preds.json
python -m engine.eval.top_band_check /tmp/raw_preds.json
```
`--no-save-maps` keeps live maps unchanged. If hit rate rises across the 0.91–0.94 / 0.94–0.97 / 0.97+ bands, add finer top bins (or bin by raw value with a minimum n). If it is flat, keep the ceiling, show it as a band, and don't rank by it.

## 2. Goal estimates against league rates (1,128 forecasts with a matching baseline)

| Market | n | Median vs league | At/below league | ≥ +10 pts |
| --- | --- | --- | --- | --- |
| All | 1,128 | +2.6 | 40% | 23% |
| Match Over 1.5 | 360 | −1.5 | 59% | 4% |
| Team to score | 288 | +0.4 | 49% | 8% |
| Match Under 3.5 | 288 | +5.0 | 30% | 24% |
| Match Over 2.5 | 82 | +12.8 | 11% | 61% |
| Match Under 2.5 | 62 | +19.5 | 0% | 90% |
| Team win | 38 | +31.0 | 0% | 100% |
| Goals 1–4 shortlist | 40 | −0.7 | 28 of 40 | best +1.6 |

The highest-probability, highest-volume markets (Over 1.5, team to score, Goals 1–4) add little over the league. The model separates from the league in lower-probability markets (Over/Under 2.5, wins).

## 3. Pattern lift by event type (3,488 displayable records, n ≥ 10)

- Results-type events (win, loss, draw, unbeaten, winless, half-time win) are 9% of displayable patterns but 62% of the top 50 and 54% of the top 200 by lift. Owner's hypothesis confirmed: standout patterns are mostly results anomalies.
- Unbeaten and winless: median +19.7 points over league.
- Goals-type patterns dominate volume but mostly mirror the league. Under 4.5 alone is 1,008 records (29%), median +1.8 points.
- 33% of displayable patterns are within ±5 points of their league; 10% are ≥ +20 points.

## Implications (proposed, not agreed)

- League average stays: it's the context that tells users how a league plays. Pair it with an explicit relationship label ("in line with league" / "+N above league"), so league-driven confidence isn't mistaken for team-specific evidence.
- Rank Discover Goals and Patterns by lift with a minimum sample, not by raw estimate or posterior.
- The display threshold (≥ 75%) favours league-typical goals events. Consider a lift floor for Discover.

## 4. Per-market DC calibration — offline comparison (2026-10-03)

Script: `engine/eval/compare_dc_calibration.py` on the owner's walk-forward dump. Maps fitted on folds 1–4 (6,610 DC predictions), scored on folds 5–9 (19,074). Goals 1–4 excluded (own map in both). Lower is better.

| Market family | Brier | Log loss | Calibration error |
| --- | --- | --- | --- |
| Team to score (6,186) | 0.1754 → 0.1736 | 0.5327 → 0.5288 | 3.8 → 1.1 pts |
| Over 1.5 (3,281) | 0.1660 → 0.1633 | 0.5131 → 0.5066 | 4.9 → 2.0 pts |
| Under 3.5 (2,959) | 0.2142 → 0.2130 | 0.6208 → 0.6170 | 4.2 → 3.5 pts |
| Other, pooled (6,648) | 0.2435 → 0.2396 | 0.6822 → 0.6721 | 5.5 → 2.2 pts |
| All combined | 0.2035 → 0.2009 | 0.5951 → 0.5886 | 1.0 → 1.4 pts |

The shared map looks fine overall only because opposite errors cancel across markets. Per market, it's off by 4–5.5 points.

Shown at ≥ 80% (shown vs actual): with the shared map, Under 3.5 is +8.2 and other markets +14.5 (overstated), and team to score −2.6. With per-market maps, every family is within +0.1 to +1.7; "other" no longer reaches 80%.

New map tops: team to score 90.6%, Over 1.5 85.9%, Under 3.5 81.0%, other 70.1% (shared: 86.8%).

Predictions clearing the 65% display gate: 14,838 → 12,604 (overstated markets drop out).

Status: offline result only. Engine and live maps unchanged.

### League sensitivity test for Under 3.5 (owner hypothesis, 2026-10-03)

After per-market maps, league-level error (leagues with ≥ 40 eval predictions, grouped by api_league_id) compared with the error a perfectly calibrated model would show from chance alone (300 simulations):

| Family | League error | Chance alone | Excess |
| --- | --- | --- | --- |
| Team to score | 3.4 | 3.2 | +0.3 |
| Over 1.5 | 4.2 | 4.2 | 0.0 |
| Under 3.5 | 4.1 | 4.6 | −0.5 (+1.0 with folds 1–6 fit, only 11 leagues) |
| Other, pooled | 6.4 | 3.5 | +2.8 |

The existing league logit offsets (min n 30, prior 200) don't change Under 3.5 (4.0 → 4.0).

**Conclusion:** at current sample sizes (~40–90 eval predictions per league), Under 3.5's league differences can't be told apart from chance. Dixon–Coles already models each team's attack and defence within its league, so league scoring levels are largely in the raw probability. The real league effect is in the pooled "other" markets. Plan: ship per-market maps without league offsets for Under 3.5; split "other" into specific markets; re-test Under 3.5 league offsets once the forecast ledger has ≥ 200 predictions per league.


### Live refit check and bin-size fix (2026-10-03, 18:30 local refit)

The owner's refit wrote 14 map keys (Poisson included). Held-out check on all DC predictions (fit folds 1–4, eval 5–9), shown ≥ 70%, shown minus actual: team to score +0.8, Over 1.5 −0.6, Goals 1–4 +0.2, Under 3.5 +4.2, but thin markets fitted on ~450 predictions stayed overstated (BTTS +7.8, team win +7.8, Over 2.5 +4.8). Requiring ≥ 150 predictions per bin for per-market maps (`DC_MARKET_MIN_BIN_N`): Under 3.5 +3.5, team win +5.4, Over 2.5 +4.2; BTTS, Under 2.5 and team 2+ no longer reach 70%. Over 3.5 has too little early-fold data and is +21 on the pooled fallback; its own live map tops at ~55%, so it is rarely shown.

The report's consensus `predictions` with models == "dixon_coles" are biased (only fixtures where DC alone passed the gate, so it over-selects DC optimism). Don't use that subset to judge calibration.

Status: bin-size rule in code with tests (182 passed). Needs one more owner refit to take effect.

## 5. Wins: does DC add an edge? Mismatch with league offsets (2026-10-04)

Held-out folds 5–9 (maps and league offsets fitted on folds 1–4, as live). Baseline = league home-win / away-win rate from fit-fold results (4,828 scored fixtures). This baseline doesn't know who the favourite is, so beating it is necessary but not proof of an edge; the real test is bookmaker prices.

| Set | n | Actual | Model shown | Gap | Brier | Ranking (AUC) |
| --- | --- | --- | --- | --- | --- | --- |
| DC win, all priced | 1,221 | 59.7% | 58.4% | −1.3 | 0.2366 | 0.585 |
| DC win shown ≥ 65% | 345 | 69.6% | 72.1% | +2.5 | 0.2143 | 0.494 |
| Mismatch win raw ≥ 70%, live (map + league offset) | 232 | 69.8% | 77.3% | +7.5 | 0.2199 | 0.482 |
| Same 357 fixtures: DC | 357 | 69.7% | 63.6% | −6.1 | 0.2176 | 0.546 |
| Same 357 fixtures: Mismatch | 357 | 69.7% | 71.2% | +1.5 | 0.2126 | 0.554 |

- **No evidence DC adds anything on wins.** On the same fixtures Mismatch scores better (Brier 0.2126 vs 0.2176). Within Mismatch's probability bands, splitting by DC's estimate gives inconsistent differences (+14, −10, +8 points on 40–80 picks each), which is noise. Owner's scepticism supported.
- **Within the picks either model would show, neither ranks winners** (AUC ≈ 0.49): once a win pick is shown, a higher percentage doesn't mean it's more likely to win.
- **Mismatch league offsets barely matter:** only 20 leagues fitted, heavily shrunk; gap +7.6 → +7.5.
- **Mismatch win is overconfident in every month** (raw 78–80%, won ~70% in Jul–Sep). Cause: one Mismatch map pooled across Over 1.5, team to score, Over 2.5 and win. Leave-one-fold-out test of a win-specific Mismatch map (≥ 150 per bin): gap +6.9 → +0.3, Brier 0.2132 → 0.2082.
- Real edge on wins is untested: favourites winning ~70% typically carry odds around 1.35–1.45 (implied ~69–74%). Needs the SportyBet odds already being scraped into the DB.

Proposed: DC wins removed from user-facing lists (no hidden agreement role for wins either); Mismatch gets per-market maps (same pattern as DC); test win picks against stored bookmaker odds before claiming value.
