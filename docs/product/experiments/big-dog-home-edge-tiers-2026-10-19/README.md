# Big dog home edge, slight-favourite levels and league tiers (2026-10-18/19)

Status: **D137 and D138 implemented** (engine + match API, commits `3b5cef7`, `b1e1e16`; live from the next engine run). Card wording for tiers is **open** (owner to pick A or B). Frontend rendering not done. Internal document; not user-facing copy.

## 1. What changed

| Area | Before | Now |
|---|---|---|
| Big dog gap | Flat Elo gap (fav − opp) > 250 | **Venue-adjusted**: (home Elo + 65) − away Elo > 250 (D137) |
| League trust record and gap bands | Flat gap | Same adjusted gap |
| Slight favourite (match page) | One state, gap 100–250 | Two levels by adjusted gap: `slight_edge` (100–175), `favourite` (175–250). Label only, no "x in 10" (D138) |
| "Close" | Flat gap ≤ 100 | Unchanged (flat) |
| Team tiers | None | Top / Middle / Bottom per league, labels only, shown on every strength state except Big dog (card change pending in frontend) |

Fields: export `elo_gap_shortlist[].elo_gap` (adjusted), `elo_gap_flat`, `thresholds.big_dog_home_edge`, `fixture_analysis[].tiers`; match API `strength.level`, `strength.tiers`. See `data-contract.md` (2026-10-19 entry).

Bug fixed on the way: the match page found the favourite from the first rating row only; when that row was match-level ("Over 1.5") the strength card was hidden. It now checks all rows (12 hidden cards on the 18 Oct export).

## 2. Why: away favourites were overstated

Production Elo (flat K=30 track), top flight, walk-forward, trust-gated per rule (`engine/eval/diagnostics/strength_home_adv.py`, `ELO_COL=rating_before_flat`):

| Home edge | Big dogs | Won | Home favs | Away favs |
|---|---|---|---|---|
| 0 (old) | 2,026 | 76.3% | 81.1% | 71.2% |
| +50 | 2,275 | 77.4% | 78.2% | 75.3% |
| **+65** | **2,464** | **77.6%** | **77.6%** | **77.6%** |
| +70 | 2,530 | 77.0% | 76.7% | 78.3% |
| +80 | 2,720 | 76.4% | 75.7% | 80.1% |

The shown Big dog rate mixed home and away, so away Big dogs at gap 250–300 showed ~72% but won ~66%. Elo itself uses a 70-point home advantage when updating ratings; the selection rule ignored it.

Example: Bayern at Freiburg, flat gap 265 → adjusted 200 → "favourite", not Big dog. Arsenal v Everton, flat 225 → adjusted 290 → Big dog.

## 3. Rejected in the same review

| Idea | Result | Script |
|---|---|---|
| Gap thresholds 200–300 (flat) | 200–250 favourites won 59–64% | `strength_gap_sweep.py` |
| Gap 200 with home edge | Extra 1,557 matches won ~64% | `strength_home_adv.py` |
| Clustering options 2, 3, 5 (with and without home edge) | Matches only clusters add won 55–71%; overlap just repeats the gap | `strength_clusters_v2.py`, `strength_clusters_v3.py` |
| Rough attack/defence (rolling goals) | Weaker than Elo alone | `strength_attack_defence.py` |
| Dixon–Coles as replacement | Weaker than Elo alone | `strength_dc_check.py` |
| Dixon–Coles as last check (gap 200 + DC ≥ 65%) | Adds Big dogs that won 65%, drops ones that won 75% | `strength_home_dc.py` |
| DC without home advantage | Brings back Bayern only; drops home favourites for no reason | (current slate check) |

Kept in shadow: Dixon–Coles cross-check on Big dogs (DC ≥ 65% for the same favourite) is logged to `raw.big_dog_watch` and reported in the daily data email (D136). Decide after ≥ 100 confirmed Big dogs settle.

## 4. Slight favourite levels and tiers

Three seasons top flight, adjusted gap (`tier_slight_fav.py`):

| Adjusted gap | Matches | Favourite won | Drew |
|---|---|---|---|
| 100–175 | 8,158 | 51% | 26% |
| 175–250 | 4,430 | 62% | 23% |

At the same gap, tier matchups moved win rates by ≤ 5 points (e.g. 175–250: Top v Bottom 64%, Top v Middle 62%, Middle v Bottom 60%). Tiers repeat the gap, so they are context labels with no numbers of their own; the gap sets the level.

Tier matchup history across all gaps (`tier_context.py`, 31,865 top-flight matches): Top v Bottom at home 71% home wins; Bottom v Top 57% away wins; games involving a Top side average ~2.8–3.1 goals vs ~2.55 otherwise.

Tier method: k-means (k=3) on current flat Elo of the league's latest season, teams with ≥ 3 games, league needs ≥ 9 such teams (`tier_labels()`, `_load_team_tiers()` in `consensus_slips.py`). Groups follow real rating gaps, so sizes are uneven (Premier League Top = 2 teams on 18 Oct; Liverpool reads "Middle").

## 5. Card design (open: wording)

Figma board: `Match detail · Strength + tier context · prototype · 19 Oct` (file `dLgWAaafmzxQNcp2Rb9P7u`, node `1811:63160`). Browser preview with a wording toggle: artifact "Strength Card Tiers".

Rules agreed (owner, 19 Oct):
- Big dog card unchanged, **no tier line**.
- Favourite, Slight edge and Close get one tier line under the divider, plus a small note: venue ("Bayern are away. Home advantage is counted.") and "Tiers come from current team ratings in the <league>, not the table."
- No "x in 10" on strength cards.

| State | Example | Wording A | Wording B |
|---|---|---|---|
| Favourite | Bayern München at Freiburg | Top side v Middle side | In form v Mid-table |
| Slight edge | RB Leipzig v Elversberg | Top side v Middle side | In form v Mid-table |
| Close | Barcelona v Real Madrid | Top side v Top side | In form v In form |

Notes for the choice: B reads more naturally but "In form" suggests recent results while tiers come from long-run Elo; A is more accurate but can be read as table position (hence the "not the table" note). Option: B with "Strong side" for the top tier.

## 6. Open items
- Owner: pick wording A or B; keep or drop the venue note.
- Frontend: render `strength.level` and `strength.tiers` on the match page (frontend repo not in this workspace).
- Decide later whether "close" (≤ 100) should also use the adjusted gap (would flip favourites on tiny gaps).
- To-do: extend the calibration backtest to 24 months with every Dixon–Coles win chance and lambdas, then re-test the DC cross-check with the home edge.
