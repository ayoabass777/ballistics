# Models and events by section

Status: proposed 2026-10-04. It reflects engine code as of the 2026-10-04 run (`consensus_slips_2026-10-04.json`). Items marked **Open** need Ayo's decision. Once decided, log them in decisions.md.

Each section answers one question with one model. Users never see model names (D01).

| Section | Question it answers | Kind of number (onboarding step 1) | Model | Source field |
|---|---|---|---|---|
| Strength advantages | Is one side clearly stronger? | Ranking / label | Mismatch (Elo) | `elo_gap_shortlist`, `fixture_analysis[].mismatch` |
| Goals outlook | How many goals may there be? | Estimated chance | Dixon–Coles only (D07) | `fixture_analysis[].dixon_coles` |
| Consistent patterns | What has this team actually done this season? | Season record (k of n) | Bayesian streaks (record + base rate) | `fixture_analysis[].bayesian` |
| Forecast record | How often were our estimates right? | Hit / miss | Dixon–Coles goal forecasts | `latest_pre_kickoff_dc_goals_v1` (D55) |

Poisson stays internal. It feeds consensus slips and gates only, and is never shown.

## Strength advantages: Mismatch

- **Qualifies** when the raw Elo gap is above 250 (no home advantage) and the league upset rate is ≤ 27% over ≥ 50 matches. Superseded by D81: the upset-rate rule no longer applies. A league gets Big dogs when it is top flight (tier 1), has ≥ 30 past Big dog matches, and its win-rate lower bound is ≥ 60% to join (stays in until it falls below 57%). The list is capped at 40, with at most 7 per league (D43/D44).
- **Shows** the favourite and underdog, the gap and the league upset context.
- **Win chance** (leave-one-fold-out, favourites with raw ≥ 70%, n = 303; diagnostics/mismatch_win_lofo.py):
  - The live map shows 77.5% on average, but these favourites won 70.6%.
  - A win-specific map fixes the average (70.9% vs 70.6%), but neither map separates stronger favourites from weaker ones. Under the live map, the 80%+ band won only 68%.
  - **Proposed:** no per-match win %. Strength shows the label plus a group rate: "Favourites like this won about 7 in 10".
- **Not shown here:** Mismatch "favourite to score" and "favourite Over 1.5". Goal numbers come only from Dixon–Coles.

## Goals outlook: Dixon–Coles

| Event | User label | Calibration map (D54) |
|---|---|---|
| Match Over 1.5 / 2.5 / 3.5 | 2+ / 3+ / 4+ goals | `dixon_coles_over_1_5` / `_2_5` / `_3_5` |
| Match Under 2.5 / 3.5 | 0–2 / 0–3 goals | `dixon_coles_under_2_5` / `_3_5` |
| Goals 1–4 | 1 to 4 goals | `dixon_coles_bounds_1_4` (+ league offset) |
| BTTS Yes | Both teams score | `dixon_coles_btts` |
| Team Over 0.5 | Team scores | `dixon_coles_team_score` |
| Team Over 1.5 | Team scores 2+ | `dixon_coles_team_over_1_5` |

- **Shown** only when the calibrated value is ≥ 65%.
- **League comparison:** the same event's rate in this league this season. It uses the D57 labels, and copy names the league.
- **League rates (D60, implemented):** every Dixon–Coles row carries `league_rate`, `league_gap` and `league_comparison`.
  - Match events use the current-season league rate.
  - Team events use the team's venue: home-team scoring rate for the home side.
  - Goals 1–4 is counted from final scores.
  - Over 3.5 and team win have rates too, but they aren't user-facing.
- **Removed:** Dixon–Coles team win. It has no edge over Mismatch (Brier 0.2176 vs 0.2126) and doesn't rank winners (AUC ≈ 0.49), so it doesn't appear in user-facing lists.

## Consistent patterns: Bayesian streaks

- **Headline number** is the season record: "lost 14 of 15 away". The posterior is used only for the display gate (≥ 75%) and is never shown as a chance.
- **League comparison** uses `base_rate`: the same event in the same league and venue context.
- **Team events:** scores 1+ / 2+, concedes 1+ / 2+, clean sheet, win, loss, draw, unbeaten, winless, half-time win.
- **Match events also present:** Over 1.5–4.5, Under 1.5–4.5, BTTS, goalless.
- **Match totals stay in Patterns (Ayo, 2026-10-04).** A pattern describes one team's games this season ("Cuiaba's home games: 12 of 14 had under 3.5 goals"). Goals is a forecast for this fixture. They're different stories, and when they disagree, the user learns something.
- **Ranking by gap from the league keeps common events from flooding the list** (4 Oct run, 4,801 display-eligible patterns):
  - Under 4.5 has 1,355 eligible but a median gap of only +1.9 points. Only 3 make the top 200.
  - The top 200 are led by unbeaten (54) and winless (32), then score 1+, concede 1+ and Under 3.5 (22 each).

## Discover rule

- Three sections (D68). Within each, a fixture appears once on its strongest item: goals by `league_gap`, patterns by the shrunk gap (20-match prior), strength by the Big dog score.
- PROPOSAL (D71): a members-only Biggest standouts strip picks across sections using `fixture_analysis[].discover_pick`.
- Score = points above the league norm: goals use `league_gap` × 0.76; patterns shrink the record toward the league with a 20-match prior; strength uses the Big dog band win rate minus the league win rate (venue, else all games).
- Ties: strength > goals > patterns, then the favourite's side. No card if no score is above 0.
- Match details show all events.
