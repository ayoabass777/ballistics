# Match detail — data contract (v2)

Status: **proposal for the frontend build** (2026-10-07). Design: Figma section "Match detail v2 · fixes 1–6" (668:57671) and "Match detail v2 · weak states" (698:57342); decisions D102–D104. Nothing here is deployed. Field names below are the real engine/database names unless marked **NEW**.

## 1. Sources

| Source | What it gives | Where |
|---|---|---|
| Engine run export | Strength splits, patterns, goal estimates, Big dog label and league record | `engine/eval/consensus_slips_<run_ds>.json` → `fixture_analysis[]`, `elo_gap_shortlist[]`, `elo_gap_group.big_dog_leagues[]` |
| League standings | Position, played, goal difference, points | `raw.raw_league_standings` (rule: D69 `display_standings`, latest refresh cohort) |
| Head-to-head | Completed meetings between the two teams | `raw.raw_fixtures` (D69: both team IDs, either venue, FT only, kickoff before the export cutoff, newest 5) |
| Crests | Team badge image | `https://media.api-sports.io/football/teams/{api_team_id}.png` (D76); initials if missing |
| User state | Saved, followed leagues, venue preference | Account (not built) |

One page = one `fixture_id`. Every value on the page must come from **one engine run** (`run_ds`) plus **one standings/H2H snapshot** (`standings_updated_at`). Both dates are shown on the page.

## 2. Payload (proposed `GET /api/match/{fixture_id}`)

```jsonc
{
  "fixture": { "fixture_id": 90863, "home": "Zlin", "away": "Slavia Praha",
               "home_team_id": "<id>", "away_team_id": 560,   // NEW in export (ids exist in DB)
               "league": "Czech Liga", "country": "Czech-Republic", "api_league_id": 345,
               "kickoff_utc": "2026-10-09T16:00:00Z", "run_ds": "2026-10-07",
               "standings_updated_at": "2026-10-05T00:09:10Z" },
  "entry":    { "from": "discover|standouts|matches|saved", "section": "strength|goals|patterns|null",
                "highlight": { "section": "patterns", "team": "Zlin", "event": "loss" },
                "why": "Slavia Praha · Big dog, #1 on today's strength shortlist" },   // only when from Discover/standouts
  "context":  { ... }, "strength": { ... }, "patterns": { ... }, "goals": { ... },
  "user":     { "saved": false, "league_followed": true, "venue_view": "home_away|all" }
}
```

## 3. Sections

### 3.1 Header and sticky bar
| UI | Field | Rule |
|---|---|---|
| Teams, league, kickoff | `home`, `away`, `league`, `country`, `kickoff` | Kickoff rendered in the user's timezone (browser). |
| Crest | `api_team_id` | Initials badge when no image. |
| Save | `user.saved` | Toggle; toast on save only. Saved list shows values frozen at save time (D93/D98). |
| Follow league | `user.league_followed` | Same component as elsewhere. |
| "Why it's here" | `entry.why` | Only when `entry.from` is discover/standouts. Built from `discover_pick` (`list`, `big_dog`, rank in that list). |

### 3.2 Context (opens first)
**Comparison table** — two columns, home team left.

| Row | Home/Away view | All venues view |
|---|---|---|
| League position | standings `rank`, `points` (overall table in both views) | same |
| Goals scored / match | `strength.home_team.home.attack_rate` · `strength.away_team.away.attack_rate` | `…overall.attack_rate` |
| Goals conceded / match | `…home.defense_rate` · `…away.defense_rate` | `…overall.defense_rate` |
| Attack rank | `attack_rank` "of" `league_size` | overall |
| Defence rank | `defense_rank` "of" `league_size` | overall |

- Show totals always: "#13 of 17", "10th of 17" (`league_size` / number of standings rows).
- Shade the better side per row (higher scored, lower conceded, lower rank number, higher position). Equal → no shade.
- Show rates to 2 decimals. Do not use `*_shrunk` or `*_pct` on this page.

**Head-to-head summary** (from `raw.raw_fixtures`, newest 5):
- Tally: wins per team and draws, from `home_team_fulltime_goal` / `away_team_fulltime_goal`.
- Sheet (3+ meetings): goals per game (1 decimal), over 2.5 count, both-scored count, "At {home team}" split (meetings where today's home team was at home), list of meetings with date, home, score, away; winner bold.
- 1–2 meetings: tally + list only, no goal stats ("too few for goal stats").
- 0 meetings: "No previous meetings in our records" — not tappable; not the same as "never played".

**League table page**: all `display_standings` rows (`rank`, `team_name`, `games_played`, `goal_diff`, `points`); both teams highlighted; "This match" card with position, points, W/D/L (`wins`/`draws`/`losses`), GD and points gap.

### 3.3 Strength
| State | Condition | Shown |
|---|---|---|
| Big dog | fixture in `elo_gap_shortlist[]` (league `status: in` in `big_dog_leagues[]`) | `favourite` · Big dog, `underdog` · Underdog; league record from `league_big_dog_won` / `league_big_dog_matches` (win %, drew/lost from `elo_gap_group.by_league[]` `drew`/`lost`, lower bound `win_rate_low`) |
| Label off for league | `mismatch[0].gap` > 250 but league `status: out` | "No Big dog label" + why + league record from `by_league[]` (`won`/`matches`, `win_rate_low` vs 60%) |
| Close ratings | gap ≤ 250 | "No Big dog label" — ratings are close |
| No rating | no `mismatch` rows | Card hidden |

Never show the rating gap number or a win percentage (D61).

### 3.4 Team patterns (`bayesian[]`)
- Use rows with `primary: true` (the engine's context for that team: home team at home or all venues, away team away or all) and `n ≥ 6`.
- Record: `k / n` (large). Team rate: `round(100·k/n)%`. League rate: `round(100·base_rate)%`.
- Label from `league_comparison` (bands: |gap| < 5 pts typical, 5–15 above/below, ≥ 15 much; `LEAGUE_GAP_SOME` / `LEAGUE_GAP_WELL`):

| `league_comparison` | Patterns text (D104) | Goals text (D62) |
|---|---|---|
| `well_above` | Much more often than usual | Much more likely than usual |
| `above` | More often than usual | More likely than usual |
| `typical` | Typical for this league | Typical for this league |
| `below` | Less often than usual | Less likely than usual |
| `well_below` | Much less often than usual | Much less likely than usual |
| `too_few_matches` / `unavailable` | hide the row | hide the row |

- Order: largest `|league_gap|` first (gap value itself not shown).
- Pairing: put mirror events side by side — home `loss` ↔ away `win` (and vice versa), home `concede_N` ↔ away `score_N`, home `score_N` ↔ away `concede_N`. Remaining rows go under "Also" (max 3). Drop the weaker of complements (`win`/`winless`, `loss`/`unbeaten`, `Over_x`/`Under_x`) when both appear (D73). D106 owner clarification: loss/winless and win/unbeaten are distinct, non-complementary events and may both remain; do not discard them as duplicate evidence.
- Current run: `streak`, shown only when > 0.
- Exclude `halftime_win` (D80).
- Empty states: both teams `n < 6` → one card "Too early for patterns" with "{n} of 6 matches played"; one team short → show the other, "Not enough matches · {n} of 6".

### 3.5 Goals outlook (`dixon_coles[]`)
- Up to 3 rows, highest `probability`, excluding win markets (`… Win`) (D80).
- Value `round(100·probability)%`; label per table above + "league {round(100·league_rate)}%" + "(home/away)" when `league_rate_context` ≠ all.
- All rows `typical` → banner "Nothing stands out" and muted values.

## 4. Entry behaviour (D104)
- From a Discover / standouts card: open scrolled to `entry.section`, its chip active, the card's item highlighted (`entry.highlight`).
- From Matches / Saved: open at the top (Context), no highlight, no "Why it's here".
- Back returns to the previous screen.

## 5. Missing data — never shown as zero
| Missing | Behaviour |
|---|---|
| Standings groups unresolved (`standings_group_unresolved`) | Hide position row and table link; explain conferences |
| Team not in standings | Position "—" with "Not in table yet" |
| No H2H | Non-tappable "No previous meetings in our records" |
| Pattern `n < 6` | See 3.4 |
| No league rate (< 20 league matches) | Hide the comparison line, keep the record |
| No DC rows | Hide Goals card |

## 6. Gaps to close before build
1. ~~W/D/L~~ — already stored: `raw.raw_league_standings` has `wins`, `draws`, `losses`, `goals_for`, `goals_against`, `form` (etl/src/update_standings.py). Only the 2026-10-05 mock export omitted them. No ETL change needed.
2. **Team IDs in the engine export** — standings and H2H currently join by team name; add `home_team_id` / `away_team_id` to `fixture_analysis[]`.
3. **Pattern league sample size** — `bayesian[]` has `base_rate` but no league match count (DC rows have `league_rate_matches`). Needed to apply the 20-match rule to patterns.
4. **H2H stats** — computed on read from the meeting list; no stored table. Fine for 5 rows; document the cutoff (`kickoff < export cutoff`).
5. **One snapshot rule** — standings/H2H are a current DB snapshot, not point-in-time with the forecast (D69); page must show both dates.
6. **API** — no match endpoint exists; `engine/api/` only serves the Forecast record.

## 7. Worked example — Zlin v Slavia Praha (90863), run 2026-10-07
- Context (Home/Away): Zlin 0.40 / 1.80, #16 / #13 of 16; Slavia 2.50 / 0.25, #1 / #1. All venues: Zlin 0.89 / 2.11, #12 / #16; Slavia 2.78 / 0.67, #1 / #2. Position 16th 1 pt vs 1st 23 pts (snapshot 2026-10-05).
- H2H: Slavia 3, draws 2, Zlin 0; 2.6 goals/game; over 2.5 in 3 of 5; both scored 3 of 5; at Zlin: Slavia 2, draw 1.
- Strength: Slavia Praha Big dog; Czech Liga Big dogs won 144 of 196 (73%), drew 35, lost 17, lower bound 68%.
- Patterns: Zlin lost 8/9 (89% vs 39%, much more often) ↔ Slavia won 7/9 (78% vs 39%); Zlin conceded 1+ 9/9 ↔ Slavia scored 1+ 9/9 (100% vs 74%).
- Goals: Slavia over 0.5 90% (league 73% away, much more likely); over 1.5 84% (83%, typical); goals 1–4 75% (85%, less likely).
