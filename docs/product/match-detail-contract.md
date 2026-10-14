# Match detail — data contract (v2)

2026-10-12 latest owner correction: extra probability-above-league gate rejected. Use the Bayes display-eligible pool and separate Results / Attack-defence categories as described in data-contract.md. Earlier Beta exceedance thresholds and no-pairs rules below are superseded history, not active selection requirements.

Status: **proposal for the frontend build** (2026-10-07; sources and IDs updated 2026-10-11). Design: Figma section "Match detail v2 · fixes 1–6" (668:57671) and "Match detail v2 · weak states" (698:57342); decisions D102–D104. Nothing here is deployed. Field names below are the real engine/database names unless marked **NEW**.

## 1. Sources

| Source | What it gives | Where |
|---|---|---|
| Engine run export | Strength splits, patterns, goal estimates, Big dog label and league record; warehouse `home_team_id`, `away_team_id`, `league_id` | `engine/eval/consensus_slips_<run_ds>.json` → `fixture_analysis[]`, `elo_gap_shortlist[]`, `elo_gap_group.big_dog_leagues[]` |
| League standings | Position, played, W/D/L, goal difference, points, form, group (conference / split round) | dbt `raw_mart.mart_league_standings`: warehouse ids, latest refresh per league season (D69), one row per group and team; `standings_updated_at` is the ETL refresh time |
| Head-to-head | Completed meetings between the two teams | dbt `raw.fact_head_to_head` (warehouse ids, played only): rows where `team_id` = one team and `opponent_id` = the other, either venue, `kickoff_utc` before the export cutoff, newest 5 |
| Crests | Team badge image | S3 `crests/teams/<logo_file_id>` (public read, immutable). `raw.fact_fixtures.home/away_logo_file_id` → API builds `home/away_crest_url` from `CREST_BASE_URL`; the frontend never builds provider URLs; initials if `null` |
| User state | Saved, followed leagues, venue preference | Account (not built) |

**ID rule** (`docs/contracts/ids.md`): everything this page receives uses warehouse ids (`fixture_id`, `team_id`, `league_id`). No API-Football ids reach the frontend, and no source here reads `raw.raw_fixtures` or `raw.raw_league_standings` directly.

One page = one `fixture_id`. Every value on the page must come from **one engine run** (`run_ds`) plus **one standings/H2H snapshot** (`standings_updated_at`). Both dates are shown on the page.

## 2. Payload (proposed `GET /api/match/{fixture_id}`)

```jsonc
{
  "fixture": { "fixture_id": 90863, "home": "Zlin", "away": "Slavia Praha",
               "home_team_id": "<team_id>", "away_team_id": "<team_id>",   // warehouse ids (in export since 2026-10-11)
               "home_crest_url": "https://<assets>/crests/teams/<uuid>", "away_crest_url": null,   // null = initials
               "league": "Czech Liga", "country": "Czech-Republic", "league_id": "<league_id>",
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
| Crest | `home_crest_url` / `away_crest_url` | Initials badge when no image. Never an API-Football id or URL. |
| Save | `user.saved` | Toggle; toast on save only. Saved list shows values frozen at save time (D93/D98). |
| Follow league | `user.league_followed` | Same component as elsewhere. |
| "Why it's here" | `entry.why` | Only when `entry.from` is discover/standouts. Built from `discover_pick` (`list`, `big_dog`, rank in that list). |

### 3.2 Context (opens first)
**Comparison table** — two columns, home team left.

| Row | Home/Away view | All venues view |
|---|---|---|
| League position | `rank`, `points` from the team's row where `is_team_current_group` | same |
| Goals scored / match | `strength.home_team.home.attack_rate` · `strength.away_team.away.attack_rate` | `…overall.attack_rate` |
| Goals conceded / match | `…home.defense_rate` · `…away.defense_rate` | `…overall.defense_rate` |
| Attack rank | `attack_rank` "of" `league_size` | overall |
| Defence rank | `defense_rank` "of" `league_size` | overall |

- Show totals always: "#13 of 17", "10th of 17" (`league_size` / number of rows in the team's group).
- Conferences or split rounds (`n_groups > 1`): position is within the team's group; name it ("4th of 15 · Eastern Conference"). Teams in different conferences: show each team's own group.
- Shade the better side per row (higher scored, lower conceded, lower rank number, higher position). Equal → no shade.
- Show rates to 2 decimals. Do not use `*_shrunk` or `*_pct` on this page.

**Head-to-head summary** (from `raw.fact_head_to_head`, newest 5):
- Tally: wins per team and draws, from `goals_for` / `goals_against` on the row where `team_id` is today's home team (`is_home` gives the venue of that meeting).
- Sheet (3+ meetings): goals per game (1 decimal), over 2.5 count, both-scored count, "At {home team}" split (meetings where today's home team was at home), list of meetings with date, home, score, away; winner bold.
- 1–2 meetings: tally + list only, no goal stats ("too few for goal stats").
- 0 meetings: "No previous meetings in our records" — not tappable; not the same as "never played".

**League table page**: rows of the team's current group from `mart_league_standings` (`rank`, `team_name`, `games_played`, `goal_diff`, `points`); both teams highlighted; `group_name` as the table title when `n_groups > 1`, with the other groups as tabs; "This match" card with position, points, W/D/L (`wins`/`draws`/`losses`), GD and points gap (gap only when both teams share a group). `description` (e.g. "Promotion - Champions League") can mark zone rows.

### 3.3 Strength
| State | Condition | Shown |
|---|---|---|
| Big dog | fixture in `elo_gap_shortlist[]` (league `status: in` in `big_dog_leagues[]`) | `favourite` · Big dog, `underdog` · Underdog; league record from `league_big_dog_won` / `league_big_dog_matches` (win %, drew/lost from `elo_gap_group.by_league[]` `drew`/`lost`, lower bound `win_rate_low`) |
| Label off for league | `mismatch[0].gap` > 250 but league `status: out` | "No Big dog label" + why + league record from `by_league[]` (`won`/`matches`, `win_rate_low` vs 60%) |
| Close ratings | gap ≤ 250 | "No Big dog label" — ratings are close |
| No rating | no `mismatch` rows | Card hidden |

Never show the rating gap number or a win percentage (D61).

### 3.4 Team patterns (`bayesian[]`)

Rules below are current as of 2026-10-14 (D122). Category intent and examples: [data-contract.md](data-contract.md#team-patterns-top-results-and-top-attackdefence--2026-10-12).

- Use rows with `primary: true` (the engine's context for that team: home team at home or all venues, away team away or all) and `n ≥ 6`.
- Record: `k / n` (large). Team rate: `round(100·k/n)%`. League rate: `round(100·base_rate)%`, from `league_rate_matches` current-season fixtures.
- The engine sets `league_comparison: too_few_matches` when the team has `n < 6` **or** the league rate has `league_rate_matches < 20` (same floor as goals). Either way: keep the record, hide the comparison line.
- Label from `league_comparison` (bands: |gap| < 5 pts typical, 5–15 above/below, ≥ 15 much; `LEAGUE_GAP_SOME` / `LEAGUE_GAP_WELL`):

| `league_comparison` | Patterns text (D104) | Goals text (D62) |
|---|---|---|
| `well_above` | Much more often than usual | Much more likely than usual |
| `above` | More often than usual | More likely than usual |
| `typical` | Typical for this league | Typical for this league |
| `below` | Less often than usual | Less likely than usual |
| `well_below` | Much less often than usual | Much less likely than usual |
| `too_few_matches` / `unavailable` | hide the row | hide the row |

- Never show a record with `k = 0` (e.g. "0 / 6") anywhere (owner, 2026-10-12).
- **Qualify** (every placement): primary context per view, `n ≥ 6`, `k > 0`, `display_eligible` (engine posterior ≥ 0.75). No hit-rate floor. Common records (e.g. Under 4.5, Over 1.5) may qualify.
- **Top Results** — events `win`, `loss`, `draw`, `winless`, `unbeaten`. At most one per team. Needs league-difference confidence ≥ 0.75: P(θ > league base) when the team is above the league, P(θ < league base) when below, from the record's Beta(`alpha`, `beta`). Pick by largest `|league_gap|`; confidence breaks ties. Winless/Unbeaten count as results; do not hardcode Won/Lost.
- **Top Attack/defence** — events `score_1goal`, `score_2goals`, `goalless`, `concede_1`, `concede_2`, `clean_sheet`. Same confidence gate and ranking. Prefer a corresponding pair across the two teams (scored N+ ↔ conceded N+, failed to score ↔ clean sheet) when both pass; if only one side passes, show it alone; never fill with an unqualified counterpart.
- One rung per ladder per team (scored 1+/2+, conceded 1+/2+, over x, under x): where rungs compete for the same slot, the larger gap wins.
- Omit a top category when nothing passes its gate.
- **Also** (max 3, no padding) and **See all (N)**: every remaining qualified record, including match totals (`Over_x_5`, `Under_x_5`) and `BTTS`, which never take a top slot. Ranked by hit rate `k/n`, then `n`, then `|league_gap|`. A record shown in a top category is not repeated.
- Both views (venue and all venues) are selected server-side and sent in the payload; the Home / Away | All venues switch is frontend only and opens on the engine's primary view.
- Neither gate value is shown to users (D115).
- Event labels (user-facing): `goalless` → "Failed to score" (never "Goalless", which reads as 0–0), `score_1goal` → "Scored 1+", `score_2goals` → "Scored 2+", `concede_1` → "Conceded 1+", `concede_2` → "Conceded 2+", `clean_sheet` → "Clean sheet", `BTTS` → "Both teams scored", `Over_x_5` / `Under_x_5` → "Over / Under x.5 goals", `win` → "Won", `loss` → "Lost", `draw` → "Drew", `winless` → "Winless", `unbeaten` → "Unbeaten".
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
| Group's ranks repeat (`ranks_repeat`) | Hide position row and table link for that group |
| Team not in standings | Position "—" with "Not in table yet" |
| No H2H | Non-tappable "No previous meetings in our records" |
| Pattern `n < 6` | See 3.4 |
| No league rate (< 20 league matches) | Hide the comparison line, keep the record |
| No DC rows | Hide Goals card |

## 6. Gaps to close before build
1. ~~W/D/L~~ — already stored (`wins`, `draws`, `losses`, `goals_for`, `goals_against`, `form`). No ETL change needed.
2. ~~Team IDs in the engine export~~ — done 2026-10-11: `fixture_analysis[]` carries warehouse `home_team_id`, `away_team_id`, `league_id` (engine reads dbt `fact_fixtures`).
3. ~~Pattern league sample size~~ — done 2026-10-11: `bayesian[]` rows carry `league_rate_matches`; under 20 → `too_few_matches`. Follow-up idea: blend last season into thin early-season league rates.
4. ~~Standings mart~~ — built 2026-10-11: `mart_league_standings`. The ETL now keeps each API table as a group (`group_name`, `group_order`, `description`) instead of overwriting; `standings_format` (single / conferences / split) is derived per league season from the standings themselves, no league config. Pending: first refresh with groups.
5. **Crests** — decided 2026-10-13: store our own copies in S3 (not a provider proxy). `etl/src/update_logos.py` (ported from football_stats_project) fetches from the API-Sports media CDN by provider id (no API quota) and writes `crests/<kind>/<uuid>` with Content-Type, no extension; only rows without a `logo_file_id` are fetched. The old project's disk files were not reused (their UUIDs only mapped to teams in the old database). `fact_fixtures` carries `home/away_logo_file_id`; `/context` returns `home/away_crest_url` (`engine/api/crests.py`). `CREST_BASE_URL` can point at a CDN later without a payload change. Apply: `scripts/apply_crests_s3.sh`. Pending: first run and coverage numbers.
6. ~~H2H stats~~ — computed on read (see 8) from the meeting list; no stored table. Fine for 5 rows; cutoff `kickoff_utc < export cutoff`.
7. **One snapshot rule** — standings/H2H are a current DB snapshot, not point-in-time with the forecast (D69); page must show both dates.
8. **API** — partly done 2026-10-12: `GET /api/match/{fixture_id}/context` (`engine/api/match_context.py`) serves position, full table groups and H2H (tally, stats at 3+ meetings) from the dbt models above; H2H cutoff is the fixture kickoff. CLI: `python -m engine.api.match_context <fixture_id>`. Strength, Patterns and Goals still come from the export only.

## 7. Worked example — Zlin v Slavia Praha (90863), run 2026-10-07
- Context (Home/Away): Zlin 0.40 / 1.80, #16 / #13 of 16; Slavia 2.50 / 0.25, #1 / #1. All venues: Zlin 0.89 / 2.11, #12 / #16; Slavia 2.78 / 0.67, #1 / #2. Position 16th 1 pt vs 1st 23 pts (snapshot 2026-10-05).
- H2H: Slavia 3, draws 2, Zlin 0; 2.6 goals/game; over 2.5 in 3 of 5; both scored 3 of 5; at Zlin: Slavia 2, draw 1.
- Strength: Slavia Praha Big dog; Czech Liga Big dogs won 144 of 196 (73%), drew 35, lost 17, lower bound 68%.
- Patterns: Zlin lost 8/9 (89% vs 39%, much more often) ↔ Slavia won 7/9 (78% vs 39%); Zlin conceded 1+ 9/9 ↔ Slavia scored 1+ 9/9 (100% vs 74%).
- Goals: Slavia over 0.5 90% (league 73% away, much more likely); over 1.5 84% (83%, typical); goals 1–4 75% (85%, less likely).
