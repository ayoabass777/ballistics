# Match detail — data contract (v2)

2026-10-22: Strength, Patterns and Goals now come from the fixture's most recent complete run, so a match keeps its last pre-kickoff forecast after kickoff (D152). New field `fixture.forecast_from_latest_run` (false for such a kept forecast; the frontend should label it "made before kickoff"). `engine_stale` is not raised for it once kickoff has passed. Migration `0002` adds the index this lookup uses.

2026-10-12 latest owner correction: extra probability-above-league gate rejected. Use the Bayes display-eligible pool and separate Results / Attack-defence categories as described in data-contract.md. Earlier Beta exceedance thresholds and no-pairs rules below are superseded history, not active selection requirements.

Status: **proposal for the frontend build** (2026-10-07; sources and IDs updated 2026-10-11). Design: Figma section "Match detail v2 · fixes 1–6" (668:57671) and "Match detail v2 · weak states" (698:57342); decisions D102–D104. Nothing here is deployed. Field names below are the real engine/database names unless marked **NEW**.

## 1. Sources

| Source | What it gives | Where |
|---|---|---|
| Engine run export | Strength splits, patterns, goal estimates, Big dog label and league record; warehouse `home_team_id`, `away_team_id`, `league_id` | `engine/eval/consensus_slips_<run_ds>.json` → `fixture_analysis[]`, `elo_gap_shortlist[]`, `elo_gap_group.big_dog_leagues[]` |
| League standings | Position, played, W/D/L, goal difference, points, form, group (conference / split round) | dbt `raw_mart.mart_league_standings`: warehouse ids, latest refresh per league season (D69), one row per group and team; `standings_updated_at` is the ETL refresh time. When the API sent rows we couldn't load (D147), `table.incomplete` is true and the affected team keeps its last good row with `stale: true` (also `position.stale`): show "Table may be out of date" Every payload also carries `data_quality: {ok, issues:[{code, section, severity, since, message, detail}]}` (D148): codes `standings_incomplete`, `fixtures_incomplete`, `standings_stale`, `engine_stale`; `section` = context / fixture / engine. Render one banner per section from it |
| Head-to-head | Completed meetings between the two teams | dbt `raw.fact_head_to_head` (warehouse ids, played only): rows where `team_id` = one team and `opponent_id` = the other, either venue, `kickoff_utc` before the export cutoff, newest 5 |
| Crests | Team badge image | S3 `crests/teams/<logo_file_id>` (public read, immutable). `raw.fact_fixtures.home/away_logo_file_id` → API builds `home/away_crest_url` from `CREST_BASE_URL`; the frontend never builds provider URLs; initials if `null` |
| User state | Saved, followed leagues, venue preference | Account (not built) |

**ID rule** (`docs/contracts/ids.md`): everything this page receives uses warehouse ids (`fixture_id`, `team_id`, `league_id`). No API-Football ids reach the frontend, and no source here reads `raw.raw_fixtures` or `raw.raw_league_standings` directly.

One page = one `fixture_id`. Every value on the page must come from **one engine run** (`run_ds`) plus **one standings/H2H snapshot** (`standings_updated_at`). Both dates are shown on the page.

## 2. Payload — `GET /api/match/{fixture_id}` (built 2026-10-14, `engine/api/match_detail.py`)

```jsonc
{
  "fixture":  { "fixture_id": 90876, "home": "Pardubice", "away": "Zlin",
                "home_team_id": 26815, "away_team_id": 26802, "league_id": 233,     // warehouse ids
                "home_crest_url": "https://<assets>/crests/teams/<uuid>", "away_crest_url": null,  // null = initials
                "league": "Czech Liga", "country": "Czech-Republic", "kickoff_utc": "2026-10-17T13:00:00+00:00",
                "run_ds": "2026-10-13",                       // engine run behind Strength/Patterns/Goals; null if not in run
                "run_finished_at": "2026-10-14T03:07:00Z",    // when that run was written (run_ds is the Airflow interval start)
                "forecast_from_latest_run": true,             // false: an earlier run's forecast (after kickoff: the last pre-kickoff one, D152); null if no forecast
                "standings_updated_at": "2026-10-12T00:00:00Z" },   // DB snapshot behind Context (D69)
  "context":  { "position": {...}, "table": {...}, "h2h": {...},      // as /context (§3.2)
                "form": { "home_team": {"overall": {...}, "home": {...}, "away": {...}}, "away_team": {...} } },
                // form rows: attack_rate, defense_rate, attack_rank, defense_rank, league_size only
  "strength": { "state": "big_dog", "favourite": "AEK Athens FC", "underdog": "Atromitos" },  // §3.3
  "patterns": { "default_view": "all", "venue": null, "all": { "top_results": [], "top_attack_defence": [], "also": [], "see_all": [] } },  // §3.4
  "goals":    { "rows": [ {"market": "Match Goals 1-4", "probability": 0.8, "league_rate": 0.85,
                           "league_rate_context": "all", "league_comparison": "typical"} ],
                "nothing_stands_out": false },                                         // §3.5; null = hide card
  "discover": { "list": "patterns", "team": "Zlin", "event": "loss" }   // entry highlight / "Why it's here"; null if not on Discover
}
```

- Context comes from the live database snapshot. Strength, Patterns, Goals, `context.form` and `discover` come from `raw.match_analysis` (page-ready jsonb per `run_ds`, `fixture_id`), read from the fixture's most recent run marked `complete` in `raw.engine_runs` (`engine/eval/match_store.py`; before 2026-10-22, only the latest run, which left started and finished matches empty because runs cover not-started fixtures). After kickoff that is the last pre-kickoff forecast (D152): `fixture.forecast_from_latest_run` is false, and the page should label the sections as made before kickoff on `run_ds`. Its age then does not raise `engine_stale`. The consensus task writes both in one transaction before publishing the JSON file, so the page shows a whole run or the previous one, never a partial. `run_ds` is Airflow `{{ ds }}` (an @daily run firing on 14 Oct carries 2026-10-13), hence `run_finished_at`. Backfill old runs: `scripts/apply_match_store.sh`. The JSON export stays as a debug copy; the API no longer reads it.
- A fixture not in the latest complete run returns Context with the engine sections null (no fallback to an older run). Before the tables exist, the engine sections are null rather than an error.
- Not in the payload (D61, D115): rating gaps, posteriors, calibration values, shrunk/percentile rates, Big dog counts, Discover scores. `entry` (where the user came from) and `user` (saved, followed, venue preference) are frontend/account state, not served here.

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
| League position / venue rank | Calculated home-only rank and points for the home team, away-only for the away team, using the provider's `home` and `away` standings records when available; a clearly labelled completed-fixture fallback applies in single-table leagues while split records are absent. Missing for ambiguous groups or no venue games. | Official `rank`, `points` from the team's row where `is_team_current_group`; if missing/rankless/stale in a single-table league, a clearly labelled fixture-derived fallback is used |
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
| `state` | Condition | Shown |
|---|---|---|
| `big_dog` | fixture in `elo_gap_shortlist[]` | `favourite` · Big dog, `underdog` · Underdog; "Big dogs usually win in this league" (no counts, owner 2026-10-12) |
| `no_label` | rating gap > 250, league not in Big dog list | "No Big dog label" + `reason`: `top_flight_only`, `not_enough_matches` or `unpredictable` |
| `close` | gap ≤ 250, or no rating rows (the engine writes them only for gaps ≥ 100) | "No Big dog label" — ratings are close |
| null | no team data | Card hidden |

Never show the rating gap number, a win percentage or league counts (D61). 13 Oct run: close 561, big_dog 26, no_label 5.

### 3.4 Team patterns (`bayesian[]`)

Rules below are current as of 2026-10-14 (D122). Category intent and examples: [data-contract.md](data-contract.md#team-patterns-top-results-and-top-attackdefence--2026-10-12).

- Use rows with `primary: true` (the engine's context for that team: home team at home or all venues, away team away or all) and `n ≥ 6`.
- Record: `k / n` (large). Team rate: `round(100·k/n)%`. League rate: `round(100·base_rate)%`, from `league_rate_matches` current-season fixtures.
- The engine sets `league_comparison: too_few_matches` when the team has `n < 6` **or** the league rate has `league_rate_matches < 20` (same floor as goals). Either way: keep the record, hide the comparison line.
- Label from `league_comparison`. **Team patterns (D126): evidence decides, not gap size alone.** More/less often = league-difference confidence ≥ 0.75 in that direction (Beta posterior vs league base); "much" = also |gap| ≥ 15 pts. Typical = |gap| < 5 pts **and** at least 12 games. Anything else = `inconclusive`: the record is shown without a comparison line (a big gap on few games is not called typical). Games needed to reach 0.75 at a steady rate: about 5–7 for a 20+ pt gap, 8–16 for 10–15 pts, 25–50 for 5 pts. Goals outlook keeps the gap bands (|gap| < 5 pts typical, 5–15 above/below, ≥ 15 much; `LEAGUE_GAP_SOME` / `LEAGUE_GAP_WELL`):

| `league_comparison` | Patterns text (D104) | Goals text (D62) |
|---|---|---|
| `well_above` | Much more often than usual | Much more likely than usual |
| `above` | More often than usual | More likely than usual |
| `typical` | Typical for this league | Typical for this league |
| `below` | Less often than usual | Less likely than usual |
| `well_below` | Much less often than usual | Much less likely than usual |
| `inconclusive` (patterns only, D126) | show the record, no comparison line | — |
| `too_few_matches` / `unavailable` | hide the row | hide the row |

- Never show a record with `k = 0` (e.g. "0 / 6") anywhere (owner, 2026-10-12).
- **Qualify** (D123, replaces the D122 qualify rule). Always: primary context per view, `n ≥ 6`, `k > 0`. Then by event family — **Results / Attack / Defence**: above the league average (`league_gap > 0`, comparison available) and league-difference confidence ≥ 0.75 (Beta posterior vs league base). The engine score is not used: it is calibrated on betting events only and floors rare events (Lost 9/10 and Drew 1/10 both score 0.62). Below-league team records are not shown (each mirrors an above-league one: Won 2/10 ↔ Winless 8/10). **Match goals** (Over/Under, BTTS): `display_eligible` (calibrated engine score ≥ 0.75), so common lines like Under 4.5 stay available for Also; may be below or typical vs league.
- **Top Results** — events `win`, `loss`, `draw`, `winless`, `unbeaten`, `halftime_win`. At most one per team, from qualified records. Pick by largest `|league_gap|`; confidence breaks ties. Winless/Unbeaten and half-time leads count as results; do not hardcode Won/Lost.
- **Top Attack/defence** — events `score_1goal`, `score_2goals`, `goalless`, `concede_1`, `concede_2`, `clean_sheet`. Same confidence gate and ranking. Prefer a corresponding pair across the two teams (scored N+ ↔ conceded N+, failed to score ↔ clean sheet) when both pass; if only one side passes, show it alone; never fill with an unqualified counterpart.
- One rung per ladder per team (scored 1+/2+, conceded 1+/2+, over x, under x). Top slots are picked first (so a corresponding pair survives a higher rung); a ladder used by a top slot is not repeated below; otherwise the larger gap wins.
- Omit a top category when nothing passes its gate.
- **Also** (max 3, no padding) and **See all (N)**: every remaining qualified record, including match totals (`Over_x_5`, `Under_x_5`) and `BTTS`, which never take a top slot. Ranked by hit rate `k/n`, then `n`, then `|league_gap|`. A record shown in a top category is not repeated.
- Both views (venue and all venues) are selected server-side and sent in the payload; the Home / Away | All venues switch is frontend only and opens on the engine's primary view.
- Neither gate value is shown to users (D115).
- **Built 2026-10-14** (`engine/eval/team_patterns.py`, called after `attach_league_comparisons` in the export): each fixture gets `patterns = {default_view: "venue"|"all", venue: {...}|null, all: {...}}`, each view with `top_results`, `top_attack_defence`, `also`, `see_all`. Records carry `side, team, event, label, k, n, rate, league_rate, league_comparison, streak, context` only — no posterior, Beta parameters, gap or confidence. `venue` is null when neither team has a venue sample; `default_view` is `venue` only when both teams' primary rows are venue rows. `bayesian[]` rows also gain `league_confidence` (internal). Check on the 13 Oct run after D123: fixture 111915 reproduces the review example; Zlin (90876) shows Lost 9/10 in Top Results; 592 fixtures, Top Results present in 82%, Top Attack/defence in 87%, median 6 records per fixture.
- Event labels (user-facing): `goalless` → "Failed to score" (never "Goalless", which reads as 0–0), `score_1goal` → "Scored 1+", `score_2goals` → "Scored 2+", `concede_1` → "Conceded 1+", `concede_2` → "Conceded 2+", `clean_sheet` → "Clean sheet", `BTTS` → "Both teams scored", `Over_x_5` / `Under_x_5` → "Over / Under x.5 goals", `win` → "Won", `loss` → "Lost", `draw` → "Drew", `winless` → "Winless", `unbeaten` → "Unbeaten", `halftime_win` → "Led at half-time".
- Current run: `streak`, shown only when > 0.
- Exclude `Over_4_5` and `Under_1_5` (D124: never qualified on the 13 Oct run). `halftime_win` is included by the 2026-10-22 owner correction; `draw` stays in Results.
- Attack/defence stays one category with a corresponding pair across the teams (D124); a single record when only one side qualifies.
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
8. ~~API~~ — done 2026-10-14: `GET /api/match/{fixture_id}` (§2) combines `/context` with the latest export. CLI: `python -m engine.api.match_detail <fixture_id>`. `/context` stays for Context-only refreshes.

## 7. Worked example — Zlin v Slavia Praha (90863), run 2026-10-07
- Context (Home/Away): Zlin 0.40 / 1.80, #16 / #13 of 16; Slavia 2.50 / 0.25, #1 / #1. All venues: Zlin 0.89 / 2.11, #12 / #16; Slavia 2.78 / 0.67, #1 / #2. Position 16th 1 pt vs 1st 23 pts (snapshot 2026-10-05).
- H2H: Slavia 3, draws 2, Zlin 0; 2.6 goals/game; over 2.5 in 3 of 5; both scored 3 of 5; at Zlin: Slavia 2, draw 1.
- Strength: Slavia Praha Big dog; Czech Liga Big dogs won 144 of 196 (73%), drew 35, lost 17, lower bound 68%.
- Patterns: Zlin lost 8/9 (89% vs 39%, much more often) ↔ Slavia won 7/9 (78% vs 39%); Zlin conceded 1+ 9/9 ↔ Slavia scored 1+ 9/9 (100% vs 74%).
- Goals: Slavia over 0.5 90% (league 73% away, much more likely); over 1.5 84% (83%, typical); goals 1–4 75% (85%, less likely).
