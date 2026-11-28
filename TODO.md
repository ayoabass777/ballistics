# To-do

Open work only, most urgent first. Rewritten 2026-11-13 (v1 retirement added 2026-11-16, staging and live data updated 2026-11-18); the earlier list,
with its dated history (beta blockers, staging setup, engine review), is in
[docs/todo-archive-2026-11-13.md](docs/todo-archive-2026-11-13.md). Mark items
done here with a date, and move them to the archive when a section gets long.

Where things stand (2026-11-18): staging (`beta.ballistics.football`) deploys
from main again. The beta UI rollout from `codex/owner-live-matches` is merged
(PR #38), and so are the home/away swap correction (#36), deploys keeping the
engine's calibration files (#34), the market watch table (#35) and the research
API scipy fix (#37). Main `701ae49` is deployed (release `b701ae49f66`,
18 Nov 23:03 UTC).
The nightly pipeline has run clean for 12 nights in a row (2–13 Nov, every
`fixture_update_dag` and `engine_predict` task succeeded). Beta target window:
10–24 Nov ([ship checkpoints](docs/product/ship-checkpoints.md)).

## This week

- [ ] **Thu 19 Nov morning: check the home/away swaps were corrected** (#36).
  The 00:00 UTC `fixture_update_dag` correction pass should flip three fixtures
  before kickoff: Livyi Bereh v Shakhtar Donetsk (20 Nov, `fixture_id` 128098,
  Shakhtar home), Hajduk Split v Lokomotiva Zagreb (21 Nov, 136811, Lokomotiva
  home), Aluminij v Koper (23 Nov, 153016, Koper home). Still swapped on
  18 Nov 23:00 UTC. Its log warns once per swap; dbt and the engine rebuild
  them the same night. Check the Shakhtar match page shows Shakhtar at home.
- [ ] **Sun 22 Nov: first weekly refit since #34/#35** (00:00 UTC). A one-time
  read-only check is scheduled in the Claude app for ~01:00 UTC: refit
  succeeded, `calibration_maps.json`/`calibration_report.json` rewritten,
  `raw.market_watch` 14 → 16 rows, Monday's email shows the 22 Nov refit.

- [ ] **Check the first night with the new consensus gate** (run_ds
  2026-11-13, ~00:10–00:20 UTC on 14 Nov). `wait_for_quorum` should open a
  minute or so after the last model finishes, and all three models should be
  in `raw.consensus_pool`. If it waits 30 minutes instead, a model wrote an
  `error` marker: check that model's task log.
- [ ] **Retention.** Only 2 of 6 testers came back after 2 Nov
  ([5 Nov report](docs/product/beta-activity-2026-11-05.md)). Ask the quieter
  testers what stopped them; interview tester A (most engaged). Feeds the
  wave 1 interviews in [distribution-plan.md](docs/product/distribution-plan.md).
- [ ] **Re-run the beta activity queries** to confirm the `/standouts` LCP
  improvement from PR #21 with tester readings, not owner ones.
- [ ] **Wave 2 invites** (D150: 10–15 testers in two waves; wave 1 sent by
  2026-10-30). Decide after the retention answers.

## Retire the v1 server

Planned in D153 (2026-10-22) for "after v2 is live"; v2 has been live since
2026-10-27. Owner decided 2026-11-17 to retire it now; v1 is offline since then
(site redirects to beta, containers stopped). Left: watch, then delete. Checked 2026-11-16 (read-only): Hetzner
`vector-claw-01` (5.78.177.28, Hillsboro), ~$23/month, disk 91%, all six
containers up (Postgres, Airflow, API, Redis, frontend). Its
`fixture_update_dag` and `fixtures_etl_dag` were unpaused and ran every night,
on the same API-Football key as staging (7,500 requests/day shared with the
staging pipeline and live poller). `ballistics.football` still serves the v1
site through Cloudflare; `api.ballistics.football` points straight at v1.
Usage (v1 API log, 3–17 Nov): about 37 homepage data refreshes in two weeks;
the rest was bot traffic.
Background: [staging plan, v1 inspection](docs/staging-deployment-plan.md#v1-server-inspection-2026-10-21-read-only-as-sage).

- [x] **Pause v1's two DAGs** (`fixture_update_dag`, `fixtures_etl_dag`):
  done 2026-11-17; v1 no longer calls API-Football. Unpause to undo.
- [x] **Back up v1's database:** done 2026-11-17 on the Mac,
  `~/Backups/ballistics-v1/` (`football_betting_2026-11-17.dump`, 128 MB,
  pg_dump custom format, plus roles in `globals_2026-11-17.sql` and
  `SHA256SUMS`). Restored into a throwaway postgres:17: all 78 tables match
  v1's row counts exactly (15.1M rows). Not yet copied off the Mac (S3 or
  another disk). Not checked table by table against v2; v2 was seeded from
  local, and v1's tables are the old project's.
- [x] **Decide what `ballistics.football` shows:** redirect to
  `beta.ballistics.football` (owner, 2026-11-17) until the beta ends.
- [x] **Cloudflare:** done 2026-11-17. Redirect Rule "Root and www to beta
  (v1 retired)": `http.host in {"ballistics.football" "www.ballistics.football"}`
  → 301 `https://beta.ballistics.football/` (static: v1's paths don't exist on
  beta). The `api` A record (→ 5.78.177.28) is deleted. Checked: root, `www`,
  old paths and `http://` all 301 to beta; beta `/welcome`, `/sign-in` and
  staging return 200; Clerk and email records untouched.
- [x] **Stop v1's containers:** done 2026-11-17 (`docker stop`, restart
  policy set to `no`; nothing removed, `docker start` undoes it).
- [ ] The `ballistics-frontend` Worker still holds `ballistics.football` and
  `www` as Custom Domains. It gets no traffic (the redirect runs first) but
  its domains are what give the root and `www` DNS. To delete the Worker:
  detach the domains, then at once add proxied `AAAA 100::` records for the
  root and `www` so the redirect keeps working.
- [ ] **Watch for a week:** no traffic or errors on the old URLs, staging's
  API usage normal.
- [ ] **Take it down** (owner, Hetzner console): a final snapshot, then delete
  `vector-claw-01`. Remove its `~/.ssh/known_hosts` entry and archive the
  `football-stats-frontend` deploy notes.

## Live data: combined branch, ship before 21 Nov

The three live-data branches closed on 16 Nov (#30, #31, #32) are combined
on `live-data-combined`, **PR #39**, from main `701ae49` (2026-11-18). Their two
`0004` migrations are one `0005_live_data.sql` (main's `0004` is
`market_watch`); the `live_poller.py` conflict keeps both features. 576 tests
pass; 0003 → 0005 applied twice on a throwaway Postgres.

- [ ] **Merge and deploy PR #39 by Fri 20 Nov**, so the 21–22 Nov matchday
  checks it. Local DB first: delete `schema_migrations` version `0004` (the
  old `0004_fixture_statistics`), then `migrate.py up`.
  - #30: live final results go to `raw_fixtures` at full time; the nightly
    sweep re-checks each and rewrites only those the provider changed
    (alert lists them). Loads reject a final status without a score; no
    exclusive lock on `raw_fixtures`. `LIVE_PROMOTE_RESULTS=0` turns it off.
  - #31: full-time stats (corners, shots, possession, cards, xG) from the
    poller's by-id fetch into `raw.fixture_statistics`, no extra calls.
  - #32: 8 leagues added to `XG_LEAGUES` (26 → 34).
- [ ] **First matchday after deploy:** poller log `Promoted N live final
  results` and `Stored N full-time statistics`.
- [ ] **Next morning:** `fixture_update_dag` `load_updates` log `Swept N live
  results: … unchanged, … corrected`; any correction alert names the matches.
- [ ] **Three nights after:** the 8 leagues' last 120 days (~710 fixtures) are
  in `raw.fixture_xg`; re-run `calibration_wf` before trusting their
  forecasts. Fifteen nights after: the two-season backlog (~3,700) is done.
- [ ] **Wed 16 Dec: re-probe xG** for Hungary NB I, Ligue 2, Veikkausliiga,
  Virsliga, and the partial Superettan, Serbia, Slovakia (20 FT fixtures over
  120 days, add at ≥80%); method in `docs/xg_integration.md` on PR #39.
- [ ] After #39 merges, delete branches `live-results-promotion`,
  `live-fulltime-stats`, `xg-more-leagues` and `todo-live-data-checkpoints`.

## Live scores and Matches

- [ ] **Watch live scores on a full matchday on staging** (weekend of 14–15
  Nov): the poller keeps up at peak (~95 concurrent fixtures), scores match
  the final `raw_fixtures` result, and the Match details live card updates.
  Design and numbers: `docs/live_scores.md`, only on branch
  `live-scores-phase1` (copy it to main before retiring the branch).
- [ ] **Decide who sees live Matches.** It is account-gated today (owner and
  the `matches_first_beta` flag). Open it to all testers once the matchday
  check passes.
- [ ] Retire branch `live-scores-phase1`: PR #26 shipped its code through
  `codex/owner-live-matches`, now on main (PR #38, 2026-11-18), all but
  `docs/live_scores.md`. `codex/owner-live-matches` is fully merged and can go too. The `../ballistics-live` worktree still has
  uncommitted owner changes; review them before deleting the branch.

### Live data: three branches closed 2026-11-16, ready to reopen

Built and tested 2026-11-16, then closed by the owner. Each branch is pushed
and can be reopened as is. If more than one ships, renumber the later
migration: both live branches add `0004` (the local DB has
`0004_fixture_statistics` applied).

- [ ] **Decide by Fri 20 Nov whether to reopen any**, so a deploy lands
  before the weekend of 21–22 Nov and can be checked on a full matchday.
  - `live-results-promotion` (PR #30): final results from the live poller go
    straight into `raw_fixtures` at full time; the nightly update re-checks
    each one and rewrites only those the provider changed (alert lists them).
    Also: every load rejects a final status without a score, and the loader
    no longer takes an exclusive lock on `raw_fixtures` each run.
  - `live-fulltime-stats` (PR #31): full-time match stats (corners, shots,
    possession, cards, xG) from the poller's existing by-id fetch into
    `raw.fixture_statistics`, no extra calls. Local check: 394 of 499 finals
    had stats.
  - `xg-more-leagues` (PR #32): 8 leagues added to `XG_LEAGUES` (Argentina,
    Austria, Croatia, Romania, Greece, Liga MX, Eerste Divisie, J1); 26 → 34.
- [ ] **If #30 or #31 ships: check the first matchday after deploy** — poller
  log `Promoted N live final results` and `Stored N full-time statistics`.
- [ ] **If #30 ships: check the next morning** — `fixture_update_dag`
  `load_updates` log `Swept N live results: … unchanged, … corrected`; any
  correction alert names the matches.
- [ ] **If #32 ships: three nights after deploy** — the 8 leagues' last 120
  days (~710 fixtures) are in `raw.fixture_xg`; then re-run `calibration_wf`
  before trusting their forecasts. **Fifteen nights after:** the two-season
  backlog (~3,700 fixtures, 250 a night) is done.
- [ ] **Wed 16 Dec: re-probe xG** for the leagues that only started having it
  (Hungary NB I, Ligue 2, Finland Veikkausliiga, Latvia Virsliga) and the
  partial ones (Superettan, Serbia, Slovakia 60–65%). Method: 20 FT fixtures
  over the last 120 days, add at ≥80%. The 16 Nov results are in
  `docs/xg_integration.md` on branch `xg-more-leagues` only.

## First visit speed

~4 s cold before Discover cards appear.

- [ ] Make the Discover response smaller (1.1 MB raw, ~170 KB gzipped, 522 matches).
- [ ] Run the account calls (`/me`, saved, saved ids, leagues, preferences) in parallel.
- [ ] Record redirect time separately from server time in `page_speed`.
- [ ] Optional: record the source page on `match_opened`.

## Staging operations

- [x] **Stop deploys reverting the engine's runtime files.** Done 2026-11-17
  (PR #34): `deploy_staging.sh` keeps the server's
  `engine/eval/calibration_maps.json` and `calibration_report.json` across its
  forced checkout (`RUNTIME_FILES`); the committed copies only seed a fresh
  server. Verified 2026-11-18: unchanged through four deploys. The 13 Nov
  backups are deleted.
- [x] **Market watch log in Postgres.** Done 2026-11-18 (PR #35):
  `raw.market_watch` (migration 0004) replaces `engine/eval/watch/*.csv`;
  the server's 14 rows are imported.
- [x] **Match research down 22:04–22:46 UTC, 18 Nov.** `/api/match/<id>` returned 500:
  match details started importing `engine.eval.team_patterns` (scipy), which
  the research API image lacked. Fixed in PR #37 (scipy/numpy in
  `engine/api/requirements.txt`, plus a test that the API's imports are all
  installed).
- [ ] **Deploy only through `deploy_staging.sh`, one at a time.** On 18 Nov a
  hand checkout landed 18 s into a scripted deploy (images and `RELEASES`
  disagreed until a redeploy), and 15 hand-built frontend images (~23 GB,
  tags the deploy's prune never matches) filled the disk to 81%. They are
  deleted (disk 37%). Don't wipe the build cache to free space: the deploy
  keeps it at ≤ 10 GB, and a cold rebuild slowed the 18 Nov hotfix.
- [ ] **After the 22 Nov refit check passes:** on the server, delete
  `~/runtime-backup-2026-11-18`, `~/runtime-before-deploy.sha256`,
  `~/match_over_1_5-2026-11-18.csv` and `/srv/ballistics/engine/eval/watch/`.
- [ ] Remove `engine/eval/watch` from `RUNTIME_FILES` in `deploy_staging.sh`
  (nothing writes it since #35; harmless until then).
- [ ] Hetzner weekly snapshots (owner, Hetzner console).
- [ ] `saved_snapshot_maintenance` can't run in the Airflow image (SQLAlchemy
  1.4 vs the account API's 2.x), so it stays paused. Move it to a
  DockerOperator on the account API image **before Feb 2027**, when the last
  pre-made Saved partition runs out.
- [ ] Remove the leftover `deploy/OWNER_PREVIEW_RELEASES` on the server (from
  the hand-built owner preview; releases are in `deploy/RELEASES` again).

## Codebase audit follow-ups

Triage (unit 6.4, 2026-11-11) and each finding's evidence and retest are in
[docs/codebase-audit-map.md](docs/codebase-audit-map.md). The P0 (engine
quorum gate and DAG test) is done: PR #27, release `bac3aba0650`, 2026-11-13.

- [ ] Commit the audit map's 6.4 triage entry (uncommitted on
  `match-preview-and-docs` on 2026-11-13).
- [ ] **P1, data correctness:**
  - bootstrap metadata validation (2.1)
  - fixture response rate limiting (2.2)
  - partial xG (2.4)
  - replay errors and snapshot ordering (2.5)
  - detector event scope (3.2)
  - daily flow: latest-run status (3.3)
  - staging late or equal-timestamp rows (4.1)
  - intermediate and mart fixture coverage (4.2–4.3)
  - Elo backfills and result corrections: replay from the earliest changed kickoff (5.1)
  - forecast settlement for replaced snapshots (5.3)
- [ ] **P2, operations and API bounds:**
  - environment precedence and image/dependency pins (1.2–1.3)
  - whether side-task failures should block dbt and the engine (3.1)
  - a maximum range or pagination for Discover (5.4)
  - dbt data tests in a controlled gate; CI only compiles (6.2)
- [ ] **P3, cleanup:** the smaller follow-ups in units 1.2, 1.3, 2.1–2.2, 3.2,
  5.5 and 6.2; do them alongside related work.

## Engine

- [ ] Decide whether to drop the dead betting data: `raw.sportybet_odds` and the
  `est_odds/real_odds/odds/ev/has_odds` columns of `raw.consensus_pool` (kept, D149).
- [ ] Rename `sportybet_market` / `sportybet_selection` to neutral market names
  (pool rows, `raw.consensus_pool`, grading, dashboard, tests). Deferred in D149.
- [ ] Smaller: unused `threshold` CLI param in `predict_upcoming`; `int(NaN)` on
  missing goals in calibration scripts; empty `IN ()` SQL edge cases.

Parked (owner, 2026-10-20: no engine rework; flag concrete problems instead):
learned stacker to replace tier voting (would also fix Bayesian signals counted
twice in `avg_conf`); gating Bayesian picks on edge over the base rate;
aligning backtest and live Bayesian base rates; Poisson bias rechecks.

## Pipeline and API

- [ ] `/home/streaks` and `/explore/streaks` return 503 on staging: they read
  `public.team_form`, an old-project table staging never had. Only the old
  `/explore` and `/redesign` pages call them, not the beta pages. Drop the
  endpoints, or build `team_form` in dbt if those pages come back.
- [ ] ~110 past-season fixtures still NS from postponements (Egyptian league,
  USL Tormenta). Not shown anywhere (current-season readers only); clean up
  or mark them if the old seasons are ever reloaded.

## Product and design

Details: [docs/product/implementation-status.md](docs/product/implementation-status.md).

- [ ] Desktop Match details: production implementation; sidebar and H2H
  prototype navigation regression
  ([desktop-match-details-todo.md](docs/product/desktop-match-details-todo.md)).
- [ ] Design backlog: onboarding / first run; real Strength content (settle
  the Naples label and Big dog threshold rule first); consistent mock/live labels.
- [ ] Figma: Settings (timezone, followed leagues) not designed; Settings
  ships as implemented (D152).
- [ ] After beta: fuller `data_quality` treatment on the match page; D146
  evidence-depth bar (Focused / More context / Full view). Both deferred, D152.
