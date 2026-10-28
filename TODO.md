# To-do

One list for open work across the pipeline, engine and product. Detailed
history stays where it was recorded (linked); this file is the index.
Updated 2026-10-21 (beta blockers added; beta scope frozen, D152).

## Beta blockers (critical path)

What stands between today and the invite-only beta (target 10–24 Nov 2026,
[ship checkpoints](docs/product/ship-checkpoints.md)). Ordered by critical
path: each item gates the ones below it, except where marked parallel.

1. [x] **Commit the open working tree.** Done 2026-10-21 (`974e83d`, `f138422`). DLQ, fixture extraction, standings and
   match-detail changes plus their tests are uncommitted; a deploy needs a
   known commit to build from.
2. [x] **Freeze beta scope (checkpoint 1).** Reached 2026-10-21: cohort
   (D157), 10–15 testers in two waves (D150), Google and email sign-in (D151),
   pages = Welcome, Discover, Matches, Match details, Saved, Settings (D152).
   Deferred: D146 evidence depth, forecast record, forecast retention.
3. [ ] **Stand up staging (checkpoint 3, longest item).** Frontend (merge or
   deploy `codex/frontend-redesign`), account API, research API, Postgres,
   Clerk, Airflow jobs, secrets and S3. Includes a repeatable schema migration
   step: on 2026-10-20 a mart missing new standings columns made Match details
   return 503 until a manual refresh. Plan (2026-10-21, decisions in D153, revised
   2026-10-22: new Hetzner CAX21 (ARM, 8 GB, Helsinki; CX43 out of stock), frontend on it, Clerk production instance,
   beta at `beta.ballistics.football`): [docs/staging-deployment-plan.md](docs/staging-deployment-plan.md).
   v1 server inspected 2026-10-21 (too small to share). ARM build check passed
   2026-10-22. Server `ballistics-v2` created
   2026-10-22 (CAX21, Helsinki, 2.29.118.240, Ubuntu 24.04; SSH as root with
   `~/.ssh/ballistics_v2_ed25519`, alias `ballistics-v2`, user `sage`). Base
   setup done 2026-10-22 (swap, Docker, SSH hardening, UFW, fail2ban). Migration runner,
   baseline and deploy files (compose, Caddyfile, roles, grants) done and
   checked 2026-10-22. **Deployed 2026-10-27** (release `b5ed5b3f505`): seeded
   from local, all services up at `beta.ballistics.football`, Clerk production
   sign-in renders, crests copied to the staging bucket. Local DAGs paused and
   local Airflow stopped; staging's DAGs unpaused (except
   `saved_snapshot_maintenance`), first nightly cycle 2026-10-28 00:00 UTC.
   Nightly backup built and restore-tested 2026-10-27
   (`deploy/backup_postgres.sh`, 03:30 UTC cron). Hetzner Cloud Firewall
   2026-10-28 (`deploy/hetzner_firewall.sh`): web traffic only through
   Cloudflare. Still open: Hetzner weekly
   snapshots (owner, console), missed-run alert, uptime and disk checks.
4. [ ] **Member journey gaps (checkpoint 2, parallel with 3).**
   - Tester instructions for Google sign-in (D151): use the Google account the
     invitation went to; the 2026-10-21 "Last used" button signed in as another
     account.
   - Observe browser-triggered profile sync for a new account.
   - Check Settings and Saved under the invited account.
   - Minimal first-run onboarding (D152): in progress separately.
   - Privacy policy page on the frontend (`/privacy`), then add the home page
     and privacy URLs on the Google consent screen (project
     `ballistics-512818`, Branding) and **publish** it. Until then Google
     sign-in is in Testing: add each tester's Google address as a test user
     (Audience page, max 100).
5. [ ] **Match details build items (D152, parallel with 3).**
   - After kickoff, show the last pre-kickoff forecast. API side done
     2026-10-22: `/api/match` reads the fixture's most recent complete run and
     sets `fixture.forecast_from_latest_run` (migration 0002 adds the index;
     `engine_stale` skipped once kickoff passed). Frontend still to label the
     sections "made before kickoff" (frontend repo).
   - Small `data_quality` badge from the API block (second D148).
6. [ ] **Daily data reliability on staging.** Several clean daily
   `fixture_update` + engine cycles with no lost days (Oct 15 and 17 were lost
   to a standings failure), an alert that fires on failure, and the ~110
   stuck-NS fixtures resolved or hidden.
7. [ ] **Staging smoke test.** Fresh invited account end to end on desktop and
   mobile, data freshness, failure alert and rollback, all exercised on
   staging. Then send the first invites.

## Now

- [ ] **Check tonight's engine run** (run_ds 2026-10-20, ~00:40 UTC), the first
  with everything from today: dbt is no longer blocked by xG/standings; model
  reuse (`raw.model_processing_state` fills, run starts cold); `raw.card_versions`
  gains rows beyond the 5,265 backfilled (team-name fix makes ~330 teams' cards
  new versions once); no CANC/PST/ORPHAN fixtures in the feed; Discover shows
  provider team names ("FC Copenhagen", not "Fc Copenhagen").
- [ ] **Match pages lose their forecast once a match starts.** `/api/match` reads
  only the latest engine run, and runs cover not-started fixtures, so a match
  page shows no Strength/Goals/Patterns after kickoff (and for finished matches).
  Decided (D152): keep the last pre-kickoff forecast; tracked in beta blocker 5.

Local tz data fixed 2026-10-20: Anaconda's stale `share/zoneinfo` (2025b) now
links to macOS `/usr/share/zoneinfo` (2026c); the old folder is kept at
`/opt/anaconda3/share/zoneinfo.2025b-backup`. The engine image was already right.

## Pipeline (DLQ, D147/D148)

- [ ] ~110 fixtures stuck at NS from postponements (Egyptian league, USL Tormenta).

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

Done since July: Dixon-Coles feeds consensus; engine Airflow DAGs
(`engine_predict`, `engine_calibration`); dependency-aware model result reuse
(2026-10-19). SportyBet odds, EV and slips were built and then removed (D149).

Done 2026-10-20 (engine review): Bayesian forecasts not-started fixtures only
(18 cancelled/postponed/orphaned fixtures had been published on Oct 19) and
consensus drops non-NS fixtures as a backstop; one team-name source in dbt
(`stg_dim_teams`, no initcap; incremental facts full-refreshed); team markets
labelled with the right team (88 mislabels since Oct 1); Mismatch skips teams
with no Elo history; `fixture_update` runs dbt when side steps fail (Oct 15 and
17 were lost to a standings failure). Forecast record checked: calibrated within
noise in every market (4,860 settled). All database-backed tests pass on
throwaway databases (438 + DB suites).

## Product and frontend

Details: [docs/product/implementation-status.md](docs/product/implementation-status.md).

- [ ] Figma: Settings (timezone, followed leagues) not yet designed. Skipped
  for beta (D152); Settings ships as implemented.
- [ ] Fuller `data_quality` treatment on the match page beyond the beta badge
  (deferred, D152).
- [ ] D146 evidence-depth bar (Focused / More context / Full view): deferred
  past beta (D152).
- [ ] Deployment: nothing is deployed; frontend is on `codex/frontend-redesign`.
- [ ] Desktop Match details: production implementation; sidebar and H2H
  prototype navigation regression
  ([docs/product/desktop-match-details-todo.md](docs/product/desktop-match-details-todo.md)).
- [ ] Design backlog: onboarding / first run; real Strength content (settle
  the Naples label and Big dog threshold rule first); consistent mock/live labels.
