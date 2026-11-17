# To-do

Open work only, most urgent first. Rewritten 2026-11-13; the earlier list,
with its dated history (beta blockers, staging setup, engine review), is in
[docs/todo-archive-2026-11-13.md](docs/todo-archive-2026-11-13.md). Mark items
done here with a date, and move them to the archive when a section gets long.

Where things stand (2026-11-13): staging (`beta.ballistics.football`) deploys
from main again and runs owner-live Matches with live scores, current
standings tables on Match details, and the engine consensus gate fix (PR #27),
release `bac3aba0650`.
The nightly pipeline has run clean for 12 nights in a row (2–13 Nov, every
`fixture_update_dag` and `engine_predict` task succeeded). Beta target window:
10–24 Nov ([ship checkpoints](docs/product/ship-checkpoints.md)).

## This week

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
  `codex/owner-live-matches` (all but `docs/live_scores.md`). The `../ballistics-live` worktree still has
  uncommitted owner changes; review them before deleting the branch.

## First visit speed

~4 s cold before Discover cards appear.

- [ ] Make the Discover response smaller (1.1 MB raw, ~170 KB gzipped, 522 matches).
- [ ] Run the account calls (`/me`, saved, saved ids, leagues, preferences) in parallel.
- [ ] Record redirect time separately from server time in `page_speed`.
- [ ] Optional: record the source page on `match_opened`.

## Staging operations

- [ ] **Stop deploys reverting the engine's runtime files.** The deploy's
  forced checkout resets `engine/eval/calibration_maps.json`,
  `calibration_report.json` and `watch/*.csv`, which the weekly refit
  rewrites on the server. On 13 Nov they were copied to
  `~sage/runtime-backup-2026-11-13*` and restored by hand after each deploy.
  Move them out of git (or out of the checkout), or have the deploy keep them.
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
