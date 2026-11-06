# Live scores with the forecast on top

2026-11-05 design; phase 1 built 2026-11-06 (branch `live-scores-phase1`, not
yet deployed). Phases 2–4 below are still proposals.

## Goal

A live-score experience members already know (today's matches by league, live
minute, score, red cards, goal timeline), with Ballistics' pre-match research
as the difference: every live match shows what we expected and, as it plays,
how that expectation is holding up.

Live data never changes the stored forecast. D152 still applies: the page keeps
the pre-kickoff forecast, and live tracking is a separate, clearly labelled
layer.

## Measured load (staging, last 28 days)

| | |
|---|---|
| Tracked leagues | 65 (`data/metadata.yaml`) |
| Fixtures per day | ~92 |
| Hours a day with at least one tracked match live | 16.2 |
| Peak concurrent live matches | 95 |
| 90th percentile concurrent (when any are live) | 31 |
| Staging box | 4 vCPU, 7.5 GB RAM (~4.5 GB available), 36 GB disk free |

This is a few hundred KB per poll and a handful of real changes per second at
peak. It needs no cluster.

## Provider budget (API-Football)

- `fixtures?live=all` returns every live match, with goals, status, minute and
  events, in one call; keep only tracked leagues. The provider refreshes live
  data about every 15 s, so polling faster buys nothing.
- Live scores and events, polled every 15 s, only while something is live:
  16.2 h × 240 calls/h ≈ **3,900 calls/day**. That fits Pro (7,500/day)
  alongside the nightly pipeline, but leaves a thin margin: measure the
  pipeline's daily use from `/status` before committing.
- Live match statistics (shots, possession) cost one call per live match. At
  60 s cadence that is ~10–12k calls/day, which needs **Ultra (75k/day)**.
- Lineups: one call per match near kickoff, ~92/day.

Latency is the provider's ~15 s plus our poll. Dedicated live-score apps buy
low-latency feeds (Sportradar, Opta) that cost orders of magnitude more; we
should not compete on speed, only on the research layer.

## Architecture

```
API-Football ──15s──▶ live-poller ──▶ Postgres
                         │             raw.live_fixture_state  (one row per live fixture)
                         │             raw.live_events         (append; goals, cards, subs, VAR)
                         └─ NOTIFY live_changed(fixture_id)
                                          │
                       research-api  LISTEN ─▶ GET /live/stream (SSE)   + GET /live (snapshot)
                                          │
                         Next.js / browser: Live list, Match header, Saved
```

- **live-poller**: one small long-running container in the staging compose
  (not an Airflow DAG; 15 s cadence is a service, not a batch). It sleeps when
  no tracked fixture is within kickoff−10 min … kickoff+150 min, using
  `raw.fact_fixtures`, so it spends no calls overnight.
- **Diffs, not snapshots**: compare each poll with the stored state and write
  only changes. An event that disappears is marked `revoked_at`, never
  deleted. VAR decisions come from the provider's own `Var` events, not from
  revokes (see Phase 1 as built).
- **Its own tables.** The poller never writes `raw.raw_fixtures`. The nightly
  pipeline stays the source of truth for results; the 2026-11-01 Elo crash
  (FT written without its score) is the reason not to give a second writer
  that table.
- **Fail loudly.** API errors and an empty response while fixtures should be
  live raise an alert (reuse `failure_alerts`), per the July plan-lapse lesson
  (an empty response looked like a quiet weekend).
- **Delivery, phase 1**: the browser polls `GET /live` every 15 s through the
  existing research proxy, with a 5 s server cache. No new transport, works
  with the current Clerk-checked proxy. **Phase 2**: server-sent events from the
  research API, routed by Caddy (`flush_interval -1`); decide then whether
  live scores need sign-in.

### Where Kafka fits

Not needed for the product at this volume; Postgres NOTIFY plus SSE carries it.
If a streaming pipeline is wanted for the portfolio, run **single-node
Redpanda** (Kafka protocol, ~1 GB RAM) on the same box: the poller produces
`live.fixture_updates`; separate consumers write Postgres, push notifications,
and grade forecasts. That earns topics, consumer groups and replay, but it is
learning infrastructure, and the blog post should say so.

## The differentiator: research on a live match

1. **Forecast tracker (phase 1, no new model).** Each Goals row and Pattern on
   the match turns into a live status from the score and events alone:
   - Over 2.5 · 64% → "2 goals, needs 1 more", then "Landed 58'".
   - Both teams score → "Home scored, waiting on away".
   - Pattern "Away scored in 8 of 9 away games" → "Continuing" / "Not yet".
   - Team strength: "Favourite (Big dog) leading / level / trailing".
   - At full time the forecast is graded immediately, ahead of the nightly
     Forecast record.
2. **Live estimate (phase 3, optional).** From the pre-match goal rates
   (Poisson / Dixon–Coles), the chance of the rest of the match given the
   score and minute: remaining goals ~ Poisson(λ × minutes left / 90), with a
   red-card adjustment. Cheap to compute; needs its own calibration check
   before it is shown, and must be labelled as a live estimate, not the
   forecast.

## UX (familiar first)

- **Live / Today list**: league headings with logos, kickoff or live minute,
  score, red cards, a brief highlight on a goal, and one forecast chip per
  match ("Over 2.5 · 64% · on track"). This matches the 2026-11-03 Matches
  league-grouping decision.
- **Match details**: live score and minute in the sticky header, an event
  timeline, and a "Your forecast, live" card above the existing sections.
- **Saved**: saved fixtures show live score and tracker status, so the
  shortlist becomes "my matches today".
- Later: goal and full-time push notifications for saved matches (new cost and
  consent work).

## Cost (monthly)

| Item | Phase 1–2 | With Redpanda | Managed alternatives |
|---|---|---|---|
| Provider plan | Pro ~$19 (scores/events) or Ultra ~$29 (with live stats) | same | same |
| Poller + tables + SSE | $0 (existing box) | $0 | — |
| Message broker | none (Postgres NOTIFY) | $0 on box (~1 GB RAM) | Confluent/Redpanda serverless ~$0–30; Kinesis ~$30–40; MSK ~$70–100+ |
| **Total** | **~$19–29** | **~$19–29** | **~$50–130** |

Prices from memory (mid-2026); confirm before buying.

## Phases and effort (rough)

1. Poller, tables, `/live` snapshot, Live list and Match header, forecast
   tracker: ~1 week.
2. SSE stream, event timeline, Saved live state, monitoring/alerts: ~3–4 days.
3. Live estimate with calibration check: ~1 week, only after 1–2 show use.
4. Optional Redpanda pipeline for the portfolio: ~2–3 days.

## Phase 1 as built (2026-11-06)

| Piece | Where |
|---|---|
| Tables `raw.live_fixture_state`, `raw.live_events` | `db/migrations/0003_live_scores.sql` |
| Parsing, diffs, live window, alert rules (pure) | `etl/src/live_scores.py` |
| Poller service (HTTP, Postgres, pacing, alerts) | `etl/src/live_poller.py`, service `live-poller` in `deploy/compose.staging.yml` |
| `GET /api/live`, `GET /api/live/{fixture_id}` | `engine/api/live.py`, `engine/api/app.py` |
| Proxy routes (signed-in only, like all research) | `frontend_dev/app/api/research/live/` |
| Forecast tracker (pure) | `frontend_dev/lib/live.ts` |
| Matches "Live now" / "Finished today", Match header score, "Your forecast, live" card | `frontend_dev/components/research/Live.tsx` |
| Tests | `tests/test_live_scores.py`, `tests/test_live_api.py`, `frontend_dev/scripts/test-live.cjs` |

How it behaves:

- **Window.** A fixture in `raw.raw_fixtures` is pollable from 10 minutes
  before kickoff to 180 minutes after, until its live status is final or
  stopped. With nothing pollable the poller sleeps until 10 minutes before the
  next kickoff (rechecking at least every 10 minutes) and makes no calls.
- **Full time.** `live=all` drops a match when it ends, so a fixture that
  leaves the list is fetched once by id (`fixtures?ids=`, up to 20 per call)
  for its final score and events. `finished_at` is set then.
- **Events.** The provider has no event id; `event_key` is
  type|detail|side|player|minute|extra|occurrence. The provider often
  corrects an event a poll or two later (scorer filled in, minute moved,
  booking reassigned, substitutions renumbered); a missing event is matched to
  a new one with the same type and side, a minute within 3 and a compatible
  player, and updated in place. An unmatched missing event is marked
  `revoked_at`, and restored if it returns. **Revoked means "the provider
  removed it", not VAR**: it also covers phantom entries (scorer-less goals
  later dropped) and corrections the matching misses (a minute moved by 17, a
  booking moved to the other team, a goal reclassified as an own goal). VAR
  decisions are the provider's own `Var` events (Goal cancelled, Goal
  confirmed, Penalty cancelled/confirmed, Card upgrade confirmed); anything
  that announces VAR (a phase 2 "goal disallowed" alert) reads those. Red
  cards in the state row are counted from events (Red Card, Second Yellow
  card).
- **Quota.** The provider's `x-ratelimit-requests-remaining` header drives
  pacing: below `LIVE_DAILY_RESERVE` (1,500) calls left it polls every 60 s;
  below `LIVE_DAILY_FLOOR` (300) it pauses until 00:00 UTC and alerts.
- **Alerts** (`send_alert`, at most one per kind per hour): 5 failed polls in
  a row (HTTP errors and the 200-with-`errors` responses a lapsed plan gives),
  an empty live feed while 3+ tracked fixtures kicked off 15–90 minutes ago,
  and the quota pause.
- **API.** `/api/live` lists fixtures polled in the last 6 hours or finished
  in the last 6 hours, with crests and the match's leading pre-kickoff
  insight (`pick`, chosen as Matches chooses its preview, from the fixture's
  last complete run). Cached 5 s. `/api/live/{id}` returns the state (null
  before the fixture is seen live) and events other than substitutions.
- **Frontend.** Matches shows "Live now" and "Finished today" above the
  upcoming list (which only holds fixtures not yet kicked off), grouped by
  league, filtered by the selected leagues, with a chip grading the leading
  insight. Match details shows the score and minute in the sticky header and a
  "Your forecast, live" card (strength, every Goals row, the top four
  patterns) plus goals, red cards and VAR events. Both poll every 15 s while
  the tab is visible. Match details polls from kickoff−10 min to kickoff+4 h,
  or whenever the poller has the match live (the snapshot kickoff can be stale
  when a match moves after the nightly run; seen on 2026-11-06), and reads
  once otherwise; it stops at full time. "Finished today" starts collapsed so
  a busy day's results do not push the upcoming list down. A failed poll keeps the last score and
  says updates are paused; data more than 90 s old is marked "delayed".
- **Grading.** Goals markets (`X Over n.5`, `X Under n.5`, `Match Goals a-b`)
  and patterns (W/D/L, unbeaten, winless, scored/conceded 1+/2+, clean sheet,
  failed to score, BTTS, Over/Under lines) are checked from the score alone.
  "Led at half-time" is not tracked. Matches in extra time are not graded on
  goals or patterns (90-minute markets). The full-time grade is labelled
  provisional; the Forecast record stays the source of truth.

Deploy: `deploy_staging.sh` applies migration 0003 and `up -d` starts
`live-poller` (Airflow image, Airflow env for the API key and alert channels).
`docker compose … logs -f live-poller` shows each changed poll; the log file
is `data/logs/live_poller_log.txt`.

Checked 2026-11-06: migration, store writes (goal, revoke, red card, full
time), and both API reads against the local database; the Matches and Match
details UI in the isolated QA preview
(`node frontend_dev/scripts/research-preview.cjs`, synthetic live data) at
desktop and 375 px.

First real matchday, 2026-11-06 (poller run locally from 14:50 UTC): 66
tracked fixtures polled, finished ones closed by one by-id fetch each, none
stuck, no errors; for all 66 the stored goal events add up to the score; about
1,500 calls by 21:23 UTC. Before the correction matching, 21 of 63 events
were revoked, none of them VAR. After it, 18 goals and cards were revoked by
21:23 UTC: about 5 real VAR decisions (each with its `Var` event), about 6
phantom scorer-less goals and about 7 missed corrections, which is why revoked
is not used as a VAR signal. 13 `Var` events arrived by then.

## Open decisions

- Pro vs Ultra (live statistics or not).
- Live scores for signed-out visitors (SEO, sharing) or members only.
- Whether the forecast tracker's full-time grade may appear before the nightly
  pipeline confirms the result (show as "provisional").
- API-Football terms for displaying live data to members: check before launch.
