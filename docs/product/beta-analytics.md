# Beta session analytics (D164)

Purpose: see where the 10–15 invite-only testers (D150) get stuck or drop out, so the funnel in [beta recruitment](beta-recruitment.md) can be recorded. With this few testers the goal is to find usability problems, not to measure demand or conversion.

The design is first-party usage events only. They're stored in our own Postgres `app` schema, written through the account API, and summed by dbt into a per-tester funnel. They're on by default, disclosed in the invite, and can be turned off in Settings. **There's no session replay** (no PostHog or other third party); to watch testers use the app, ask them to share their screen during the planned feedback sessions.

## What is already stored (no new event needed)

| Funnel step | Source |
|---|---|
| Invited | `app.beta_invite_codes.created_at` |
| Invite accepted | `app.beta_invite_codes.claimed_at` |
| Account created | `app.app_users.created_at` |
| Leagues chosen | `app.user_followed_leagues.created_at` |
| Fixture saved | `app.saved_fixtures.saved_at` |

## Gaps the events fill

- `app.app_users.onboarding_completed` is a boolean with no timestamp, and **Skip setup** and **Use all leagues** both send `league_ids: []`, so they can't be told apart.
- Tutorial and Match guide dismissals are kept only in browser `localStorage`, so the server never sees them.
- Visits, Match opens and returning after kickoff are not recorded anywhere.
- Removing a saved fixture deletes the row, so the removal leaves no trace.
- Errors testers hit (a failed API call, a broken page, pressing Retry) were only visible if a tester mentioned them.
- Searches and filters left no trace, so a search that finds nothing, or a filter that empties the list, couldn't be seen.
- Page speed was only known from complaints, as with the slow Matches search.

## Event list

The allow-list lives in `API/usage_events.py`. Any event or property not on it gets a 422. Properties must not contain email, name, IP address, free text or the full user agent.

| Event | Fired from | Properties |
|---|---|---|
| `session_started` | Added automatically before the first event after 30 minutes without activity | `entry_route` (route template, e.g. `/match/[id]`), `device` (`mobile` / `desktop`) |
| `onboarding_finished` | Onboarding: Continue / Use all leagues (`completed`), Skip setup (`skipped`) | `outcome`, `league_count` |
| `discover_viewed` | Discover, Matches and All standouts, once the member's leagues load | `page` (`discover` / `matches` / `all`), `followed_league_count` |
| `match_opened` | Match details | `fixture_id`, `after_kickoff` (bool) |
| `fixture_unsaved` | Any Saved toggle that removes a fixture | `fixture_id` |
| `tutorial_dismissed` | Feature tip "Got it", Match guide Done / Skip guide | `tutorial_id`, `action` (`got_it` / `done` / `skip`) |
| `tutorial_replayed` | Settings: Replay feature tips / Replay the introduction | `what` (`tips` / `introduction`) |
| `methodology_opened` | Match guide link, Match header "i", Discover and Forecast record tip links | `from_section` (`match_guide` / `match_header` / `discover` / `record`) |
| `request_failed` | Any same-origin `/api/...` call that returns an error status, gets no response, or returns data that fails its schema. One `fetch` observer in `usage-events.ts` covers every call site; aborted requests and the events endpoint itself are skipped | `api` (path template, ids as `[id]`, no query string), `method`, `status` (0 = no response), `reason` (`http` / `network` / `invalid`) |
| `client_error` | Uncaught script error or unhandled promise rejection | `route` (route template), `kind` (`error` / `unhandled_rejection`). No message: it can carry free text |
| `retry_clicked` | Try again on a failed list or Match, Retry leagues, preferences Try again | `api` |
| `search_used` | A team search on Discover, Matches or All standouts that stays unchanged for 1.5 s, so a burst of typing counts once | `page` (`discover` / `matches` / `all`), `result_count` (capped at 200), `query_length`. Never the search text |
| `filter_changed` | Dates, leagues, finding, category, market/pattern filter or sort changed; sent once the new list has loaded | `page`, `filter` (`dates` / `leagues` / `finding` / `category` / `evidence_filter` / `sort`), `result_count` |
| `page_speed` | Next's `useReportWebVitals`, once per metric per page load (INP when the tab hides) | `route`, `metric` (`lcp` / `inp` / `ttfb` / `fcp`), `value_ms`, `rating` (`good` / `needs-improvement` / `poor`) |

"Return visit after kickoff" means a `match_opened` with `after_kickoff = true` on a fixture the tester opened before kickoff or saved. A `data_issue_seen` event was considered and dropped, because Match details has no data-quality badge yet. Feedback sessions stay manual and are recorded in research notes.

## How it works

- **Browser** (`frontend_dev/lib/usage-events.ts`): `track()` adds events to a queue. The queue is sent in batches of up to 20 every 5 seconds and when the tab is hidden, using `fetch` with `keepalive`. Failures are dropped, so tracking never blocks a page. Nothing is sent until the member's preference has loaded. Signed-out visitors are never tracked.
- **Next proxy**: `/api/account/events` uses the shared account proxy (Clerk token, same-origin check, schema bound).
- **Account API**: `POST /account/events` takes `user_id` from the token, checks the allow-list, and inserts with `ON CONFLICT (event_id) DO NOTHING`, so a resent batch is never counted twice. If the member has opted out, the API stores nothing and replies `enabled: false`, and the browser then stops sending.
- **Opt-out**: `app.user_preferences.usage_events_enabled` (default true), changed through `POST /preferences/usage-events` and the Settings → Privacy checkbox.
- **Storage**: `app.app_events` (`API/sql/app_events.sql`, included in `bootstrap_accounts.sql`). Deleting an account cascades to its events.
- **dbt**: `models/analytics/` builds `stg_app__events` (view) and `beta_funnel` (table, one row per member) into `raw_analytics`. That schema is not in `deploy/grants.sql`, so neither the research nor the account API role can read it. Only the database owner can. Email is used only to join invitations and never appears in the output. The daily `dbt run` refreshes it. Per member it also carries `requests_failed`, `client_errors`, `retries`, `searches`, `empty_searches`, `filter_changes`, `lcp_p75_ms`, `inp_p75_ms` and `poor_speed_readings`; the event-level detail (which API failed, which filter emptied the list) is in `stg_app__events`.
- **Retention**: delete everything 90 days after the beta closes, or sooner if a tester asks. Not automated yet.

## Consent wording

### Invite (add to the invitation message under the D156 code)

> **What we record during the beta.** To find what's confusing or broken, Ballistics records which pages you open, which matches you open or save, when you skip a tip, how many results your searches and filters return (never what you type), errors you run into, and how fast pages load. This is stored on our own servers with your account, never sold or shared, and deleted within 90 days of the beta ending. You can switch it off in Settings → Privacy at any time.

### Settings → Privacy (built)

> During the beta, Ballistics records which pages and matches you open, when you skip a tip, how many results your searches and filters return (never what you type), errors you run into and how fast pages load, so we can find what's confusing or broken. This is stored with your account, never sold or shared, and deleted within 90 days of the beta ending.
>
> ☑ Record usage events

## Open points for the owner

- Confirm that default-on usage events with an opt-out are acceptable for UK, BC and Nigerian testers. This is not legal advice. If in doubt, flip the column default to false and accept a smaller sample.
- Write a short privacy note page linked from Welcome and Settings.
- Automate the 90-day deletion after the beta ends.
