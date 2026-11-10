# Beta activity and speed — 5 November 2026

Staging (`beta.ballistics.football`), wave 1 testers. Source: `app.app_events`, `app.app_users`, `app.saved_fixture_refs` and `app.user_followed_leagues`, queried 5 Nov 2026 ~09:00 UTC. Testers are labelled A–F because this repository is public; names are in `app.app_users`. The owner's two accounts (the main account and a second test account) are excluded from tester figures unless stated.

## Speed fix

On 4 Nov, testers saw slow pages. The server was not the cause: it was idle (load 0.15 on 4 cores) and its APIs answered in 20–140 ms. Two things were slow:

- **Returning to Standouts refetched everything.** Every return from a match refetched and re-validated the full Discover response (1.1 MB raw, ~170 KB gzipped, 522 matches).
- **Short stalls before the server.** Between 09:31 and 09:52 UTC, a few requests never reached the server, with waits of 5.6–22 s. They came from the Cloudflare, Clerk redirect or network path.

PR #21 (deployed 4 Nov 10:20 UTC, release `bf67f40b163`) keeps research data between page visits and refreshes it in the background after two minutes. On staging, going back from a match to Standouts showed the cards in 32 ms, with no new request.

| Page | Before the fix | After |
|---|---|---|
| `/standouts` TTFB | median 2.9 s, worst 6.8 s | 0.5–0.7 s |
| `/standouts` LCP | median 6.8 s, worst 12.5 s | no reading yet |
| `/discover` TTFB | median 0.7 s, worst 1.7 s | 0.2 s |
| Match page LCP | 1.0 s | 0.8 s |

No `request_failed` or `client_error` events have been recorded since the deploy.

**Limits:**
- **Few readings:** speed events are only sent on full page loads, not in-app navigation, so there are only a handful since the fix, most from the owner.
- **First visits are still slow:** a cold first visit takes about 4 s before cards appear. That splits into Clerk's script (~1.7 s cold) plus the Discover fetch (~1 s).

**Open speed work:**
- Make the Discover response smaller.
- Run the account calls (`/me`, saved, saved ids, leagues, preferences) in parallel.
- Record redirect time separately from server time in `page_speed`.

## Testers

Six testers have joined since 29 Oct. All of them use mobile.

| Tester | Joined | Last seen | Sessions | Active days | Matches opened | Discover views | Notes |
|---|---|---|---|---|---|---|---|
| A | 29 Oct | 5 Nov | 23 | 7 | 423 | 470 | Only regular user of filters (20) and search (4); 5 saved matches; follows no leagues |
| B | 1 Nov | 4 Nov | 3 | 3 | 0 | 3 | Views Discover but has never opened a match |
| C | 30 Oct | 2 Nov | 9 | 3 | 2 | 13 | Follows 33 leagues |
| D | 30 Oct | 1 Nov | 2 | 2 | 4 | 3 | 1 saved match |
| E | 29 Oct | 31 Oct | 6 | 3 | 3 | 14 | |
| F | 30 Oct | 31 Oct | 9 | 2 | 23 | 33 | Only tester opening the methodology (6 opens) |

**Daily active testers (owner excluded):**

| Day | People | Sessions | Matches opened |
|---|---|---|---|
| Thu 29 Oct | 2 | 3 | 1 |
| Fri 30 Oct | 5 | 20 | 108 |
| Sat 31 Oct | 3 | 10 | 108 |
| Sun 1 Nov | 4 | 6 | 13 |
| Mon 2 Nov | 3 | 6 | 45 |
| Tue 3 Nov | 0 | 0 | 0 |
| Wed 4 Nov | 2 | 6 | 59 |
| Thu 5 Nov (to ~09:00 UTC) | 1 | 2 | 121 |

## Where activity happens: All standouts

`match_opened` does not record the page a match was opened from. Each open is credited to the last list page (`discover_viewed`) in the same session, which is close but not exact.

**List page views:**

| Page | Views | Share | Testers |
|---|---|---|---|
| All standouts | 216 | 40% | 2 |
| Discover | 202 | 38% | 6 |
| Matches | 118 | 22% | 5 |

**Matches opened, by the list viewed just before:**

| Opened from | Opens | Share | Testers |
|---|---|---|---|
| All standouts | 192 | 42% | 1 |
| Discover | 151 | 33% | 4 |
| Matches | 96 | 21% | 3 |
| No list first (direct link or reload) | 16 | 4% | 3 |

- **All standouts is the busiest list page, but almost entirely because of A.** A opened 45% of their 423 matches from it, and 115 of 121 on 5 Nov.
- **No other tester has opened a match from All standouts.** C, D, E and F open matches from Discover or Matches. One other tester viewed the page without opening anything.
- **Two possible reasons:** the other testers may not find the page, which sits behind the "All standouts →" link on Discover. Or the five standouts in the Discover carousel may be enough for them. Worth asking in the interviews.

## Observations

- **Retention is the main problem now, not speed.** Five testers were active on 30 Oct; after 2 Nov only A and B have returned. The other four have not been seen since 2 Nov or earlier.
- **One tester accounts for most of the activity.** A has opened 423 matches, against 32 for the other five combined. Most of A's use came after the tracking release, so A's speed and error readings dominate the data.
- **The main loop is Discover → match → back,** repeated many times per session. That is the path the speed fix addressed.
- **Saving has barely caught on:** 6 saved matches across two testers. 16 unsaves were recorded, so some saves were later removed.
- **Search and filters are almost unused** outside A.
- **Followed leagues are uneven:** C follows 33 leagues, A follows none.
- **Methodology is rarely opened** (7 opens in total, 6 of them by F).

## Suggested next steps

- Ask the five quieter testers to try again now that returning to lists is instant, and ask what stopped them coming back. This feeds the [distribution plan](distribution-plan.md) wave 1 interviews.
- Interview A as the most engaged tester, about what keeps them coming back and why they haven't saved more.
- Re-run these queries after a few days of post-fix use to confirm the `/standouts` LCP improvement with tester readings rather than owner readings.
