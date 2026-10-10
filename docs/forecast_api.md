# Forecast record API — local implementation

2026-10-04 integration update: a real Next.js page now exists in the original
frontend at `football_stats_project/frontend_dev/app/forecast-record/page.tsx`,
with same-origin authenticated adapters. The Python service is still private,
local and undeployed. The frontend requires a server-only `FORECAST_API_BASE_URL`;
no live connection has been configured by this task. `GET /api/forecast-record/filters`
now returns distinct captured country/league pairs, selected under the active
policy as of an optional browsing timestamp. No mock directory fallback.
All nine API tests, including the new directory test, passed in isolated PostgreSQL.

2026-10-03. Read-only FastAPI adapter in `engine/api/`. No production deployment,
schema migration, authentication integration or frontend/Figma changes. The older
`football_stats_project/API` service is unchanged. The engine's CLI report remains
available; this adapter performs SQL-side filtering/pagination instead of loading
that complete report into memory.

## Local operation

Install `engine/api/requirements.txt` in the chosen environment. Set
`FORECAST_API_DSN` explicitly to the intended ledger database using a restricted
reader account; there is no fallback to engine/application credentials. Do not
commit a connection string. Schema setup is a separate deployment operation:
the API never initialises tables, captures forecasts or settles results.

From the Ballistics repository:

```sh
uvicorn engine.api.app:app --host 127.0.0.1 --port 8001
```

OpenAPI and interactive API documentation are available at `/openapi.json` and
`/docs` on that local service. No server is left running by this implementation task.
Do not expose this unauthenticated local adapter publicly. Account/entitlement
enforcement, CORS policy, gateway rate limits, TLS, pooling and load testing remain
deployment work. Each request creates and closes its connection, enforces a
read-only transaction and a five-second SQL timeout; connection timeout is five seconds.
The database account should have CONNECT, USAGE on `forecast`, and SELECT on
`forecast.snapshots`, `forecast.entries` and `forecast.settlements`, not write/DDL rights.
Provisioning such a role on the live database was not performed.

## List endpoint

`GET /api/forecast-record`

| Parameter | Meaning |
| --- | --- |
| `date_from`, `date_to` | Optional inclusive UTC kickoff dates (`YYYY-MM-DD`); reversed ranges return 422. |
| `team` | Case-insensitive literal substring of either team name, maximum 100 characters; SQL wildcard characters are literal. |
| `league`, `country` | Optional exact captured names, combined with all other filters using AND. Country disambiguates repeated league names. Stable provider league IDs are not yet captured in the ledger. Single-league filtering only. |
| `outcome` | `all` (default), `settled` (hit/miss), `other` (void/unavailable/excluded), or any one of the six statuses. |
| `limit`, `offset` | Fixture-level page size (default 25, maximum 100) and offset (0–100000). No fixture is split across pages. |
| `as_of` | Timezone-aware browsing snapshot returned by the first request; reuse it, with identical filters, for subsequent pages. Future/naive times return 422. Omit to refresh. |

Example: `/api/forecast-record?team=Brooklyn&outcome=settled&limit=25`.

Selection applies the active latest-pre-kickoff policy **before** filters. Historical
versions cannot reappear because a replacement has another outcome. Snapshot and
settlement observations after `as_of` are excluded. Each request takes a shared
version of the ledger's advisory lock before issuing its timestamp: concurrent
readers coexist, but cooperating writers wait until the read finishes. This avoids
an in-flight older capture appearing halfway through pagination. The guarantee
depends on every writer following the ledger lock/timestamp protocol. It does not
protect against an administrator inserting backdated data directly.

Fixtures are ordered by captured kickoff descending, then fixture ID descending.
Fixture metadata/kickoff comes from the newest selected event in that fixture,
not mutable raw tables. Individual events retain their own captured kickoff. These
dates are therefore **archived schedule context**, not a live fixture schedule.

Response contract:

- `policy`, `population`, `as_of`, `timezone` and echoed `filters` identify the scope.
- `totals.fixtures` and `totals.forecasts` count all matches after every filter,
  not just this page. Outcome filtering keeps only matching events inside each
  fixture; a mixed hit/pending fixture can appear under both tabs.
- `cohort` counts the date/team/league/country population **before** outcome filtering.
  Its `status_counts` always includes all six status keys, even when zero.
- `pagination.next_offset` is null at the end; reuse `as_of` for the next page.
- `fixtures[]` includes captured team/league/country names, kickoff, selection cutoff,
  `selection_state` and matching `forecasts[]`.
- Each forecast includes stable `entry_id`, `event_key`, captured `event_label`,
  probability, `captured_at`, `captured_kickoff`, status/reason, latest outcome
  evidence/grader, `outcome_observed_at`, and `result_observation_state`.

`selection_state` is `provisional` before the earliest captured policy cutoff and
`final` afterward, evaluated at `as_of`. This describes whether the time window is
closed, **not** whether the match has a final result. Postponement follows the
conservative no-reopening policy documented in [forecast history](forecast_history.md).
A provider may temporarily block eligibility before the cutoff (e.g. cancellation).

An unobserved entry is `pending` / `not_yet_checked`, with null outcome time and
`result_observation_state=unobserved`. Otherwise the state is `observed`. **The
outcome timestamp is the last changed observation, not last successful polling
time or source freshness.** The existing writer skips unchanged results. Do not
label this field “last checked” or derive a reliable stale-data warning from it.
A separate polling/source-freshness record remains necessary for that feature.

## Correction-history endpoint

`GET /api/forecast-record/entries/{entry_id}/history`

Accepts the list's `as_of`, `limit` (default 50, maximum 100), and `after_id`
(default zero). Returns the immutable forecast and ascending observations, with
`next_after_id` for another page. Old selected versions under the active policy
remain inspectable by entry ID; historical first-policy entries are not exposed
by this adapter. Missing or ineligible entries return 404. No observation history
is embedded in list responses, keeping fixture browsing bounded.

Invalid requests return 422. An unavailable/missing ledger returns a generic 503,
never an empty successful record or raw SQL/connection details. Successful reads
use `Cache-Control: no-store`. An empty cohort returns 200 with empty fixtures and
zero totals.

## Verification and remaining work

Verified 2026-10-03: all 17 API/ledger tests passed together (eight API tests,
nine ledger tests), including isolated PostgreSQL and in-process HTTP requests.
Covered combined filters, both-team search, literal wildcard/injection strings,
inclusive date bounds, mixed outcomes, whole-fixture pages, empty results, stable
as-of browsing across new captures/corrections, paginated history, read-only
enforcement, validation/503 responses and actual capture → settlement → HTTP.
This is not a load test or a deployed network smoke test. Temporary PostgreSQL
was stopped afterward; no live application data was read or written.

Tests: `tests/test_forecast_api.py`. Integration tests require a **fresh empty**
`forecast_test*` database supplied through `FORECAST_API_TEST_DSN`; HTTP validation
tests run without it. No tests should target the application database.

Remaining: frontend wiring; live role provisioning/deployment; true result-check
freshness; stable league identifiers/multi-select; high-volume query plans and
load testing. SQL pagination bounds returned fixtures, not total database work:
exact cohort counts and latest-entry selection still scan the relevant ledger.
Do not claim production-scale performance based on small integration fixtures.
