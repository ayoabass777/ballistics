# Real Forecast record page

Implemented locally 2026-10-04 at `/forecast-record`. Homepage navigation includes
the link (desktop and mobile menu); the older `/redesign` desktop navigation also links it.
Backend lives separately in the Ballistics repository's `engine/api` package.
No deployment or live database migration was performed.

## Connect

Configure `FORECAST_API_BASE_URL` in the frontend's server environment to the
trusted FastAPI service origin, e.g. `http://127.0.0.1:8001` for local development.
Do not append `/api/forecast-record`. Restart Next.js after configuration changes.
The server adapter sends only allowlisted query parameters and validates responses.
No database credentials or backend origin are sent to the browser.

Start the backend according to Ballistics `docs/forecast_api.md` with an explicitly
configured read-only `FORECAST_API_DSN`. This page does not initialise a ledger or
import old daily files. Nothing silently connects to the application database.
The current frontend environment did not have `FORECAST_API_BASE_URL` configured
when inspected, so a populated live record is not claimed.

The Next.js API adapters require Clerk sign-in. The page shell can be viewed
signed out and offers a sign-in link. Subscription entitlements and public
deployment hardening are still pending; sign-in alone is not payment enforcement.
Keep the unauthenticated Python API private behind the server boundary.

## Behaviour

- Uses the approved Figma desktop `287:37698`, mobile `287:37762` and expandable
  fixture component `289:37716` in file `dLgWAaafmzxQNcp2Rb9P7u`.
- Responsive green/off-white shell and blue event panels; adapts existing
  `GlassButton` because the referenced generic Figma Button package is absent.
  There are no visible image/SVG assets in the inspected record frames to download.
- All dates and either-team search are real controls, not preset examples. URL
  parameters retain applied filters. Date boundaries stay UTC to match the API;
  displayed times use the browser timezone, named explicitly. Timezone override
  remains a future account/settings feature.
- League picker is backed only by `/api/forecast-record/filters`, grouped into
  collapsed countries with search and single selection. It does not use the old
  site's mock-fallback navigation catalogue.
- Outcome filters show matching events only, with fixture and forecast totals
  from the API and pre-outcome-filter cohort counts. Never a pooled accuracy claim.
- Load more retains the API's `as_of` snapshot. Refresh discards it. Changing filters
  aborts stale requests and clears old rows. No client-side filtering of just the
  loaded subset. Histories load separately and paginate.
- Explicit loading, sign-in, unavailable, malformed-response and empty states.
  No mock fallback, including when `NEXT_PUBLIC_USE_MOCK_DATA` is enabled elsewhere.
- Per-event captured probability/time and outcome evidence remain distinct.
  Last changed outcome is explicitly not last checked. No invented freshness badge.
- Saved and Matches Figma destinations have not been implemented in this scope:
  they are omitted from this page's navigation instead of linking to unrelated pages.
  How it works is an accessible inline explanation, not the full Methodology product.

## Verification

2026-10-04: `npm run typecheck:forecast` passed (scoped to this page, root layout and
adapters/imported dependencies). `npm run test:forecast:contracts` and
`npm run test:forecast:proxy` passed.
The repository-wide type check was stopped while loading unrelated deployment
dependencies; no claim is made about its result. Backend API suite: nine tests
passed against a fresh isolated PostgreSQL database. Browser regression now passes:
expansion, correction history, as-of pagination, combined filters, empty/error states
and mobile overflow. Desktop and mobile rendered screenshots were inspected against
the previously fetched Figma design; the intentional adaptations above remain.
This supersedes the earlier startup-blocked result, not the live-connection boundary.

Start the preview with `npm run dev:forecast` and open
`http://localhost:3102/forecast-record`. This uses Turbopack without source maps and
binds to localhost to match Next's loopback normalization. The earlier 127.0.0.1
binding triggered a self-proxy error. Inter is now bundled locally in `app/fonts`
with its SIL Open Font License, preserving the existing font family while removing
the Google-font compilation network dependency that blocked the initial preview.

Browser regression script: `scripts/test-forecast-record.mjs`. Run against a local
Next.js server with Playwright available (or `PLAYWRIGHT_MODULE` pointing to an
existing installation). Use `PLAYWRIGHT_CHANNEL=chrome` to use an installed Chrome
instead of downloading a Playwright browser. `FORECAST_PAGE_URL` defaults to `http://localhost:3102`.
It intercepts browser API requests with explicitly synthetic test fixtures; those
fixtures are never imported by the app. Covers expansion, correction history,
as-of pagination, combined filters, empty/error states and mobile overflow.
Screenshots go to `/private/tmp/forecast-record-desktop.png` and
`/private/tmp/forecast-record-mobile.png`; they are test data, not live evidence.

Use an authenticated local session and configured backend for a real-data smoke
test. Do not infer a verified end-to-end live connection from intercepted UI tests.
