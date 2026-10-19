# First research frontend build — 15 October 2026

The owner authorised the frontend build after the prototype review. Source lives in
`/Users/ayomideabass/Documents/Projects/football_stats_project/frontend_dev`.
The existing homepage, redesign experiment and legacy fixture browser are preserved.

## Implemented locally

- `/discover`: shared desktop sidebar/mobile navigation, date and league filters,
  lime featured Standouts carousel, actual-scroll Previous/Next controls, three
  independent compact-card categories and explicit loading/error/empty states.
- `/standouts`: full qualifying list uses the same lime featured card, including
  Why it stands out, Explore match and Save. No new ranking rule or list cap.
- `/match/[fixtureId]`: warehouse-ID route and authenticated, validated API adapter;
  context rates/ranks, independent context/pattern venue controls, league-table and
  head-to-head disclosures, engine-selected patterns, strength and goals. Entry
  links carry section/team/event/context; Saved opens at Context. Both engine and
  standings timestamps are shown. Absent values remain absent, never zero-filled.
- `/saved`: browser-local shortlist partitioned by signed-in account; card values
  and run date frozen at save time. Explicitly labelled as browser-only; removal
  propagates to cards. This does **not** implement D37's cross-device account store.
- Forecast record's Discover links now return to `/discover`.

Shared code is `components/research/`, contracts in `lib/research.ts`, server-only
adapters in `lib/server/research-proxy.ts`. Existing GlassButton and local Inter
font are reused. Figma's generic `primitives` Code Connect import is not installed
in this application; the existing GlassButton supplies those controls. The checked
sources use text and dynamic crest slots, with no static image downloads required.

## API addition

`engine/api/discover.py` and `GET /api/discover` read the latest **complete**
`raw.engine_runs` snapshot. `match_store.build_rows` now stores a private `discovery`
object alongside existing page sections. It preserves engine-selected picks and
scores; read-time work only filters and sorts them. Scores and model internals do
not reach the frontend. Match details' public response does not expose the new
private storage object.

Parameters: `zone` (IANA), `scope=week|today|tomorrow|date`, `day=YYYY-MM-DD` for date,
and comma-separated warehouse `leagues`. The window starts after now, with calendar
midnight boundaries in the selected zone. League filtering precedes ranking.
Response: `lists.{standouts,strength,goals,patterns}`, league directory, timezone,
window, run date, finish time and `coverage=ready|needs_refresh`.

Existing stored runs need a backfill or the next consensus run to populate the new
object. This task **did not modify database contents or deploy the API**. Missing
snapshot coverage is reported explicitly. The API never reads the JSON export at
request time; saved exports were used only for offline contract verification.

## Verification

- Scoped frontend TypeScript check passed; research contract and proxy tests passed.
- Existing forecast TypeScript, contract and proxy tests still passed.
- Backend regression suite: **35 passed** in the engine container and locally with
  `PYTHONTZPATH=/usr/share/zoneinfo` (the local Conda timezone bundle is stale).
  Covers Discover API selection/window, match
  store, match detail, rolling standouts and independent Discover sections.
- All engine sections in the saved 13 October export passed the frontend match
  schema when paired with a synthetic Context fixture. Discover output built from
  that export passed its frontend schema. This is not a live database smoke test.
- Browser: real Next.js `/discover` correctly displayed the unauthenticated state.
  Populated checks used `scripts/research-preview.cjs`, an isolated esbuild harness
  with synthetic fixtures and a test account, not a runtime fallback or auth bypass.
- Checked 1440, 1024, 390 and 320px layouts; measured no page horizontal overflow.
  1024px category cards form two columns. Full-list cards retain lime styling.
- Verified carousel next enables previous, compact save updates both placements,
  Saved survives navigation, remove clears shortlist, Saved opens Context,
  Standouts opens Strength and Back returns to the list, independent venue values,
  See all patterns expansion, category switching and empty date state.
- [Desktop evidence](mock/frontend-discover-2026-10-15.jpg) uses labelled synthetic
  fixtures. No deployment or authenticated live end-to-end verification is claimed.

## Remaining integration work

Account-backed Saved and followed leagues, saved timezone preference, full Account
screen, migration of the legacy Matches browser, and the complete Methodology page
remain separate. The new league picker is a query filter, not a followed-league
store. Match-page disclosures are implemented inline; full overlay/page parity and
all rare league/group states have not been visually audited. A clicked Discover
item only receives row highlighting when that item is present in the match payload;
Standouts and match-detail eligibility remain distinct engine decisions.

Configure `BALLISTICS_API_BASE_URL` (or existing `FORECAST_API_BASE_URL`) server-side,
refresh the stored snapshot, and run an authenticated live smoke test before release.
