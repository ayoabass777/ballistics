# Research frontend

The new entry point is `/discover`; `/standouts`, `/match/[fixtureId]` and `/saved`
share the research components. Existing homepage and `/explore` remain intact.

Set server-only `BALLISTICS_API_BASE_URL` (or `FORECAST_API_BASE_URL`) to the
Ballistics FastAPI origin. Clerk authentication is required by `/api/research/*`.
The backend must include `/api/discover` and populate the discovery object in
`raw.match_analysis` through a new consensus run or the match-store backfill.
Unavailable API/configuration is shown explicitly; there is no sample-data fallback.

Saved uses browser storage scoped to the signed-in account and freezes the selected
card/run date. It is explicitly labelled local, and is not cross-device persistence.

Commands:

- `npm run dev:forecast`: real Next.js app on localhost:3102.
- `npm run typecheck:research`
- `npm run test:research:contracts`
- `npm run test:research:proxy`
- `npm run preview:research`: isolated synthetic QA harness on 127.0.0.1:3103.
  It bundles the real components with test navigation/auth adapters. No test adapter
  is imported by application routes. Uses the existing transitive esbuild installation.

See the Ballistics repository's `docs/product/frontend-build-2026-10-15.md` for
Figma source evidence, browser checks, remaining work and deployment boundaries.
