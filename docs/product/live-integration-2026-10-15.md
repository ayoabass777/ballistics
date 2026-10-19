# Real-data integration — 15 October 2026

Owner authorised refreshing the stored snapshot and connecting the real frontend.

- `scripts/apply_match_store.sh` completed against local `ballistics_postgres`.
  Latest export: `consensus_slips_2026-10-14.json`; 812 page-ready fixtures.
  Run status complete; snapshot refreshed at 2026-10-15 14:15:32 UTC.
- Read-only API started on `127.0.0.1:8001` with an explicit DSN assembled from
  existing project database settings. Connection defaults and request transactions
  are read-only. No new database roles, grants or public listener were created.
- Original frontend `.env.local` now has server-only `BALLISTICS_API_BASE_URL`
  pointing to that loopback service. No credentials were copied into frontend code.
- Real `/api/discover` response: `coverage=ready`, run 2026-10-14. At verification,
  America/Vancouver next-seven-days returned 186 Standouts, 25 Strength, 488 Goals
  and 495 Patterns entries. Counts are time-dependent and categories overlap.
- Fourteen distinct Match details payloads selected across the lists returned
  successfully and passed the frontend schema. The full Discover response passed
  its schema. Single-league filtering returned only the selected warehouse league;
  invalid timezone returned HTTP 422.
- The real Next.js app at `http://localhost:3102/discover` requires Clerk sign-in;
  unauthenticated access remains blocked. The sign-in page loaded successfully.
  **Signed-in browser flow verified on 2026-10-15:** real Discover → Nottingham Forest vs Arsenal (fixture 68124) → Save → Saved → reopen current match → remove test save. Match context, strength, patterns and goals rendered; Saved showed the frozen engine run and retained the saved state on reopen. Cleanup returned Saved to its original empty state.
- The legacy `/dashboard` still reports a separate backend `/me` sync failure. Clerk authentication and the new research routes work; this check does not resolve that legacy integration.
- Saved remains per-account browser storage; this connection does not add a
  cross-device account store. The synthetic preview on port 3103 is separate.

Restart local API (the system timezone database avoids the stale Conda bundle):

```sh
PYTHONTZPATH=/usr/share/zoneinfo /opt/anaconda3/bin/python scripts/run_local_research_api.py --project-database
```

Alternatively supply `FORECAST_API_DSN` with a dedicated reader and omit
`--project-database`. This is local development integration, not deployment.
