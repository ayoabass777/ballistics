# Ballistics frontend

The Ballistics 2.0 Next.js frontend lives here. The account API remains in the sibling `football_stats_project/API` checkout; the research API lives in this repository at `engine/api`.

## Local development

Copy `.env.example` to `.env.local` and set the local API URLs and Clerk keys. Keep `.env.local` private. From this directory:

```sh
npm ci
npm run dev -- --hostname localhost --port 3102
```

Run the account API on port 8000 and the research API on port 8001 for the signed-in research flow. Check the relevant frontend slice with `npm run typecheck:research` or `npm run typecheck:forecast`; `npm run build` builds the full Next.js app.

Staging builds this directory through `deploy/compose.staging.yml` in the Ballistics repository. The original frontend directory in `football_stats_project` is retained temporarily as a rollback source during migration.
