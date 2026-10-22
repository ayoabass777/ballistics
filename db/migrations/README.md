# Pipeline database migrations

Numbered SQL files applied in order by `scripts/migrate.py`, each once and in
its own transaction. Applied versions and file checksums are recorded in
`public.schema_migrations`.

```sh
python scripts/migrate.py status      # which files are applied or pending
python scripts/migrate.py up          # apply pending files
python scripts/migrate.py baseline 0001   # existing database: record 0001 without running it
```

Rules:

- Every schema change to `raw`, `dim`, `elo`, `forecast`, `forecast_archive`
  or `data_detector` gets a new file: `NNNN_short_description.sql`, the next
  free number. Code that creates tables lazily (`CREATE TABLE IF NOT EXISTS`)
  may keep doing so, but the table must also exist in a migration.
- Never edit an applied file; the runner refuses when a checksum changes.
  Fix a mistake with a new migration.
- Not covered here: dbt models (`dbt build` owns them), the `app` schema
  (`football_stats_project/API/sql/bootstrap_accounts.sql`, run after these
  migrations because `app.user_followed_leagues` references `dim.dim_leagues`),
  and Airflow's metadata database.
- Deploy order: `migrate.py up` → account bootstrap → `dbt build` → restart
  the APIs. A failed step stops the deploy before the APIs restart.

`0001_baseline.sql` is the schema-only dump of the local database on
2026-10-22. It was checked on a throwaway PostgreSQL 17.4: it applies cleanly,
a second `up` is a no-op, its dump matches the local schema, the account
bootstrap runs on top, and `dbt seed` + `dbt run` build all 61 models. The
local database was then baselined at 0001.
