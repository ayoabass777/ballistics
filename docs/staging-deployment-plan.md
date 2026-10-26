# Beta staging deployment plan

Recorded 2026-10-21 for beta blocker 3 ([TODO](../TODO.md)) and
[ship checkpoint 3](product/ship-checkpoints.md). This is a plan, not a record
of anything deployed. Owner decisions were made on 2026-10-21 (D153, listed
at the end); the plan below follows them.

2026-10-22 source update: the frontend and account API now live in this
repository at `frontend_dev/` and `API/`. The original checkout remains
available for rollback. The initial two-repository inspection below is
historical; staging now builds from one Ballistics commit.

## Goal

A production-like environment where an invited tester can complete the D152
journey (invite code → account → onboarding → Discover → Match → Save → Saved
→ sign out) on real data that refreshes daily, with alerts on failure, a
tested backup and a tested rollback. After the checkpoint 3 smoke test passes,
the same environment serves the invite-only beta; nothing needs to be built a
second time.

## What exists today (checked 2026-10-21)

- **Ballistics v1 is live.** `ballistics.football` serves the old Next.js app
  through Cloudflare (Workers, `frontend_dev/wrangler.jsonc`), and
  `api.ballistics.football` points at a Hetzner server (`5.78.177.28`) whose
  API still returns current streak data. Staging must not touch either.
- **One deployment repo, five services.** `ballistics`: Postgres, Airflow
  (webserver, scheduler; dbt and engine run as DockerOperator containers),
  research API (`engine/api/app.py`, port 8001, read-only DSN), account API
  (`API/app.py`, port 8000, Clerk, invites, Saved, preferences), and the
  frontend (`frontend_dev`, Next.js with Clerk). The original checkout remains
  intact during migration.
- **One database.** Account tables (`app`) live in the same `ballistics`
  database as pipeline data (`ACCOUNT_DB_NAME=ballistics`). It is 6.2 GB,
  mostly `raw.fact_head_to_head_streaks` (2.8 GB) and `_events` (2.1 GB).
  Airflow's own metadata tables also sit in `public` of that database.
- **The research API reads only the database.** The consensus task writes
  each run's page sections to `raw.match_analysis` / `raw.engine_runs`
  (`engine/eval/match_store.py`), and Match details and Discover read those
  tables. The engine also writes a JSON export per run
  (`engine/eval/consensus_slips_<ds>.json`, up to 80 MB; 1.7 GB locally), but
  nothing serves from it: `match_detail.load_export` has no callers, and the
  file is only used by `scripts/apply_match_store.sh` to backfill old runs.
- **The research API has no auth.** The frontend proxy
  (`lib/server/research-proxy.ts`) checks the Clerk session, then calls the
  API with no credential. Safe only while the API is not reachable from the
  internet.
- **Schema changes have no single path.** Bootstrap SQL in `etl/sql`, two dated
  `migrations_*.sql`, six `scripts/apply_*.sh`, tables created lazily in Python
  (`update_standings.py`, DLQ modules, engine stores), account SQL in
  `API/sql/bootstrap_accounts.sql`, then dbt. The 2026-10-20 Match details 503
  (mart missing new standings columns) is this gap.
- **The frontend is not ready to deploy from git.** `codex/frontend-redesign`
  has 20 uncommitted files and 5 unpushed commits, and tracks
  `origin/codex/vps-migration-baseline-2026-03-05`.
- **A GitHub personal access token sits in `football_stats_project/.env`**
  (ignored by git). It is not needed by any service and should not travel to
  the server.

## v1 server inspection (2026-10-21, read-only, as `sage`)

- **Size:** Hetzner Cloud `vector-claw-01` (Hillsboro, us-west), Ubuntu
  24.04, 3 vCPU, 3.7 GB RAM with 2.6 GB in use and no swap, 38 GB disk at
  91% (3.5 GB free). Up 240 days.
- **What v1 runs:** one compose project, `football-stats-frontend`
  (`/home/sage/football-stats-frontend`, branch
  `codex/vps-migration-baseline-2026-03-05` at `b922b1f`): Postgres 17
  (`football_betting`, 3.3 GB), Airflow webserver and scheduler, the v1 API,
  Redis and a frontend container. Airflow has `fixture_update_dag` and
  `fixtures_etl_dag` unpaused; `fixture_update_dag` succeeded on each of the
  last three days. The public site at `ballistics.football` is served by
  Cloudflare, not by this frontend container.
- **Disk use:** Docker images 16.6 GB (15.7 GB reclaimable) and build cache
  12.8 GB (12.4 GB reclaimable); volumes 4.6 GB.
- **Reverse proxy:** nginx on 80/443 with a single site,
  `api.ballistics.football` → `127.0.0.1:8000`, Let's Encrypt certificate
  managed by certbot. fail2ban is running (it banned this Mac's IP after
  five failed logins during this inspection).
- **Ports:** containers publish 3000, 5432, 8000 and 8080 on all interfaces,
  but none of those (or 6379) was reachable from outside, so a firewall in
  front of the server blocks them. Only 22, 80 and 443 should be open.
- **API-Football key:** the same key as local (`.env`, the Airflow scheduler
  and the v1 API all match the local key's SHA-256 prefix). Local and v1
  already share the 7,500 requests a day; v2 would be a third user.
- **Other:** v1's `.env` is world-readable (`-rw-rw-r--`).

**Consequence:** v2 does not fit on this server as it is. RAM is the hard
limit: v1 already uses 2.6 of 3.7 GB, and v2 adds about 2 GB at rest plus
several GB while the engine and dbt run. Disk could be freed by pruning
Docker images and build cache, but RAM can only be fixed by resizing.

## Target topology

The whole v2 stack runs on a new Hetzner server in the EU, as one Docker
Compose project, with Cloudflare in front and nothing but HTTPS exposed. v1
stays on its own server until it is retired after v2 is live (D153, revised
after the v1 inspection):

```
tester ──HTTPS──> Cloudflare (DNS, proxy, TLS)
                     │  staging. / beta.ballistics.football
                     ▼
              ┌──────── new v2 server (Hetzner CAX21, Helsinki) ──────┐
              │ caddy :443 ──> frontend :3000 (next start)           │
              │                   ├──> account-api :8000             │
              │                   └──> research-api :8001            │
              │ postgres :5432 (DBs: ballistics, airflow; not public)│
              │ airflow-scheduler, airflow-webserver (SSH tunnel)    │
              │   └─ DockerOperator: dbt, engine  → /srv/engine-eval │
              └──────────────────────────────────────────────────────┘
        S3: ballistics-*-staging buckets · Clerk · API-Football · SMTP alerts
```

- **Host: a new server, sized below.** The v1 server is too small to share
  (see the inspection above), and a new EU server costs less than resizing
  it, even while v1 still runs.
- **Frontend on the server, not Workers.** Running `next start` from the
  existing `frontend_dev/Dockerfile` keeps both APIs on the internal Docker
  network, so the research API needs no new auth.
- **Reverse proxy.** Caddy owns 80/443 on the new server; v1 keeps its own
  nginx. TLS uses a Cloudflare origin certificate. A Hetzner firewall allows
  80/443 (later only from Cloudflare IP ranges) and 22 from the owner's IP.
- **Internal tools over SSH tunnels.** The Airflow UI and the consensus
  dashboard listen on `127.0.0.1` only.
- **Hostnames.** `staging.ballistics.football` for the smoke test, then
  `beta.ballistics.football` for testers (same stack). v2 moves to the root
  domain when v1 is taken down.
- **Airflow UI** is not exposed; reach it with `ssh -L 8080:localhost:8080`.

## Clerk production instance (2026-10-26)

The Ballistics Clerk app already had a production instance for
`ballistics.football` (created in the v1 era, status "Incomplete": domain
records never added). It was reused rather than recreated:

- **DNS** (Cloudflare, all DNS only, verified with public resolvers):
  `clerk` → `frontend-api.clerk.services`, `accounts` →
  `accounts.clerk.services`, `clkmail` → `mail.e232z41koaxn.clerk.services`,
  `clk._domainkey` → `dkim1.e232z41koaxn.clerk.services`, `clk2._domainkey` →
  `dkim2.e232z41koaxn.clerk.services`. Clerk then marked the domain, Frontend
  API, account portal and email **Verified** and started issuing its SSL
  certificates.
- **Access mode:** Invite-only (already set).
- **Email:** sign-up and sign-in with email, verified by one-time code.
- **Google (D151):** own OAuth client created 2026-10-26 in Google Cloud
  project `Ballistics` (`ballistics-512818`, owner's account): consent screen
  External, app name Ballistics, support/developer contact the owner,
  authorised domain `ballistics.football`, Google API Services User Data
  Policy accepted by the owner; Web client "Clerk production" with redirect
  URI `https://clerk.ballistics.football/v1/oauth_callback`. **Publishing
  status: Testing** — publishing needs a home page and privacy policy URL,
  and there is no privacy page yet (TODO). Until then only listed test users
  (max 100) can sign in with Google. The owner copied the client ID and
  secret into Clerk (neither passed through Claude); Clerk's Google connection
  then showed "Used for sign-in" instead of "Setup required". Test users:
  `ayoabass777@gmail.com` (1/100).
- **Still to confirm in the smoke test:** that sessions work on
  `staging.`/`beta.ballistics.football` (subdomains of the primary domain),
  and the redirect URLs after sign-in/out.
- **Keys:** the publishable key is public; the secret key goes from Clerk
  straight into `deploy/.env.staging` by the owner.

## Server spec

Estimates from the local stack (2026-10-22) and the v1 server's container
memory. Engine peaks were measured on 2026-10-22 by running each job's
computation against the local database with the session forced read-only
(`default_transaction_read_only=on`; nothing was written), reading the
container's cgroup `memory.peak`:

| Job | Schedule | Time | Peak memory |
| --- | --- | --- | --- |
| Mismatch | nightly | 2 s | 182 MB |
| Bayesian (with analysis) | nightly | 28 s | 458 MB |
| Dixon–Coles | nightly | 45 s | 267 MB |
| Consensus (`--from-picks`, run_ds 2026-10-21, to its first write) | nightly | ~8 s | 385 MB |
| Calibration (`calibration_wf`, to its final report write) | weekly | 8 min | 589 MB |
| Postgres | always | | 11 MB anonymous + 140 MB shared now (`shared_buffers` 128 MB); its 7.9 GB peak is reclaimable file cache |

Models were run with no reuse cache (worst case). The consensus JSON export
adds a few hundred MB on top of its measured peak. dbt's work runs inside
Postgres.

| Item | Disk | RAM |
| --- | --- | --- |
| OS and packages | ~5 GB | ~0.3 GB |
| Docker images (Airflow 2.0, engine 1.3, dbt 0.5, frontend, two APIs, Postgres, Caddy), two releases kept | ~12 GB | |
| Build cache, pruned by the deploy script | ≤5 GB | |
| Postgres: 6.2 GB today, plus growth, WAL and vacuum headroom | ~15 GB | 1–2 GB |
| Airflow webserver and scheduler, logs kept 30 days (2.1 GB locally now) | ~3 GB | ~1 GB |
| Account API, research API, frontend, Caddy, dashboard server | | ~1 GB |
| Engine exports and results, 21 days | ~1.5 GB | |
| Nightly backup dump before upload | ~2 GB | |
| **At rest** | **~45 GB** | **~3.5–4.5 GB** |
| Engine peak: three models in parallel (~0.9 GB), or consensus, or calibration | | ≤1 GB extra (measured) |
| **Worst night** | | **~5.5–6 GB** with Postgres given 1 GB `shared_buffers` |

- **RAM: 8 GB is enough, 16 GB is headroom.** The measured engine peaks are
  far below the earlier guess; a bad night is about 5.5–6 GB. Add 4 GB of
  swap so a spike slows a job instead of killing it.
- **Disk: 80 GB works for now.** About 45 GB is used on day one (~56%), so
  the deploy script's image and build-cache pruning matters, and a disk
  upgrade is likely within a year as the head-to-head tables grow.
- **CPU: 4 or more vCPUs.** Shared vCPUs are fine.
- **Public IPv4: yes.** API-Football, Clerk and S3 are reached over IPv4.
- **Plan: Hetzner CAX21 (ARM), Helsinki** — 4 vCPU, 8 GB, 80 GB, $12.49 a
  month, $13.09 with IPv4 (console, 2026-10-22), chosen after the engine
  measurements above. The CX43 ($18.49) was out of stock in Helsinki,
  Nuremberg and Falkenstein; the CAX31 (8 vCPU, 16 GB, 160 GB, $25.59) is the
  upgrade path if memory or disk gets tight (a rescale that keeps the disk
  size can be reversed).
- **Server setup:** 4 GB swap file; Postgres `shared_buffers` 1 GB.
- **Created 2026-10-22:** `ballistics-v2`, 2.29.118.240, Ubuntu 24.04.4
  (aarch64, 4 vCPU, 7.5 GiB, 75 GB). SSH key `ballistics-v2`
  (`~/.ssh/ballistics_v2_ed25519`), host alias `ballistics-v2` in
  `~/.ssh/config` (user `sage`).
- **Base setup done 2026-10-22:** full upgrade and reboot (kernel
  6.8.0-147), unattended security upgrades on; 4 GB `/swapfile`,
  swappiness 10; Docker 29.9.2 + Compose v5.6.1 with container logs capped at
  3 × 20 MB; user `sage` (key login, passwordless sudo, `docker` group);
  SSH drop-in `/etc/ssh/sshd_config.d/01-hardening.conf` (no root login, no
  passwords, `AllowUsers sage`); UFW deny incoming except 22, 80, 443;
  fail2ban `sshd` jail (1 h ban after 5 failures in 10 min, owner's IP
  whitelisted in `/etc/fail2ban/jail.d/sshd.local`). Changes were applied
  under a 10-minute auto-revert that was cancelled only after a fresh `sage`
  login worked and root/password logins were refused. At rest after reboot:
  411 MiB RAM, 6.3 GB disk used. Docker bypasses UFW for published ports, so
  only Caddy may publish ports; everything else binds inside the server.
- **Cloudflare done 2026-10-22:** proxied A records `staging` and `beta` →
  2.29.118.240 (existing `api` → v1 DNS-only and the root/`www` v1 Worker
  untouched). Origin certificate (Cloudflare Origin CA, `*.ballistics.football`
  and apex, valid to 2041-10-18) issued from a CSR made on the server, so the
  private key never left it: `/etc/ballistics/certs/origin.key` (root:sage,
  640) and `origin.pem`; certificate and key verified to match. Copy or mount
  them as `deploy/certs/` at deploy. Zone SSL mode switched from Full to
  **Full (strict)** by the owner on 2026-10-22 (verified in the dashboard); it
  only affects proxied hosts with an origin, i.e. staging and beta.
- **ARM check (2026-10-22): passed.** The Mac is Apple Silicon, so the local
  Airflow, engine and dbt images were already arm64 and run nightly. Fresh
  `--platform linux/arm64 --pull --no-cache` builds of all six images
  succeeded: engine (scipy 1.17.1, statsmodels 0.15.0, scikit-learn 1.9.1,
  numpy 2.4.6 import), research API (10 routes), account API (33 routes),
  dbt 1.12.6, Airflow 2.9.3 with the Docker provider, and the frontend
  (`next start` served `/welcome` with 200). `postgres:16` and `caddy:2` have
  arm64 images. The only warning: `workerd` (Cloudflare Workers runtime, a
  dev dependency) fails its postinstall check on Alpine; it is not used when
  the frontend runs on the server.
- **At order time:** Ubuntu 24.04, this Mac's `First-Hetzner-Setup` SSH key,
  a firewall allowing only 22, 80 and 443. Hetzner's paid backups are
  optional; nightly dumps to S3 are already in the plan.

## Services

| Service | Source | Image | Exposed | Needs |
| --- | --- | --- | --- | --- |
| caddy | new `deploy/Caddyfile` | `caddy:2` | 80/443 | origin cert |
| consensus dashboard | `engine/eval/consensus_app.html` | `python3 -m http.server` | `127.0.0.1` only (SSH tunnel) | engine output folder, read-only |
| frontend | `ballistics/frontend_dev` | build from Dockerfile | via caddy | Clerk keys, API base URLs |
| account-api | `ballistics/API` | build from `Dockerfile` | internal | account DB user, Clerk secret, invite key and pepper, Saved archive bucket |
| research-api | `ballistics/engine/api` | new small Dockerfile (FastAPI + psycopg2) | internal | read-only DSN |
| postgres | | `postgres:17` (the local major; dumps don't restore into older majors) | internal | data volume |
| airflow-scheduler / webserver | `ballistics/airflow` | build from `airflow/Dockerfile` | SSH tunnel | Docker socket, `PROJECT_ROOT`, API key, AWS, SMTP |
| dbt, engine | `ballistics/dbt`, `engine/Dockerfile` | built on the server | none | started by Airflow |

The engine image tag `ballistics-engine:latest` is what the DAGs start; the
deploy script builds it from the same commit as the DAGs.

## Secrets

Rules: one `.env` per repo on the server, owner-only (`chmod 600`), never in
git or in `wrangler.jsonc`; master copies in a password manager; every
staging value is new (none reused from local or v1).

| Secret | Used by | Note |
| --- | --- | --- |
| `DB_PASSWORD` (owner), account DB user, research read-only user | postgres, Airflow, account API, research API | three roles: pipeline owner, `app`-only account user, read-only research user (`default_transaction_read_only`) |
| `API_KEY` (API-Football) | Airflow | shared quota, see decision 4 |
| AWS access key | Airflow, engine, account API | new IAM user limited to the staging buckets |
| `CLERK_SECRET_KEY`, `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY`, JWKS/issuer | frontend, account API | from the Clerk instance chosen in decision 3 |
| `BETA_INVITE_API_KEY`, `BETA_INVITE_PEPPER` | frontend, account API | new values; codes issued locally will not verify on staging |
| `AIRFLOW__WEBSERVER__SECRET_KEY`, Airflow admin password | Airflow | |
| SMTP user/password, `ALERT_EMAIL_TO` | Airflow alerts | an app password, not the account password |
| Cloudflare origin certificate and key | caddy | |

Staging buckets (separate, so DLQ replay on one environment never sees the
other's entries): raw landing, assets/crests (public read on `crests/*`),
forecast archive, Saved archive, backups.

Created 2026-10-22 by the owner in `ca-central-1` (same region as the local
buckets; the S3 traffic is nightly batch work, so the Helsinki–Canada
latency doesn't matter): `ballistics-stg-raw`, `-assets`, `-forecast-archive`,
`-saved-archive`, `-backups`. All encrypted (AES-256), ACLs disabled. Settings
applied with the owner's local credentials: **saved-archive** versioning on +
`DenyInsecureTransport` policy + full public-access block (the settings
`configure_saved_archive_bucket.py` applies; that script hard-codes its own
bucket name, so it was not run); **assets** public read on `crests/*` only,
public ACLs still blocked (checked anonymously: crests 200, other keys 403,
listing 403); **backups** expire after 14 days (incomplete uploads after 1).
IAM policy for the server's user: `deploy/iam-staging-policy.json` (no delete
on archives or backups, no bucket-level permissions).
IAM user `ballistics-staging` created by the owner 2026-10-26: only that
managed policy attached (`ballistics-staging-s3`, identical to the file), no
inline policies, groups or console login, one active access key (kept by
the owner; goes into `deploy/.env.staging`). IAM policy simulation: put and
delete allowed on raw, put allowed on backups; delete on backups and the
forecast archive, bucket-policy changes, any access to the local
`ballistics-raw` bucket, and IAM actions are all denied.

## Database and migrations

**Layout.** Airflow gets its own `airflow` database. The `ballistics`
database keeps `raw`, `dim`, `elo`, `forecast`, `data_detector`, dbt schemas
and `app`.

**Migration runner (built 2026-10-22).** `scripts/migrate.py` applies
numbered files in `db/migrations/` once each, in a transaction, recording
version and checksum in `public.schema_migrations` (an edited applied file is
refused; an advisory lock blocks concurrent runs). Rules are in
[db/migrations/README.md](../db/migrations/README.md).

- `0001_baseline.sql` is a schema-only dump of `raw`, `dim`, `elo`,
  `forecast`, `forecast_archive` and `data_detector` (36 tables, 3 views, the
  forecast immutability function and triggers, plus `pg_trgm`). It already
  contains everything the dated `migrations_*.sql`, `apply_*.sh` scripts and
  lazily created tables produced.
- **Changed from the original plan:** the `app` schema is *not* in the
  baseline. It stays owned by `ballistics/API/sql/bootstrap_accounts.sql`,
  where onboarding work is changing it now; the deploy runs it after the
  migrations. dbt models (including the nine in `raw`) are left to dbt.
- Verified on a throwaway PostgreSQL 17.4: `up` applies cleanly and a second
  `up` is a no-op; the resulting schema dump matches the local one; the
  account bootstrap runs on top; `dbt seed` + `dbt run` build all 61 models.
  The local database was baselined at 0001.
- **Deploy order:** `migrate.py up` → account bootstrap → `dbt build` →
  `deploy/grants.sql` → restart the APIs. A failed step stops the deploy
  before the APIs restart.

**Roles.** `deploy/postgres-init/01-roles.sh` (first start of an empty volume)
creates the `airflow` database and role, `account_api` and `research_reader`
(read-only by role default and by its DSN). `deploy/grants.sql`, re-run by
every deploy, grants: `research_reader` SELECT on `raw`, `raw_stg`,
`raw_int`, `raw_mart`, `dim`, `elo`, `forecast`; `account_api` DML on `app`
and SELECT on `raw`, `raw_mart`, `dim`, `elo`; default privileges so tables
dbt recreates stay readable. Checked on a fresh database from the compose
file: each allowed and denied case behaved as intended, grants are
re-runnable, and both APIs started and answered as their roles.

**Postgres version: 17.** The local database is 17.4, and a dump cannot be
restored into an older major, so staging uses `postgres:17` (not 16).

**Seeding staging data.** Restore from a `pg_dump` of the local database
rather than re-bootstrapping from API-Football (quota, and years of Elo and
forecast history that would take days to rebuild). Exclude Airflow metadata
(`public` Airflow tables) and the rows of `app` (staging users start empty;
keep the schema). The restored `raw.match_analysis` serves Match details
until the first staging engine run; no export files need copying. Then let
the DAGs run and check the next day's data is fresh.

Scripts (2026-10-22): `deploy/seed_from_local.sh` streams `pg_dump -Fc`
from the local container over SSH to `ballistics-v2:/var/lib/ballistics-seed/`
(nothing written to the laptop, whose disk is nearly full), excluding
`public.*` tables, the legacy `public_*` schemas and `app` rows.
`deploy/seed_restore.sh <dump>` runs on the server: starts only Postgres,
refuses if `raw` already has tables, restores as the owner, then baselines
0001. **First-deploy order:** check out the repos
(`deploy_staging.sh --checkout-only`) and fill `deploy/.env.staging` and
`deploy/certs/` → `seed_restore.sh` → the full deploy (which then applies
0002+). Tested locally by streaming into a throwaway PostgreSQL 17:
345 MB dump, 5 min 17 s end to end, no restore errors; row counts matched
the source for every pipeline table checked (150,536 fixtures, 274,958 Elo
rows, 69,289 forecast entries, 6,298 match analyses); `app` tables empty;
no `public` tables; the forecast append-only trigger still fires; `migrate.py
baseline 0001` then `up` applied 0002.

## Deploy, rollback and backups

- **Deploy script** (`deploy/deploy_staging.sh`, written 2026-10-22, run
  from the laptop): refuses refs that are not on an origin branch, pushes the
  Ballistics commit over SSH into `/srv/ballistics` (the server holds no
  GitHub credentials), then runs the script as committed at that SHA on the
  server: stop Airflow (it bind-mounts code from the
  checkout) → check out → render `.dbt/profiles.yml` → build every image
  tagged `b<sha>` → Postgres → `migrate.py up` → account bootstrap →
  `dbt build` → `grants.sql` → tag engine/dbt `:latest` (what the DAGs start)
  → `docker compose up -d --wait` → append to `deploy/RELEASES` → prune
  release images outside the last two, dangling images and week-old build
  cache. A failed step names itself and leaves Airflow stopped and nothing
  recorded. `--status` prints `RELEASES`; `--checkout-only` is for the first
  deploy (above). Pre-flight: `.env.staging` must be `chmod 600` with
  matching paths, and `deploy/certs/origin.{pem,key}` must exist (a symlink
  to `/etc/ballistics/certs` works). CI can come after beta.
- **Rollback:** `deploy_staging.sh --rollback` redeploys the line before the
  last in `RELEASES` (images are rebuilt, mostly from cache). It appends a
  line too, so a second rollback goes back to the release that was rolled
  back. Migrations are forward-only, so a migration that must be undone is
  undone by a new migration; a bad data change is undone from the backup.
- **Backups:** nightly `pg_dump -Fc` to the backups bucket, 14 days kept, plus
  weekly Hetzner snapshots. A restore into a scratch database is part of the
  smoke test; an untested backup does not count.
- **Exports and the consensus dashboard:** the beta site does not read the
  per-run JSON, but the internal dashboard `engine/eval/consensus_app.html`
  does: it loads the newest `consensus_slips_<ds>.json` and
  `consensus_results_<ds>.json` from its own folder, looking back 21 days.
  Local daily DAGs stop at the move (decision 4), so the dashboard moves to the
  server: a `python3 -m http.server` bound to `127.0.0.1` on the engine output
  folder, opened from the Mac through an SSH tunnel like the Airflow UI. Keep
  21 days of both file types on the server (about 1.5 GB at the current
  60–80 MB per run) and upload a gzipped copy of each export (about 4 MB) to
  the backups bucket as the archive.

## Monitoring and alerts

- Existing data-flow alerts (`ALERT_EMAIL_TO`, daily report) stay on.
- External uptime check (for example UptimeRobot or Better Stack free tier)
  on `staging.ballistics.football` and on a frontend route that calls both
  APIs.
- Dead-man's switch (for example Healthchecks.io): `fixture_update` and
  `engine_predict` ping on success, so a DAG that silently stops running
  raises an alert (the July plan-lapse lesson).
- Disk alert at 80% (the database and exports are the growth risks).

## Work breakdown

In order; estimates are focused days for one developer.

| # | Task | Days |
| --- | --- | --- |
| 1 | Frontend and account API source moved into Ballistics; commit and push the consolidated tree before staging deploy. | 0.5 |
| 2 | ✅ 2026-10-22: migration runner and baseline (`scripts/migrate.py`, `db/migrations/`), verified on a throwaway database | 1–1.5 |
| 3 | ✅ 2026-10-22: `engine/api/Dockerfile`; `deploy/compose.staging.yml`, `Caddyfile`, `postgres-init/01-roles.sh`, `grants.sql`, `dbt-profiles.yml.tmpl`, `staging.env.example`. Validated: compose renders, only Caddy publishes public ports, Caddyfile valid, research API served real data read-only, roles/grants and both APIs checked on a fresh database. Not yet run: Airflow and the frontend from the compose file | 1–1.5 |
| 4 | Order the CAX21; Caddy, DNS, firewall, origin cert; staging buckets and IAM user; Clerk production instance; secrets filled in | 1 |
| 5 | Seed data, first deploy, first full daily cycle (fixture update → dbt → engine → API) | 1 |
| 6 | Backups and restore test, uptime and dead-man's switch, disk alert | 0.5–1 |
| 7 | Checkpoint 3 smoke test (below) and fixes | 1–2 |

Total about 6–8.5 focused days, which keeps the 10–24 Nov beta window if it
starts this week and runs alongside onboarding and the Match details items.

After v2 is live at the root domain: stop v1's containers and pipeline, keep
a final dump of its database in the backups bucket, remove its DNS record,
then remove its containers and volumes.

### Open items found while writing the deploy files (2026-10-22)

- **Frontend build arguments (resolved locally 2026-10-22).** The migrated
  `frontend_dev/Dockerfile` has the `ARG`/`ENV` lines needed for Clerk's
  `NEXT_PUBLIC_*` build values. An ARM64 image built successfully before the
  move; the relocated source passed `next build` locally. The full staging
  Compose build and deployment remain untested.
- **Logo URLs (frontend repo).** `lib/logo-asset-url.ts` builds browser image
  URLs from `BACKEND_API_BASE_URL`, which on the server is the internal
  `http://account-api:8000`. The Caddyfile routes `/assets/logos/*` to the
  account API as a stopgap; the lasting fix is a separate public base URL
  (or a relative path) for logos.
- ✅ **Deploy script** (2026-10-22): `deploy/deploy_staging.sh`, described
  under "Deploy, rollback and backups". Tested on the Mac against a scratch
  "server" folder, a scratch origin and an SSH shim, as a separate compose
  project (`ballistics-deploytest`, own network and ports): the guards
  refused an unpushed ref, a bad ref, a rollback with no releases and an env
  file that was not `chmod 600`; a full run built all images, applied 0001,
  ran the account bootstrap, `dbt build` passed on the empty schema, grants
  applied (`research_reader` reads `raw_mart`), and the stack came up behind
  Caddy (`/` 200 over TLS, research API answering). A failure in the start
  step reported itself and recorded no release. `--checkout-only`, `--status`,
  the rollback target and the pre-consolidation prune filter (only
  `b<sha>-a<sha>` tags outside the last two releases) were checked. The new
  single-commit release format needs a fresh deployment rehearsal. Not
  covered locally: one uninterrupted
  run ending in `RELEASES`, a real prune, and a rollback through to the end.
  Docker Desktop's registry proxy on the Mac hung partway through, blocking
  pulls and builds, so the start step was run by hand from the built images.
  Run these on the server's first deploy.
- **League config not in git (fixed 2026-10-22, uncommitted).** `/data/` was
  gitignored, so a clean checkout had no `data/metadata.yaml` (read by the
  bootstrap DAG and `extract_metadata`), and the engine image's
  `COPY data/` failed. `.gitignore` now keeps `data/` ignored except
  `metadata.yaml`, and the deploy creates `data/` and stops if the config is
  missing.
- **Airflow admin password on the command line.** `airflow-init` passes
  `--password ${AIRFLOW_ADMIN_PASSWORD}` in its command, so it shows in
  `docker top` and `ps` on the server while the container runs. The first
  start's `airflow users create` took about 20 minutes on the Mac.
- **Airflow executor.** The compose file keeps Airflow's default executor,
  as locally, so tasks run one at a time; the measured peaks assume no more
  than that.

## Checkpoint 3 smoke test

- Fresh invite: issue a code on staging → claim → Clerk invite email → sign up
  with Google and with email (D151) → onboarding → Discover.
- Core journey on desktop and mobile: Discover → Match (before and after
  kickoff, D152) → Save → Saved shows status updates → sign out; signed-out
  `/discover` redirects to sign-in; signed-out API calls return 401.
- Data freshness: the next morning, Discover's latest run date is today's
  and Match details has no stale-data badge.
- Failure: break one DAG task on purpose and confirm the alert arrives; stop
  the research API and confirm the uptime alert.
- Rollback: deploy a previous SHA and back again.
- Restore: restore last night's backup into a scratch database and query it.

## Owner decisions (D153, 2026-10-21)

1. **Server:** first "reuse the v1 server, after v2 i would take v1 down".
   Revised the same day after the inspection showed v1's server is too small
   (3.7 GB RAM, 91% disk) and resizing costs more (~$141 USD/month) than a new
   EU CX43 (~€16) plus v1's current $23 USD until v1 is retired. v2 runs on
   a new EU server; the CX43 was out of stock on 2026-10-22, so after an ARM
   build check and measured engine peaks (worst night ~5.5–6 GB) the CAX21
   (ARM, 4 vCPU, 8 GB, 80 GB, $13.09 with IPv4) was chosen. v1 stays on its server until it is taken down.
2. **Frontend:** on the server with `next start`, not Cloudflare Workers.
3. **Clerk:** create the production instance on `ballistics.football` now and
   use it for staging and beta.
4. **API-Football quota:** stop the local daily DAGs once the server runs
   them. v1 uses the same key (checked 2026-10-21), so v1 and v2 share 7,500
   requests a day until v1 is retired.
5. **URLs:** `staging.ballistics.football` for the smoke test,
   `beta.ballistics.football` for testers, the root domain once v1 is down.
