# Staging runbook

How to operate the v2 staging/beta server day to day. How it was built and
why is in [staging-deployment-plan.md](staging-deployment-plan.md); open work
is in [TODO.md](../TODO.md) (beta blockers). Current as of 2026-10-28.

## At a glance

| What | Where |
| --- | --- |
| Public URLs | `https://beta.ballistics.football` (testers), `https://staging.ballistics.football` (same stack) |
| Server | Hetzner CAX21 `ballistics-v2` (ARM, 8 GB, Helsinki), 2.29.118.240, Ubuntu 24.04, UTC |
| SSH | `ssh ballistics-v2` (user `sage`, key `~/.ssh/ballistics_v2_ed25519`) |
| Checkout | `/srv/ballistics` on the server; compose project `ballistics`, network `ballistics_network` |
| Secrets | `/srv/ballistics/deploy/.env.staging` on the server only (mode 600; template `deploy/staging.env.example`) |
| Release history | `/srv/ballistics/deploy/RELEASES` (`deploy/deploy_staging.sh --status`) |
| Current release | `ba7fbc77773` (2026-10-28) |
| Backups | `s3://ballistics-stg-backups/postgres/<day>/`, nightly 03:30 UTC, 14 days |
| Alerts | Slack `#ballistics-alerts` (Ballistics workspace, webhook in `ALERT_SLACK_WEBHOOK_URL`) and email to `ALERT_EMAIL_TO` via Gmail SMTP (the owner's inbox) |

Request path: tester → Cloudflare (DNS, proxy, TLS) → Hetzner Cloud Firewall
→ Caddy (Cloudflare origin certificate) → `frontend` (Next.js) → `account-api`
(port 8000) and `research-api` (port 8001) → Postgres. Caddy sends
`/assets/logos/*` straight to the account API (the browser loads logos from
there); everything else goes to the frontend. Requests for any other host,
or the bare IP, are aborted.

### Services (`deploy/compose.staging.yml`)

| Service | Purpose | Reachable |
| --- | --- | --- |
| `caddy` | TLS and routing | 80/443, Cloudflare only |
| `frontend` | Next.js site, Clerk sign-in | inside the network |
| `account-api` | accounts, invites, Saved, preferences (`API/`) | inside the network |
| `research-api` | Discover, Matches, Match details (`engine/api/`), read-only DB role | inside the network |
| `postgres` | Postgres 17: `ballistics` and `airflow` databases | inside the network |
| `airflow-webserver`, `airflow-scheduler` | the pipeline DAGs; run dbt and the engine as containers via the Docker socket | UI on `127.0.0.1:8080` |
| `dashboard` | internal consensus dashboard (`engine/eval`) | `127.0.0.1:8787` |
| `engine`, `dbt`, `airflow-image` | build-only images (profile `build`) | — |

### Outside the server

| What | Where | Notes |
| --- | --- | --- |
| DNS, proxy, origin certificate | Cloudflare, zone `ballistics.football` | `beta`/`staging` proxied; origin cert valid to 2041-10-18, key only on the server (`/etc/ballistics/certs`) |
| Email | Cloudflare Email Routing | `privacy@ballistics.football` → owner's Gmail; every other address is dropped (catch-all: Drop). Receive only: replies come from Gmail |
| Firewall | Hetzner Cloud Firewall `ballistics-web` | 80/443 from Cloudflare ranges only; 22 and ICMP from anywhere |
| Sign-in | Clerk production instance (`ballistics.football`), invite-only | email code and Google |
| Google sign-in | Google Cloud project `ballistics-512818`, OAuth client "Clerk production" | consent screen published (In production) 2026-10-28; home page `/welcome`, privacy `/privacy`, no logo (a logo triggers brand review) |
| Storage | S3 `ca-central-1`, IAM user limited to the staging buckets | see below |

S3 buckets: `ballistics-stg-raw` (landing zone and DLQ),
`ballistics-stg-assets` (team crests, public read), 
`ballistics-stg-forecast-archive`, `ballistics-stg-saved-archive`,
`ballistics-stg-backups` (objects expire after 14 days; the IAM user can put
and get but not delete).

## Shortcuts

On the server, every compose command needs the file, the env file and the
current release: images are tagged `b<sha>` and no `:latest` app images
exist, so without `RELEASE` compose tries to pull images that aren't there.

```bash
ssh ballistics-v2
cd /srv/ballistics
dc() { RELEASE="$(awk 'END{print $2}' deploy/RELEASES)" docker compose -f deploy/compose.staging.yml --env-file deploy/.env.staging "$@"; }
dc ps
```

The rest of this page writes `dc` for that. Never `dc build` or `dc up
--build` by hand: the deploy builds and records releases.

## Deploy and roll back

Run from the Mac, in the repo. The script deploys only commits already on
origin, so push first.

```bash
deploy/deploy_staging.sh                  # deploy origin/main
deploy/deploy_staging.sh --status         # list RELEASES
deploy/deploy_staging.sh --rollback       # redeploy the previous release
```

A deploy takes about 10–15 minutes. Order: stop Airflow → check out → build
images tagged `b<sha>` → migrations (`scripts/migrate.py up`) → account
bootstrap → `dbt build` → grants → tag engine/dbt `:latest` → start
everything → record `RELEASES` → prune old images. If a step fails, it says
which one, Airflow stays stopped and nothing is recorded: fix and deploy
again, or roll back. A `dbt build` test failure stops the deploy before the
new images go live.

- **Timing:** avoid 00:00–01:00 UTC (nightly cycle) and 03:30 UTC (backup).
  A deploy stops Airflow, so a DAG run that was due resumes when it restarts.
- **Rollback** rebuilds the previous commit's images. Migrations are
  forward-only: undo a schema change with a new migration, and bad data from
  a backup.
- **After a deploy**, check:

  ```bash
  curl -s -o /dev/null -w '%{http_code}\n' https://beta.ballistics.football/welcome    # 200
  curl -s -o /dev/null -w '%{http_code}\n' https://beta.ballistics.football/privacy    # 200
  ssh ballistics-v2 'cd /srv/ballistics && docker compose -f deploy/compose.staging.yml --env-file deploy/.env.staging exec -T airflow-scheduler airflow dags list-import-errors'
  ```

  `curl` without browser headers gets 404 on signed-in pages (Clerk rewrites
  signed-out non-browser requests); a browser is redirected to `/sign-in`.

## Daily schedule (UTC)

| Time | What | Notes |
| --- | --- | --- |
| hourly | `replay_dag` | retries DLQ entries an hour after their last attempt |
| 00:00 | `bootstrap_dag` | skips unless `data/metadata.yaml` changed |
| 00:00 | `fixture_update_dag` | fixtures, new fixtures, corrections, standings, then dbt |
| 00:00 | `engine_predict` | waits for `fixture_update_dag`, then the models and consensus |
| 03:30 | backup (cron, not Airflow) | `deploy/backup_postgres.sh` |
| 04:15 | `saved_snapshot_maintenance` | paused (left off at cutover) |
| 05:45 | `forecast_history_archive` | copies forecast history to S3; deletes nothing |
| 09:00 | `data_flow_check` | daily report email |
| weekly | `engine_calibration` | |

## Alerts and what to do

| Email subject | Meaning | First step |
| --- | --- | --- |
| `✓ Ballistics <date>: all clear` | daily report, nothing wrong | none |
| `! Ballistics <date>: N warnings` | a check below its threshold, non-blocking | read the report; look again tomorrow |
| `✗ Ballistics <date>: N failed, …` | a dataset failed or is missing | open the Airflow UI, find the failed task, read its log |
| `✓ Ballistics <date>: recovered` | yesterday's failure cleared | none |
| `✗ Ballistics DLQ: <kind> gave up after N retries` | an extraction failed 24 hourly retries | check the API-Football quota/plan and the entry in `s3://ballistics-stg-raw/dlq/` |
| `! Ballistics <dataset>: N unprocessable row(s) parked` | rows the loader couldn't use were set aside | look at the parked rows; usually an API quirk |
| `[ballistics staging] backup FAILED: <step>` | the nightly backup stopped at that step | read `/var/lib/ballistics-backup/backup.log`; run the backup by hand once fixed |

No daily report by about 09:30 UTC means the scheduler, the server or SMTP
is down: there is no missed-run alert yet (TODO).

## Airflow and the dashboard

Both listen only on the server's localhost. Open a tunnel from the Mac:

```bash
ssh -N -L 8080:127.0.0.1:8080 -L 8787:127.0.0.1:8787 ballistics-v2
```

Then Airflow is at http://localhost:8080 (user `admin`, password
`AIRFLOW_ADMIN_PASSWORD` from `.env.staging`) and the consensus dashboard at
http://localhost:8787/consensus_app.html.

From the server shell:

```bash
dc exec airflow-scheduler airflow dags list-runs -d fixture_update_dag -o plain | head
dc exec airflow-scheduler airflow tasks states-for-dag-run fixture_update_dag <run_id>
dc exec airflow-scheduler airflow dags trigger <dag_id>
dc exec airflow-scheduler airflow dags pause <dag_id>      # or unpause
```

DAGs that are new to Airflow start paused.

## Logs

```bash
dc logs --tail 200 -f frontend            # or account-api, research-api, caddy, airflow-scheduler
sudo ls /var/lib/docker/volumes/ballistics_airflow-logs/_data/   # Airflow task logs (also in the UI)
tail -n 50 /var/lib/ballistics-backup/backup.log                 # backups
```

Container logs are capped at 3 × 20 MB per container.

## Database

```bash
dc exec postgres psql -U ballistics -d ballistics      # owner role (pipeline, dbt, migrations)
dc run --rm --no-deps -T -e CONNECTION_CHECK_MAX_COUNT=0 -e DB_HOST=postgres \
  -v /srv/ballistics/scripts:/opt/airflow/scripts:ro -v /srv/ballistics/db:/opt/airflow/db:ro \
  airflow-scheduler python /opt/airflow/scripts/migrate.py status   # the deploy runs `up` the same way
```

Roles: `ballistics` (owner), `airflow` (Airflow metadata DB), `account_api`
(app schema), `research_reader` (read-only, used by the research API). Grants
are in `deploy/grants.sql` and reapplied on every deploy. Schema changes go in
`db/migrations/` as numbered files; the deploy applies them.

## Backups and restore

Nightly at 03:30 UTC from `sage`'s crontab. Dumps `ballistics` and `airflow`
with `pg_dump -Fc`, checks each file, uploads to
`s3://ballistics-stg-backups/postgres/<day>/`, compares sizes, keeps only the
newest dump in `/var/lib/ballistics-backup`, and emails on any failure.

```bash
/srv/ballistics/deploy/backup_postgres.sh                     # back up now
/srv/ballistics/deploy/backup_postgres.sh restore-test        # restore newest into a scratch DB and compare counts
/srv/ballistics/deploy/backup_postgres.sh restore-test 2026-10-28
/srv/ballistics/deploy/backup_postgres.sh install-cron        # reinstall the cron line
```

Run a restore test after any change to the backup script, and at least once
before inviting testers. Live counts can be ahead of the backup (activity
since 03:30); they should never be behind.

**Restoring for real** (new or rebuilt server): set up the server as in the
plan doc → `deploy/deploy_staging.sh --checkout-only` → fill `.env.staging`
(the postgres-init script recreates the roles) → download the dump from the
backups bucket → `deploy/seed_restore.sh <dump>` → full deploy. Data written
after the last backup is lost (up to a day).

Hetzner weekly snapshots of the whole server: not enabled yet (owner,
console → server → Backups).

## Beta invites

Issue a single-use, email-bound code on staging (codes made locally won't
verify here):

```bash
dc exec account-api python -m API.issue_beta_invite person@example.com            # valid 14 days
dc exec account-api python -m API.issue_beta_invite person@example.com --days 30  # 1–30
```

The tester claims the code, Clerk sends the sign-up email, and they sign up
with that address (by email code or Google). Tell Google users to pick the
Google account the invite went to.

## Secrets

All live in `deploy/.env.staging` on the server; master copies belong in the
password manager. Never paste them into chat or commit them.

To change one: `ssh ballistics-v2`, `nano /srv/ballistics/deploy/.env.staging`,
then recreate what uses it (`dc up -d --no-build <service>`; the Airflow
services share one block, so name `airflow-webserver airflow-scheduler`), or
redeploy. A changed
`CLERK_PUBLISHABLE_KEY` needs a deploy (it is built into the frontend). A
random value can be generated on the server without printing it:

```bash
python3 -c 'import secrets; print(secrets.token_urlsafe(48))'
```

| Secret | Used by | Rotating it |
| --- | --- | --- |
| `DB_PASSWORD` and the three service-role passwords | Postgres, services | also `ALTER ROLE … PASSWORD` in Postgres; the init script only runs on an empty volume |
| `AIRFLOW_SECRET_KEY` | Airflow webserver | restart Airflow; signs out UI sessions (rotated 2026-10-27) |
| `API_FOOTBALL_KEY` | DAGs | shared quota with v1 until v1 is retired |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | DAGs, account API, backups | new key in IAM, then delete the old one |
| `CLERK_SECRET_KEY` / `CLERK_PUBLISHABLE_KEY` | frontend, account API | from the Clerk production instance |
| `BETA_INVITE_API_KEY` / `BETA_INVITE_PEPPER` | frontend, account API | changing the pepper invalidates unclaimed codes |
| `SMTP_PASSWORD` | alerts, backups | a Gmail app password |

## Firewall and Cloudflare

The Hetzner Cloud Firewall is managed from the Mac with
`deploy/hetzner_firewall.sh` (needs an `hcloud` context with a Read & Write
API token: `hcloud context create ballistics`). It fetches Cloudflare's IP
ranges each run, so rerun it if Cloudflare publishes new ones
(https://www.cloudflare.com/ips/). UFW on the server also allows only 22, 80
and 443, but Docker's published ports bypass UFW, so the Hetzner firewall is
what keeps web traffic on Cloudflare. Only Caddy may publish ports; anything
else binds to `127.0.0.1`.

Check that the origin refuses direct traffic:

```bash
curl -sk -o /dev/null -w '%{http_code}\n' --max-time 8 \
  --resolve beta.ballistics.football:443:2.29.118.240 https://beta.ballistics.football/welcome   # 000 (timeout)
```

If SSH ever stops answering after a firewall change, use the Hetzner
console's web terminal or detach the firewall there.

## Disk

75 GB disk, 52% used on 2026-10-28; the largest parts are Docker build cache
(about 14 GB, mostly reclaimable), images (12 GB) and the Postgres volume
(7.2 GB). Deploys prune dangling images and build cache older than a week. To free space by
hand:

```bash
df -h /
docker system df
docker builder prune -f --filter until=72h
```

No disk alert yet (TODO).

## Open operational items

- Missed-run alert (dead-man's switch) for `fixture_update_dag`,
  `engine_predict` and the backup.
- External uptime check on `beta.ballistics.football`.
- Disk alert at 80%.
- Hetzner weekly snapshots (owner, console).
- `saved_snapshot_maintenance` is still paused.
