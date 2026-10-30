# Account schema bootstrap

## Beta onboarding rollout (2026-10-21)

`app.app_users.onboarding_completed` already exists and defaults to false. The local onboarding flow now uses it to route first-time signed-in members to `/onboarding`, then atomically saves optional followed leagues and marks the account complete through `POST /account/onboarding/complete`. Before enabling this flow in another environment, grandfather accounts that existed before rollout in a one-time transaction, using the rollout timestamp as a bound: `UPDATE app.app_users SET onboarding_completed=TRUE, updated_at=NOW() WHERE onboarding_completed=FALSE AND created_at < :rollout_at`. Do not place this update in repeatable bootstrap SQL. The two existing local account rows were grandfathered on 2026-10-21. New accounts remain incomplete until they finish or skip. Browser verification of the new flow is pending.

## Beta invitation codes (D147, 2026-10-21)

`bootstrap_accounts.sql` now includes `beta_invite_codes.sql`. It was applied to the local account database on 2026-10-21; apply it to each other environment before issuing codes. Set the same `BETA_INVITE_API_KEY` in the account API and Next.js server environments, and set a separate random 32+ character `BETA_INVITE_PEPPER` only in the account API environment. Both local services were restarted with these env settings on 2026-10-21. Restart them after later env changes. Never commit or log these values.

From the repository root, issue an email-bound code with `python -m API.issue_beta_invite member@example.com`. Deliver that code to the intended member through a private channel. The code expires after 30 days by default; `--days` accepts 1–30. The public Welcome form asks for the code and matching email, then requests a Clerk invitation email. Its link leads to Clerk account creation. An issued code cannot be reused after Clerk accepts the invitation request. If Clerk accepted a request but the database update failed, reconcile the invitation and code manually rather than retrying with `ignore_existing`.

In the Clerk Dashboard, set **Access mode → Invite-only** and configure email/password as the allowed account method before inviting anyone. This is required: the Welcome form does not itself close Clerk's direct signup route. A signed-out browser visit to `/sign-up` displayed Clerk's Access restricted screen on 2026-10-21. Google remained visible on sign-in, so email-only configuration is still open. Test a wrong/expired/used code, a valid code for the intended inbox, account creation from the emailed link, and member access after signup. The local migration, rolled-back claim/reuse database check, and invalid-code/cross-origin HTTP smoke checks passed on 2026-10-21. A read-only Clerk API check isolated Cloudflare 1010 to Python `urllib`; the same endpoint and key returned HTTP 200 with `httpx`, which the invitation client now uses. On 2026-10-21 a real invitation to `abassayo777@gmail.com` was accepted and separately listed by Clerk as pending (`inv_3L1X2aYMAcFwrDEy3PKGc6Ty0ea`); inbox delivery and signup remain unverified.

2026-10-20 billing update (D143): the bootstrap now ends with `billing.sql`: `app.billing_customers` (user_id PK → app.app_users, unique stripe_customer_id), `app.subscriptions` (Stripe subscription mirror, written only by the webhook handler; status as sent by Stripe), `app.stripe_events` (event_id PK for webhook idempotency) and the `app.user_access` view, the single paywall source: has_access is true when a subscription is `active` or `trialing` and its current_period_end is null or in the future. `app.app_users.plan` is legacy and not read by the paywall. All tables cascade on account delete. Verified on a throwaway PostgreSQL 16 (bootstrap applied twice). Test: `BILLING_TEST_DATABASE_URL='postgresql+psycopg2:///your_disposable_db' python -m pytest API/tests/test_billing_postgres.py -q`. Not applied to the account database; checkout and webhook routes not built. Statements below that subscription tables remain planned are superseded.

2026-10-18 account data update: the bootstrap now also includes `user_account_data.sql` (`app.saved_fixtures`) and `user_followed_leagues.sql` (`app.user_followed_leagues`) after `app.app_users`. Saved records use `(user_id, fixture_id)` as the key and retain the first card snapshot on repeated save/import. Followed leagues use `(user_id, league_id)` and a foreign key to `dim.dim_leagues`; the league catalog must exist before bootstrap. Both tables cascade when the local account is deleted. API routes require verified Clerk claims and perform no request-time DDL. `app.saved_fixtures` was applied to the configured local account database on 2026-10-18; `app.user_followed_leagues` was already present. A synthetic PostgreSQL transaction verified Saved create, frozen snapshot, delete, and league isolation, then rolled back. This update is local and not deployed. Historical statements below about absent follow tables or unavailable account connectivity are superseded by this update.

Updated 2026-10-18. These are application-account tables, separate from the football analytics pipeline and dbt models. Run against the configured **account database**, not an assumed analytics database.

## Ordered, repeatable bootstrap

From the repository root, with PostgreSQL connection settings supplied securely through your normal environment or service configuration:

```sh
psql -X --set=ON_ERROR_STOP=1 --file=API/sql/bootstrap_accounts.sql
```

The script runs a transaction, stops on errors and creates objects in dependency order:

1. `app` schema.
2. `app.app_users` (existing account schema from `API/app.py`).
3. `app.user_preferences` via `user_preferences.sql`.

The executing role needs permission to create these objects. Runtime API access requires schema usage and SELECT/INSERT/UPDATE on these tables. No passwords, Clerk secrets or user records belong in bootstrap SQL. No users or preferences are seeded.

`CREATE ... IF NOT EXISTS` makes repeated creation safe; it does **not** repair or migrate an existing table with different columns or constraints. Future schema changes require explicit migrations. Keep the app_users definition aligned with the existing API helper until request-time DDL is replaced by a migration lifecycle.

## Implemented schema

### app.app_users

`user_id TEXT PRIMARY KEY` is the Clerk user ID (`sub`), not a session ID. Existing fields: nullable `email`, `first_name`, `last_name`, `image_url`; non-null `auth_provider` default `clerk`, `plan` default `free`, `onboarding_completed` default false; non-null `created_at` and `updated_at` timestamps with time zone default now. No duplicate user_profiles table is required.

### app.user_preferences

| Column | Type and constraint | Meaning |
| --- | --- | --- |
| user_id | TEXT PRIMARY KEY; FK to app.app_users(user_id), ON DELETE CASCADE | One preference record per account |
| timezone_override | TEXT NULL | IANA timezone; NULL means use the current browser timezone |
| created_at | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Updated explicitly by the API upsert |

The primary key supplies the user lookup index. Timezone validation happens in `API/preferences.py`, not a database CHECK. Direct SQL writers must honor that contract. Reading a user without a preferences row returns null defaults; POST creates or updates it. API ownership comes from verified Clerk claims, never a request-supplied user ID. GET is read-only. POST inserts the local account identity if missing and upserts preferences in the same transaction; failure rolls back both. Preferences routes never execute DDL. Cascading deletion applies when the local account row is deleted; Clerk account-deletion synchronization is not implemented by this script.

## Verification

```sql
SELECT table_name, column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'app'
  AND table_name IN ('app_users', 'user_preferences')
ORDER BY table_name, ordinal_position;

SELECT conrelid::regclass AS table_name, conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid IN ('app.app_users'::regclass, 'app.user_preferences'::regclass);
```

Confirm the preferences primary key, account foreign key and ON DELETE CASCADE. API tests: `python -m pytest API/tests/test_preferences.py -q` (isolated mocked-storage tests, not a migration test).

## Delivery status and future tables

SQL and API source exist. Eleven isolated API tests and one PostgreSQL storage test passed. The bootstrap was applied twice to an isolated temporary PostgreSQL instance; persistence, reset to null, user isolation, preserved creation timestamps, foreign-key rejection and cascade deletion passed. The configured account database was unreachable on 2026-10-18, so this bootstrap has **not been applied to the account database**. It is not wired into Airflow or production deployment.

Followed leagues, followed teams, dedicated onboarding state and subscription tables remain planned. Add each approved schema to this bootstrap in dependency order when implemented; do not create placeholder tables from proposed designs.

## Frontend and API contract

`GET /preferences` returns `{ "data": { "timezone_override": null, "created_at": null, "updated_at": null } }` for an account with no saved row. Existing rows return ISO timestamps. `POST /preferences` accepts exactly `{ "timezone_override": "Europe/London" }` or `{ "timezone_override": null }`. Missing field, unknown field, or invalid zone returns 422; missing/invalid authentication returns 401; database failure returns a sanitized 503. Successful responses use Cache-Control: no-store. The server installs tzdata for hosts without a system timezone database.

Next.js `/api/preferences` forwards the Clerk session token to `ACCOUNT_API_BASE_URL` (fallback `BACKEND_API_BASE_URL`), never the research API. Configure this server-only URL to the account FastAPI origin. POST requires a matching Origin; upstream redirects are refused. Responses are validated and uncached; upstream error details and tokens are not exposed.

`/settings`, linked in the research header, offers Automatic/browser timezone or an explicit zone. Save success is shown only after API confirmation; load/save failures have retry/error states. Preferences are isolated by signed-in user and reset when the account changes. The override controls Discover/Standouts/Matches request date boundaries and research fixture/update timestamps, including Match details and Saved. Browser-local Saved storage itself is unchanged. Forecast record and legacy dashboard timezone integration are outside this change.

Tests:

```sh
python -m pytest API/tests/test_preferences.py -q
# Only against an explicitly disposable, already-bootstrapped database:
PREFERENCES_TEST_DATABASE_URL='postgresql:///your_disposable_db' python -m pytest API/tests/test_preferences_postgres.py -q
cd frontend_dev
node scripts/test-preferences.cjs
npm run typecheck:research
```

Live account save/reload verification remains pending account database connectivity. The real frontend's unavailable-account error/retry state was checked; successful proxy paths and timezone date boundaries were tested with isolated fixtures. No persistent browser fallback pretends that an unavailable save succeeded.

2026-10-18 database diagnosis correction: PostgreSQL at localhost:5432 is reachable and the ballistics_postgres container is healthy. A connection attempt outside the filesystem/network sandbox reached PostgreSQL but failed password authentication using the original account API configuration. Earlier generic OperationalError/unreachable wording did not distinguish sandbox denial from the underlying credential mismatch. No credentials were displayed or changed; account bootstrap is still unapplied.


2026-10-18 — Account database connection repaired at owner request. Verified existing Ballistics PostgreSQL credentials, then set gitignored ACCOUNT_DB_* overrides for the original API without changing legacy DB_* pipeline settings. API/core/database.py now loads its project .env explicitly, uses SQLAlchemy URL.create (safe credential encoding), a five-second connect timeout and pool pre-ping. Account bootstrap applied successfully to the configured running PostgreSQL database: app.app_users and app.user_preferences columns inspected, preferences foreign key ON DELETE CASCADE confirmed. Account API started on 127.0.0.1:8000. Eleven isolated API tests passed. Earlier unavailable/password-failure notes are superseded for database connectivity; browser authentication verification tracked separately. Credentials not recorded in documentation or Git.

Connection configuration: API/core/database.py prefers ACCOUNT_DB_USER, ACCOUNT_DB_PASSWORD, ACCOUNT_DB_HOST, ACCOUNT_DB_PORT and ACCOUNT_DB_NAME, falling back to their DB_* equivalents. Bootstrap psql must use these resolved values via PG* environment variables. Keep credentials in the gitignored environment file; never copy them into SQL or commands committed to Git.

2026-10-18 live verification: real signed-in localhost settings GET succeeded through Clerk → Next.js proxy → account API → PostgreSQL. Saved Europe/London, reloaded and confirmed it persisted, then restored Automatic/browser timezone and confirmed save success. Screenshot: docs/product/mock/preferences-live-2026-10-18.png in Ballistics repo. This supersedes the pending live-account verification note above. Local only; no deployment or push.


## Followed leagues — implemented 2026-10-18

Canonical table: `app.user_followed_leagues` (the earlier workspace draft `app.followed_leagues` was not applied). Bootstrap dependency order: existing metadata `dim.dim_leagues` → `app.app_users` → `user_followed_leagues.sql`. Run the metadata pipeline first on an empty database. Existing account bootstrap includes this file after account tables. The separate saved-fixtures file is pre-existing workspace work, not evidence of Saved rollout in this change.

| Column | Definition |
| --- | --- |
| user_id | TEXT, FK app.app_users(user_id), ON DELETE CASCADE |
| league_id | BIGINT, FK dim.dim_leagues(league_id), ON DELETE RESTRICT |
| created_at | TIMESTAMPTZ NOT NULL DEFAULT NOW() |

Composite primary key `(user_id, league_id)` prevents duplicates and indexes account lookup; a league_id index supports referenced-league checks. League deletion is blocked while followed, preventing silent loss of preferences; use an explicit migration if metadata deletion is intended. IDs are internal league IDs, not provider api_league_id or league-season IDs.

Authenticated endpoints: GET `/account/leagues` returns `{data:{league_ids:[]}}`; PUT replaces the set with `{league_ids:[4,5]}` (maximum 200 unique strictly positive integers). Empty list unfollows all. Unknown catalog IDs, duplicates, non-integers and extra request fields return 422. User ownership comes only from Clerk sub. Replacement locks the account row, validates all IDs before deleting removed follows, preserves timestamps for retained follows and commits atomically. Concurrent replacements serialize; the last completed replacement wins. No request-time DDL.

GET `/account/leagues/directory` supplies the full metadata league catalog with country and optional public crest URL, independent of upcoming fixture coverage. Frontend Settings allows search, selection and explicit Save; controls remain disabled during save. Discover/Matches Your leagues copies the current account follows into the temporary league filter. Date changes retain that filter; adjusting or clearing it never changes permanent follows. Returning to Your leagues reapplies the latest account set. Empty follows link to Settings; account-load failures offer retry. No automatic onboarding selection is imposed.

Verification: 13 isolated followed-league API tests plus 11 preference regressions pass; PostgreSQL integration test verifies idempotence, timestamp preservation, isolation, invalid-league rejection, FK enforcement, clear-all and account-delete cascade. Synthetic integration account writes roll back. TypeScript passes. Schema applied to the configured database; live UI verification recorded in Ballistics product implementation status. No commit/push or deployment is implied.

Run checks:
```sh
python -m pytest API/tests/test_followed_leagues.py API/tests/test_preferences.py -q
# With an explicitly configured PostgreSQL test connection:
python -m pytest API/tests/test_followed_leagues_postgres.py -q
cd frontend_dev && npm run typecheck:research
```

Live browser verification: saved Premier League, reloaded to confirm persisted selection, applied Your leagues in Matches and Discover (leagues=4), cleared a temporary filter without changing follows, then saved an empty set to restore the original account state. League directory crest configuration aligned with the existing research asset store; all four visible England league logos loaded.

## Shared Saved snapshots — 2026-10-19

New bootstrap includes `shared_saved_snapshots.sql`. On an existing installation,
stop the account API and run `python -m scripts.migrate_saved_snapshots --apply`
from this repository, then restart it. Without `--apply` this reports the legacy
row/payload-size baseline only. The migration is atomic, reconciles card JSON,
run date and saved timestamps, and records a one-time migration marker. Re-running
it never resurrects removed saves. Original `app.saved_fixtures` rows remain as a
rollback checkpoint; the new API reads/writes `app.saved_fixture_refs` only.
Do not switch to the old API after new saves without exporting/reconciling current
references first: the retained legacy table is a cutover checkpoint, not a mirror.

- `app.saved_snapshots`: global identity, fixture ID, canonical SHA-256 including
  schema/provenance/run/card, DB capture time, month and archive locator.
- `app.saved_snapshot_payloads`: immutable JSON once per snapshot, partitioned by
  UTC capture month, composite FK to the permanent identity registry.
- `app.saved_fixture_refs`: `(user_id, fixture_id)` unique, snapshot FK and saved
  time. Duplicate saves preserve the first snapshot; unsave removes only the ref.
- Old database and browser-import cards are `legacy_card_v1`; new submitted
  cards are `client_card_v1`. Neither claims verified engine forecast provenance.
  Existing `forecast.entries.id` / `forecast.snapshots.id` remain canonical; no
  inferred links to current forecasts are created from labels or run dates.

The current month and next three months are provisioned during bootstrap.
Run `python -m scripts.maintain_saved_snapshots` daily to keep partitions ready.
The Ballistics `saved_snapshot_maintenance` Airflow DAG is paused on creation;
mount this account repository on the worker and set `ACCOUNT_PROJECT_ROOT`, plus
its account DB environment and dependencies, before unpausing. No network service
or cross-repository mount was configured automatically.

### Private archival and retention

`SAVED_ARCHIVE_BUCKET` must be an explicitly configured private S3 bucket, readable
by the account API and writable by maintenance. Never use the public crest bucket.
The archive stores football payloads only; user IDs/references stay in Postgres.
The API resolves missing hot payloads using the permanent locator and validates
SHA-256 plus snapshot identity before returning them to the authenticated owner.

Explicit command (defaults to dry-run):

```sh
python -m scripts.archive_saved_snapshots --month 2026-01-01 --hot-days 90
# Export + read-back verification, preserving hot data:
python -m scripts.archive_saved_snapshots --month 2026-01-01 --hot-days 90 --apply
# Only with a finalized policy and verified private archive configuration:
python -m scripts.archive_saved_snapshots --month 2026-01-01 --hot-days 90 --apply --retire
```

All rows in the partition must have `settled_at`, an explicitly released
`retention_hold`, and both capture/settlement age beyond the supplied interval.
New rows are held by default. No automatic settlement/hold release is implemented;
an operational release must verify terminal results and the correction window.
Maintenance writes content-addressed encrypted S3 objects and a count/checksum
manifest, reads them back through the archive resolver, publishes locators, then
optionally detaches/drops the whole verified partition transactionally. Failed
verification leaves hot data intact. Retrying after a committed retirement is a
no-op. Shared identities and account references are never removed by this job.

Daily job settings: `SAVED_ARCHIVE_HOT_DAYS` has no default; without it only
partition provisioning runs. `SAVED_ARCHIVE_APPLY=true` enables export;
`SAVED_ARCHIVE_RETIRE=true` separately enables hot partition retirement.
The proposed 30-day display/90-day forecast windows are not active policy.
This implementation covers Saved payloads; existing forecast history remains
append-only and has not been repartitioned or given a destructive cleanup job.

Verification: 38 API/Postgres tests passed, including real rolled-back shared
identity, first-save preservation, independent unsave, immutable payload/version
protection, held-partition rejection, corrupt S3 read-back failure, successful
retirement/retrieval and job retry. S3 integration uses an in-memory transport;
no actual S3 upload, bucket change or hot-data retirement was performed.


2026-10-19 — Private archive bucket `ballistics-archive-438542302919-ca-central-1` provisioned in ca-central-1 at owner request. Public access blocking, bucket-owner-enforced ownership, AES256 default encryption, versioning and HTTPS-only policy verified. Account gitignored environment now sets SAVED_ARCHIVE_BUCKET; API restarted. Actual S3 synthetic write/read through the account archive resolver passed checksum/identity verification; test object version removed. No user snapshots archived, no lifecycle expiry set, no retention activation. Existing AWS credential chain used; dedicated deployment IAM roles and worker configuration remain deployment work. Reprovision via original repo `python -m scripts.configure_saved_archive_bucket`. Local SDK installed at boto3/botocore 1.39.8 to preserve existing aiobotocore compatibility.
