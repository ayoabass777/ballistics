"""Apply numbered SQL migrations from db/migrations/ to the pipeline database.

Each file NNNN_description.sql runs once, in its own transaction, in version
order. Applied versions are recorded in public.schema_migrations with the
file's SHA-256, so editing an applied migration is reported instead of
silently ignored. A session advisory lock keeps two deploys from migrating at
the same time.

Connection: DATABASE_URL, or the usual DB_NAME / DB_USER / DB_PASSWORD /
DB_HOST / DB_PORT. Never prints the connection string.

Usage:
    python scripts/migrate.py status
    python scripts/migrate.py up
    python scripts/migrate.py baseline 0001   # record <= 0001 as applied without running

`baseline` is for a database whose schema already exists (the local database,
or one restored from a dump): it marks the baseline applied so `up` only runs
later files.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "db" / "migrations"
FILE_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
LOCK_KEY = 731_046_001

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version     text        PRIMARY KEY,
    name        text        NOT NULL,
    checksum    text        NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    baselined   boolean     NOT NULL DEFAULT false
)
"""


def connect():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "ballistics"),
        user=os.getenv("DB_USER", "ballistics_user"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
    )


def migration_files() -> list[tuple[str, Path, str]]:
    found = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        m = FILE_RE.match(path.name)
        if not m:
            raise SystemExit(f"Unexpected file in {MIGRATIONS_DIR.name}/: {path.name}")
        found.append((m.group(1), path, hashlib.sha256(path.read_bytes()).hexdigest()))
    versions = [v for v, _, _ in found]
    if len(versions) != len(set(versions)):
        raise SystemExit("Duplicate migration version numbers")
    return found


def applied(cur) -> dict[str, str]:
    cur.execute("SELECT version, checksum FROM public.schema_migrations")
    return dict(cur.fetchall())


def check_checksums(files, done) -> None:
    changed = [p.name for v, p, c in files if v in done and done[v] != c]
    if changed:
        raise SystemExit("Applied migrations were edited: " + ", ".join(changed)
                         + ". Add a new migration instead.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("up")
    b = sub.add_parser("baseline")
    b.add_argument("version", help="highest version to record as already applied")
    args = ap.parse_args()

    files = migration_files()
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
            cur.execute(TABLE_DDL)
        conn.commit()

        with conn.cursor() as cur:
            done = applied(cur)
        check_checksums(files, done)
        pending = [(v, p, c) for v, p, c in files if v not in done]

        if args.cmd == "status":
            for v, p, _ in files:
                print(f"{'applied' if v in done else 'pending'}  {p.name}")
            return

        if args.cmd == "baseline":
            marked = [(v, p, c) for v, p, c in pending if v <= args.version]
            with conn.cursor() as cur:
                for v, p, c in marked:
                    cur.execute(
                        "INSERT INTO public.schema_migrations (version, name, checksum, baselined)"
                        " VALUES (%s, %s, %s, true)", (v, p.name, c))
            conn.commit()
            print(f"baselined {len(marked)} migration(s)")
            return

        for v, p, c in pending:
            print(f"applying {p.name}")
            try:
                with conn.cursor() as cur:
                    cur.execute(p.read_text())
                    cur.execute(
                        "INSERT INTO public.schema_migrations (version, name, checksum)"
                        " VALUES (%s, %s, %s)", (v, p.name, c))
                conn.commit()
                # A migration may SET session options (pg_dump output clears
                # search_path); don't let them leak into the next file.
                with conn.cursor() as cur:
                    cur.execute("RESET ALL")
                conn.commit()
            except Exception:
                conn.rollback()
                print(f"failed: {p.name}; nothing from it was applied", file=sys.stderr)
                raise
        print(f"{len(pending)} migration(s) applied")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
