"""Run once with account API stopped; originals retained for explicit rollback.

python -m scripts.migrate_saved_snapshots [--apply]
"""
import argparse
from pathlib import Path
from sqlalchemy import text
from API.core.database import engine
from API.saved_snapshots import save_reference


def migrate(apply=False):
    with engine.connect() as db:
        before = db.execute(text('SELECT count(*), COALESCE(sum(pg_column_size(card)),0) FROM app.saved_fixtures')).one()
        print({'legacy_rows': before[0], 'legacy_card_bytes': before[1], 'apply': apply})
        db.rollback()
        if not apply:
            return
        with db.begin():
            db.execute(text('SELECT pg_advisory_xact_lock(734120261)'))
            db.execute(text(Path('API/sql/shared_saved_snapshots.sql').read_text()))
            # A one-time marker avoids resurrecting subsequent user removals on reruns.
            db.execute(text('CREATE TABLE IF NOT EXISTS app.account_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())'))
            if db.execute(text("SELECT 1 FROM app.account_migrations WHERE name='shared_saved_v1'")).first():
                print('Already migrated; no user references changed.')
                return
            db.execute(text('LOCK TABLE app.saved_fixtures IN SHARE MODE'))
            rows = db.execute(text('SELECT * FROM app.saved_fixtures ORDER BY user_id, fixture_id')).mappings().all()
            for row in rows:
                save_reference(db, row['user_id'], row['card'], row['run_ds'], row['saved_at'], 'legacy_card_v1')
            mismatch = db.execute(text('''SELECT count(*) FROM app.saved_fixtures o
                LEFT JOIN app.saved_fixture_refs r USING(user_id, fixture_id)
                LEFT JOIN app.saved_snapshots s ON s.id=r.saved_snapshot_id
                LEFT JOIN app.saved_snapshot_payloads p ON p.snapshot_id=s.id AND p.capture_month=s.capture_month
                WHERE r.saved_snapshot_id IS NULL OR p.card IS DISTINCT FROM o.card
                  OR s.run_ds IS DISTINCT FROM o.run_ds OR r.saved_at IS DISTINCT FROM o.saved_at''')).scalar_one()
            if mismatch:
                raise RuntimeError('Migration reconciliation failed')
            db.execute(text("INSERT INTO app.account_migrations(name) VALUES ('shared_saved_v1')"))
            print({'migrated_and_reconciled': len(rows), 'originals_retained': True})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--apply', action='store_true')
    migrate(parser.parse_args().apply)
