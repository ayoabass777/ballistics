"""Daily partition provisioning and explicitly configured archive policy."""
import os
from sqlalchemy import text
from API.core.database import engine
from scripts.archive_saved_snapshots import archive_partition


def main():
    with engine.begin() as db:
        db.execute(text('SELECT app.ensure_saved_snapshot_partitions()'))
    days = os.getenv('SAVED_ARCHIVE_HOT_DAYS')
    if not days:
        print('Future partitions ready; no retention period configured, archival disabled.')
        return
    apply = os.getenv('SAVED_ARCHIVE_APPLY') == 'true'
    retire = os.getenv('SAVED_ARCHIVE_RETIRE') == 'true'
    with engine.connect() as db:
        months = db.execute(text('SELECT DISTINCT capture_month FROM app.saved_snapshots ORDER BY capture_month')).scalars().all()
    for month in months:
        with engine.begin() as db:
            print(archive_partition(db, month.isoformat(), int(days), apply=apply, retire=retire))

if __name__ == '__main__': main()
