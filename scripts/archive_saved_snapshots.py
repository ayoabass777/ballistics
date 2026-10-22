"""Verified S3 export of whole eligible capture partitions; dry-run by default.

No retention age is implicit. All rows must be settled and released from hold.
Archive access uses SAVED_ARCHIVE_BUCKET with private bucket/IAM configuration.
--apply exports and verifies; --retire additionally removes the verified hot partition.
"""
import argparse
import hashlib
import json
import os
import re
from datetime import date
from sqlalchemy import text
from API.core.database import engine
from API.saved_snapshots import canonical, archived_card


def archive_partition(db, month, hot_days, client=None, apply=False, retire=False):
    if not re.fullmatch(r'\d{4}-\d{2}-01', month) or hot_days < 1:
        raise ValueError('Use a month start and a positive explicit retention period')
    date.fromisoformat(month)
    if retire and not apply:
        raise ValueError('Retirement requires verified export')
    # Conflicts with save INSERTs, hold/settlement changes; serialized maintenance.
    db.execute(text('LOCK TABLE app.saved_snapshots IN SHARE ROW EXCLUSIVE MODE'))
    db.execute(text('LOCK TABLE app.saved_fixture_refs IN SHARE MODE'))
    partition = 'saved_snapshot_payloads_' + month[:7].replace('-', '')
    rows = db.execute(text('''SELECT s.*, p.card FROM app.saved_snapshots s
        JOIN app.saved_snapshot_payloads p ON p.snapshot_id=s.id AND p.capture_month=s.capture_month
        WHERE s.capture_month=CAST(:month AS date) ORDER BY s.id'''), {'month': month}).mappings().all()
    blocked = db.execute(text('''SELECT count(*) FROM app.saved_snapshots
        WHERE capture_month=CAST(:month AS date) AND
        (retention_hold OR settled_at IS NULL OR settled_at > NOW()-make_interval(days => :days)
         OR captured_at > NOW()-make_interval(days => :days))'''), {'month': month, 'days': hot_days}).scalar_one()
    summary = {'month': month, 'rows': len(rows), 'blocked': blocked, 'apply': apply, 'retire': retire}
    if not rows or blocked or not apply:
        return summary
    bucket = os.environ.get('SAVED_ARCHIVE_BUCKET')
    if not bucket:
        raise ValueError('Configure a private SAVED_ARCHIVE_BUCKET before export')
    if client is None:
        import boto3
        client = boto3.client('s3')
    manifest = []
    for row in rows:
        body = canonical({'schema_version': 1, 'snapshot_id': row['id'], 'card': row['card'],
                          'run_ds': row['run_ds'], 'provenance': row['provenance'],
                          'captured_at': row['captured_at'].isoformat(), 'content_hash': row['content_hash']}).encode()
        checksum = hashlib.sha256(body).hexdigest()
        key = f'saved-snapshots/v1/{month}/{row["id"]}/{checksum}.json'
        # Content-addressed keys make retries deterministic; no user identifiers.
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json', ServerSideEncryption='AES256')
        check = dict(row, archive_key=key, archive_sha256=checksum)
        if archived_card(check, client) != row['card']:
            raise ValueError('Archive read-back mismatch')
        manifest.append({'snapshot_id': row['id'], 'key': key, 'sha256': checksum})
    manifest_body = canonical({'schema_version': 1, 'capture_month': month, 'count': len(manifest), 'objects': manifest}).encode()
    manifest_hash = hashlib.sha256(manifest_body).hexdigest()
    manifest_key = f'saved-snapshots/v1/{month}/manifest-{manifest_hash}.json'
    client.put_object(Bucket=bucket, Key=manifest_key, Body=manifest_body, ContentType='application/json', ServerSideEncryption='AES256')
    if client.get_object(Bucket=bucket, Key=manifest_key)['Body'].read() != manifest_body:
        raise ValueError('Manifest read-back mismatch')
    for item in manifest:
        db.execute(text('''UPDATE app.saved_snapshots SET archive_key=:key, archive_sha256=:sha256,
            archived_at=clock_timestamp() WHERE id=:snapshot_id'''), item)
    # Transactional DDL: rollback restores hot data and locators on any failure.
    if retire:
        db.execute(text(f'ALTER TABLE app.saved_snapshot_payloads DETACH PARTITION app.{partition}'))
        db.execute(text(f'DROP TABLE app.{partition}'))
    summary.update(manifest=manifest_key, verified=True)
    return summary

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--month', required=True)
    parser.add_argument('--hot-days', type=int, required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--retire', action='store_true')
    args = parser.parse_args()
    with engine.begin() as db:
        print(json.dumps(archive_partition(db, args.month, args.hot_days, apply=args.apply, retire=args.retire)))
