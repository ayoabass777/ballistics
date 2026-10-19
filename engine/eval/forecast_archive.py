"""Verified full/incremental forecast copies. Never deletes source history."""
from __future__ import annotations
import argparse
import hashlib
import gzip
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from psycopg2.extras import RealDictCursor
from engine.eval.db import get_conn

TABLES = ('snapshots', 'entries', 'settlements')
DDL = '''CREATE SCHEMA IF NOT EXISTS forecast_archive;
CREATE TABLE IF NOT EXISTS forecast_archive.checkpoints (
 bucket text NOT NULL, source_table text NOT NULL, last_id bigint NOT NULL DEFAULT 0,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(bucket,source_table));
CREATE TABLE IF NOT EXISTS forecast_archive.batches (
 bucket text NOT NULL, source_table text NOT NULL, first_id bigint NOT NULL, last_id bigint NOT NULL,
 row_count integer NOT NULL, object_key text NOT NULL, sha256 text NOT NULL,
 manifest_key text NOT NULL, verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket, source_table, last_id));'''


def encode(value):
    def default(obj):
        if isinstance(obj, (date, datetime)): return obj.isoformat()
        if isinstance(obj, Decimal): return str(obj)
        raise TypeError(type(obj).__name__)
    return json.dumps(value, default=default, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode()


def upload_verified(s3, bucket, key, data):
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType='application/gzip' if key.endswith('.gz') else 'application/json', ServerSideEncryption='AES256')
    response = s3.get_object(Bucket=bucket, Key=key)
    try: restored = response['Body'].read()
    finally: response['Body'].close()
    if hashlib.sha256(restored).digest() != hashlib.sha256(data).digest():
        raise ValueError('Archive read-back checksum mismatch')
    return restored


def archive(conn, s3, bucket, batch_size=100, mode='incremental'):
    if not bucket or not 1 <= batch_size <= 1000 or mode not in ('full','incremental'):
        raise ValueError('Bucket, mode and bounded batch size required')
    # Session lock serializes archive workers, not forecast capture throughout upload.
    with conn.cursor() as cur:
        cur.execute('SELECT pg_try_advisory_lock(734120262)')
        if not cur.fetchone()[0]: raise RuntimeError('Forecast archive already running')
    conn.commit()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
                # SHARE waits for outstanding INSERT transactions. High-water marks
                # cannot skip lower sequence IDs from transactions still committing.
                cur.execute('LOCK TABLE forecast.snapshots, forecast.entries, forecast.settlements IN SHARE MODE')
                bounds = {}
                for table in TABLES:
                    cur.execute(f'SELECT COALESCE(max(id),0) FROM forecast.{table}')
                    bounds[table] = cur.fetchone()[0]
        totals = {}
        for table in TABLES:
            totals[table] = 0
            while True:
                with conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute('''INSERT INTO forecast_archive.checkpoints(bucket,source_table)
                            VALUES (%s,%s) ON CONFLICT DO NOTHING''', (bucket,table))
                        cur.execute('''SELECT last_id FROM forecast_archive.checkpoints
                            WHERE bucket=%s AND source_table=%s FOR UPDATE''', (bucket,table))
                        last = cur.fetchone()['last_id']
                        cur.execute(f'SELECT * FROM forecast.{table} WHERE id>%s AND id<=%s ORDER BY id LIMIT %s',
                                    (last,bounds[table],1 if table == 'snapshots' else batch_size))
                        rows = [dict(r) for r in cur.fetchall()]
                        if not rows: break
                        body = gzip.compress(encode({'schema_version':1, 'source_table':table, 'rows':rows}), mtime=0)
                        checksum = hashlib.sha256(body).hexdigest()
                        # Payload date is an export partition; original capture and
                        # settlement timestamps are retained in every archived row.
                        day = datetime.now(timezone.utc).strftime('%Y/%m/%d')
                        prefix = f'forecast-history/v1/{table}/{day}/{rows[0]["id"]}-{rows[-1]["id"]}-{checksum}'
                        key = prefix+'.json.gz'
                        restored = upload_verified(s3,bucket,key,body)
                        parsed = json.loads(gzip.decompress(restored))
                        assert len(parsed['rows']) == len(rows)
                        manifest = {'schema_version':1,'source_table':table,'object_key':key,
                                    'row_count':len(rows),'first_id':rows[0]['id'],'last_id':rows[-1]['id'],
                                    'sha256':checksum,'run_upper_id':bounds[table]}
                        manifest_key = prefix+'.manifest.json'
                        upload_verified(s3,bucket,manifest_key,encode(manifest))
                        cur.execute('''INSERT INTO forecast_archive.batches
                            (bucket,source_table,first_id,last_id,row_count,object_key,sha256,manifest_key)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                            (bucket,table,rows[0]['id'],rows[-1]['id'],len(rows),key,checksum,manifest_key))
                        cur.execute('''UPDATE forecast_archive.checkpoints SET last_id=%s,updated_at=clock_timestamp()
                            WHERE bucket=%s AND source_table=%s''',(rows[-1]['id'],bucket,table))
                        totals[table] += len(rows)
        return {'mode':mode,'copied_rows':totals,'upper_ids':bounds,'source_deleted':False}
    finally:
        conn.rollback()
        with conn.cursor() as cur: cur.execute('SELECT pg_advisory_unlock(734120262)')
        conn.commit()


def main():
    p=argparse.ArgumentParser();p.add_argument('--mode',choices=['full','incremental'],default='incremental')
    p.add_argument('--batch-size',type=int,default=100);args=p.parse_args()
    bucket=os.getenv('FORECAST_ARCHIVE_BUCKET')
    if not bucket: raise SystemExit('FORECAST_ARCHIVE_BUCKET must be configured')
    import boto3
    conn=get_conn()
    try: print(json.dumps(archive(conn,boto3.client('s3'),bucket,args.batch_size,args.mode)))
    finally: conn.close()

if __name__=='__main__': main()
