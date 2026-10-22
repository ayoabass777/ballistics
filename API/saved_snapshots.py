"""Immutable, shared card versions. Client cards are not certified forecasts."""
from __future__ import annotations
import hashlib
import json
import os
from sqlalchemy import text


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def snapshot_identity(card, run_ds, provenance):
    envelope = {'schema_version': 1, 'provenance': provenance, 'run_ds': run_ds, 'card': card}
    return hashlib.sha256(canonical(envelope).encode()).hexdigest()


def save_reference(db, user_id, card, run_ds, saved_at=None, provenance='client_card_v1'):
    # Serialize saves/unsaves for one account; first save wins without orphan versions.
    db.execute(text('SELECT user_id FROM app.app_users WHERE user_id=:user_id FOR UPDATE'), {'user_id': user_id})
    args = {'user_id': user_id, 'fixture_id': card['fixture_id']}
    if db.execute(text('SELECT 1 FROM app.saved_fixture_refs WHERE user_id=:user_id AND fixture_id=:fixture_id'), args).first():
        return
    digest = snapshot_identity(card, run_ds, provenance)
    # ON CONFLICT DO NOTHING followed by a new statement sees the winning concurrent insert.
    created = db.execute(text('''INSERT INTO app.saved_snapshots (fixture_id, content_hash, provenance, run_ds)
        VALUES (:fixture_id, :digest, :provenance, :run_ds) ON CONFLICT (content_hash) DO NOTHING RETURNING id'''),
        {**args, 'digest': digest, 'provenance': provenance, 'run_ds': run_ds}).first()
    identity = db.execute(text('SELECT id, capture_month FROM app.saved_snapshots WHERE content_hash=:digest'), {'digest': digest}).mappings().one()
    if created:
        db.execute(text('''INSERT INTO app.saved_snapshot_payloads (capture_month, snapshot_id, card)
            VALUES (:month, :id, CAST(:card AS jsonb)) ON CONFLICT DO NOTHING'''),
            {'month': identity['capture_month'], 'id': identity['id'], 'card': canonical(card)})
    db.execute(text('''INSERT INTO app.saved_fixture_refs (user_id, fixture_id, saved_snapshot_id, saved_at)
        VALUES (:user_id, :fixture_id, :id, COALESCE(:saved_at, NOW())) ON CONFLICT DO NOTHING'''),
        {**args, 'id': identity['id'], 'saved_at': saved_at})


def archived_card(row, client=None):
    bucket = os.environ.get('SAVED_ARCHIVE_BUCKET')
    if not bucket or not row['archive_key'] or not row['archive_sha256']:
        raise ValueError('Snapshot payload unavailable')
    if client is None:
        import boto3
        client = boto3.client('s3')
    result = client.get_object(Bucket=bucket, Key=row['archive_key'])
    body = result['Body'].read(16385)
    if len(body) > 16384 or hashlib.sha256(body).hexdigest() != row['archive_sha256']:
        raise ValueError('Snapshot archive integrity failure')
    value = json.loads(body)
    if value['snapshot_id'] != row['id'] or snapshot_identity(value['card'], row['run_ds'], row['provenance']) != row['content_hash']:
        raise ValueError('Snapshot archive identity failure')
    return value['card']


FILTERS = {
    'all': None, 'upcoming': ['NS', 'TBD'], 'live': ['1H','HT','2H','ET','BT','P','LIVE'],
    'finished': ['FT','AET','PEN'],
    'other': ['PST','CANC','ABD','SUSP','INT','AWD','WO'],
}


def read_saved(db, user_id):
    return read_saved_page(db, user_id)['items']


def read_saved_page(db, user_id, scope='all', cursor=None, limit=12):
    import base64
    from datetime import datetime
    if scope not in FILTERS or not 1 <= limit <= 50:
        raise ValueError('Invalid Saved filter or page size')
    args = {'user_id': user_id, 'limit': limit+1}
    conditions = ['r.user_id=:user_id']
    if scope == 'other':
        conditions.append("(f.fixture_status IS NULL OR NOT (f.fixture_status = ANY(:known)))")
        args['known'] = FILTERS['upcoming']+FILTERS['live']+FILTERS['finished']
    elif FILTERS[scope]:
        conditions.append('f.fixture_status = ANY(:statuses)')
        args['statuses'] = FILTERS[scope]
    if cursor:
        try:
            value = json.loads(base64.urlsafe_b64decode(cursor.encode()))
            stamp = datetime.fromisoformat(value['saved_at'])
            if value['scope'] != scope or stamp.tzinfo is None or type(value['fixture_id']) is not int or value['fixture_id'] <= 0:
                raise ValueError()
            args.update(after_time=stamp, after_id=value['fixture_id'])
        except Exception as exc:
            raise ValueError('Invalid Saved cursor') from exc
        conditions.append('(r.saved_at,r.fixture_id) < (:after_time,:after_id)')
    rows = db.execute(text("""SELECT r.saved_at, s.*, p.card
        FROM app.saved_fixture_refs r JOIN app.saved_snapshots s ON s.id=r.saved_snapshot_id
        LEFT JOIN raw.fact_fixtures f ON f.fixture_id=r.fixture_id
        LEFT JOIN app.saved_snapshot_payloads p ON p.capture_month=s.capture_month AND p.snapshot_id=s.id
        WHERE """ + ' AND '.join(conditions) + ' ORDER BY r.saved_at DESC,r.fixture_id DESC LIMIT :limit'), args).mappings().all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    items = [{'card': row['card'] if row['card'] is not None else archived_card(row),
              'saved_at': row['saved_at'], 'run_ds': row['run_ds'], 'snapshot_id': row['id']} for row in rows]
    next_cursor = None
    if has_more:
        last = rows[-1]
        next_cursor = base64.urlsafe_b64encode(canonical({'scope':scope, 'saved_at':last['saved_at'].isoformat(), 'fixture_id':last['fixture_id']}).encode()).decode()
    return {'items': enrich_saved(db, items), 'next_cursor': next_cursor}


def saved_ids(db, user_id):
    return {'fixture_ids': list(db.execute(text('SELECT fixture_id FROM app.saved_fixture_refs WHERE user_id=:user_id ORDER BY fixture_id'), {'user_id':user_id}).scalars().all())}


def fixture_state(code):
    if code in ('NS', 'TBD'): return 'upcoming'
    if code in ('1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE'): return 'live'
    if code in ('FT', 'AET', 'PEN'): return 'finished'
    return {'PST': 'postponed', 'CANC': 'cancelled', 'ABD': 'abandoned',
            'SUSP': 'suspended', 'INT': 'interrupted', 'AWD': 'awarded', 'WO': 'walkover'}.get(code, 'unknown')


def enrich_saved(db, items):
    from API.card_identity import identity
    if not items:
        return items
    ids = [item['card']['fixture_id'] for item in items]
    fixtures = {r['fixture_id']: r for r in db.execute(text(
        'SELECT fixture_id, fixture_status, kickoff_utc FROM raw.fact_fixtures WHERE fixture_id = ANY(:ids)'),
        {'ids': ids}).mappings()}
    for item in items:
        context, content = identity(item['card'])
        params = {'context': context, 'content': content, 'saved_at': item['saved_at']}
        baseline = db.execute(text("""SELECT version FROM raw.card_versions
            WHERE context_hash=:context AND content_hash=:content AND recorded_at<=:saved_at
            ORDER BY version DESC LIMIT 1"""), params).mappings().first()
        latest = db.execute(text("""SELECT version, content_hash, recorded_at FROM raw.card_versions
            WHERE context_hash=:context ORDER BY version DESC LIMIT 1"""), params).mappings().first()
        fixture = fixtures.get(item['card']['fixture_id'], {})
        item.update(status=fixture_state(fixture.get('fixture_status')),
                    current_kickoff=fixture.get('kickoff_utc'),
                    card_version=baseline['version'] if baseline else None,
                    content_hash=content,
                    latest_version=latest['version'] if latest else None,
                    updated_since_saved=bool(latest and latest['recorded_at'] > item['saved_at'] and
                        (latest['version'] > baseline['version'] if baseline else latest['content_hash'] != content)))
    return items
