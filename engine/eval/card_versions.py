"""Meaningful presentation versions, appended atomically with published match data."""
import hashlib
import json
from datetime import datetime, timezone

PICK_FIELDS = ('list', 'favourite', 'underdog', 'team', 'event', 'context', 'k', 'n', 'market', 'probability', 'big_dog')
CONTEXT_FIELDS = ('list', 'team', 'event', 'context', 'market')

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()).hexdigest()

def identity(card):
    pick = card.get('pick') or {}
    context = {'schema': 1, 'fixture_id': card['fixture_id'], **{k: pick.get(k) for k in CONTEXT_FIELDS}}
    kickoff = datetime.fromisoformat(str(card['kickoff']).replace('Z', '+00:00')).astimezone(timezone.utc).isoformat()
    content = {'context': context, 'kickoff': kickoff, 'pick': {k: pick.get(k) for k in PICK_FIELDS}}
    return digest(context), digest(content)

DDL = '''
CREATE TABLE IF NOT EXISTS raw.card_versions (
 id bigserial PRIMARY KEY, fixture_id bigint NOT NULL, context_hash text NOT NULL,
 content_hash text NOT NULL, version integer NOT NULL, card jsonb NOT NULL,
 run_ds date NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(context_hash, version)
);
CREATE INDEX IF NOT EXISTS card_versions_fixture_context ON raw.card_versions(fixture_id, context_hash, version DESC);
'''

def publish(cur, rows, run_ds, recorded_at=None):
    # Shared lock covers competing runs and first insert in a new context.
    cur.execute('SELECT pg_advisory_xact_lock(734120281)')
    for fid, payload in rows:
        data = payload.get('discovery')
        # No kickoff (TBD fixture): nothing to version yet, and it must not fail the run.
        if not data or not (data.get('fixture') or {}).get('kickoff'):
            continue
        picks = [None, data.get('standout'), *(data.get('sections') or {}).values()]
        seen = set()
        for pick in picks:
            card = {**data['fixture'], 'pick': {k: pick.get(k) for k in PICK_FIELDS} if pick else None}
            context, content = identity(card)
            if context in seen:
                continue
            seen.add(context)
            cur.execute('SELECT content_hash, version, run_ds FROM raw.card_versions WHERE context_hash=%s ORDER BY version DESC LIMIT 1', (context,))
            old = cur.fetchone()
            if old and (old[0] == content or str(old[2]) > str(run_ds)):
                continue
            cur.execute('''INSERT INTO raw.card_versions(fixture_id,context_hash,content_hash,version,card,run_ds,recorded_at)
                VALUES (%s,%s,%s,%s,%s::jsonb,%s,COALESCE(%s,clock_timestamp()))''', (fid, context, content, old[1]+1 if old else 1, json.dumps(card), run_ds, recorded_at))
