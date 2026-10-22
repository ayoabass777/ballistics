"""Card identity contract v1; kept in parity with engine/eval/card_versions.py."""
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
