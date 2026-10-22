from datetime import datetime, timezone, timedelta
from copy import deepcopy
from API.card_identity import identity
from API.saved_snapshots import enrich_saved, fixture_state

CARD = dict(fixture_id=1, kickoff='2026-10-25T12:00:00Z', pick=dict(list='goals',market='over_1.5', probability=.72))

def test_meaningful_identity():
    assert identity(CARD) == identity(dict(CARD, run_ds='new',home_crest_url='new',home='Renamed'))
    assert identity(CARD) == identity(dict(CARD, kickoff='2026-10-25T14:00:00+02:00'))
    changed = deepcopy(CARD); changed['pick']['probability']=.68
    assert identity(CARD)[0] == identity(changed)[0]
    assert identity(CARD)[1] != identity(changed)[1]
    changed['pick']['market']='under_3.5'
    assert identity(CARD)[0] != identity(changed)[0]

class Result:
    def __init__(self, rows): self.rows=rows
    def mappings(self): return self
    def __iter__(self): return iter(self.rows)
    def first(self): return self.rows[0] if self.rows else None
class DB:
    def __init__(self, history): self.history=history
    def execute(self, sql, params):
        if 'fact_fixtures' in str(sql): return Result([dict(fixture_id=1,fixture_status='PST',kickoff_utc=None)])
        rows = self.history
        if 'recorded_at<=' in str(sql): rows = [r for r in rows if r['content_hash']==params['content'] and r['recorded_at']<=params['saved_at']]
        return Result(list(reversed(rows))[:1])

def test_return_to_original_content_is_new_event_but_original_save_stays():
    now=datetime.now(timezone.utc); context,content=identity(CARD)
    rows=[dict(context_hash=context,content_hash=content,version=1,recorded_at=now-timedelta(days=1)),
          dict(context_hash=context,content_hash='changed',version=2,recorded_at=now+timedelta(days=1)),
          dict(context_hash=context,content_hash=content,version=3,recorded_at=now+timedelta(days=2))]
    item=dict(card=deepcopy(CARD),saved_at=now,run_ds=None)
    result=enrich_saved(DB(rows),[item])[0]
    assert result['card_version']==1 and result['latest_version']==3
    assert result['updated_since_saved'] and result['status']=='postponed'
    assert result['card']==CARD
    result=enrich_saved(DB(rows[:1]),[item])[0]
    assert not result['updated_since_saved']

def test_fixture_status_is_not_guessed_from_kickoff():
    for code, status in [('NS','upcoming'),('FT','finished'),('PEN','finished'),('CANC','cancelled'),('2H','live'),(None,'unknown')]:
        assert fixture_state(code)==status
