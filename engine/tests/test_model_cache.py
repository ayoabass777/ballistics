"""Processing-state/version regression tests; opt-in PostgreSQL writes roll back."""
import os
from uuid import uuid4
from datetime import datetime, timezone
import pytest
from engine.eval.model_cache import ModelCache, pack, unpack, digest
from engine.eval.db import get_conn


def test_codec_preserves_numeric_keys_and_times():
    value={'attack':{3:1.2},'time':datetime(2026,10,19,tzinfo=timezone.utc),'none':None}
    assert unpack(pack(value))==value
    assert digest(value)==digest(dict(reversed(list(value.items()))))

@pytest.mark.skipif(os.getenv('TEST_MODEL_CACHE')!='1',reason='Explicit local PostgreSQL test opt-in required')
def test_versions_reuse_and_failure():
    conn=get_conn();cache=ModelCache(conn);scope='test.'+uuid4().hex
    try:
        count=[0]
        def calculate(value):count[0]+=1;return value
        a=cache.memo(scope,'fixture1',{'team':1,'league':1},lambda:calculate({'p':.72}))
        first_versions=cache.new_versions
        assert cache.memo(scope,'fixture1',{'league':1,'team':1},lambda:calculate('wrong'))==a
        assert count==[1]
        # Changed league data invalidates, even with no team change. Same result
        # does not create another version.
        cache.memo(scope,'fixture1',{'team':1,'league':2},lambda:calculate({'p':.72}))
        assert count==[2] and cache.new_versions==first_versions
        # A -> B -> A is three change events.
        cache.memo(scope,'fixture1',{'team':2,'league':2},lambda:{'p':.68})
        cache.memo(scope,'fixture1',{'team':3,'league':2},lambda:{'p':.72})
        assert cache.new_versions==3
        # Separate fixture and model state, including valid empty outputs.
        cache.memo(scope,'fixture2',{},lambda:[])
        assert cache.memo(scope,'fixture2',{},lambda:['wrong'])==[]
        cache.memo(scope+'.other','fixture1',{},lambda:{'p':.1})
        def broken():raise RuntimeError('failed computation')
        with pytest.raises(RuntimeError):cache.memo(scope,'fixture1',{'team':4},broken)
        assert cache.memo(scope,'fixture1',{'team':3,'league':2},lambda:'wrong')=={'p':.72}
    finally:conn.rollback();conn.close()

@pytest.mark.skipif(os.getenv('TEST_MODEL_CACHE')!='1',reason='Explicit local PostgreSQL test opt-in required')
def test_bayesian_league_change_only_recomputes_affected_team():
    import pandas as pd
    from engine.eval.model_cache import active_cache
    from engine.eval.bayesian_streaks import _cached_beliefs
    conn=get_conn();cache=ModelCache(conn)
    try:
        ctx='test-'+uuid4().hex
        events=pd.DataFrame([{'team_id':team,'team_name':str(team),'league':league,
            'event_name':'win','event_flag':flag} for team,league in [(900001,'A'),(900002,'B')]
            for flag in (1,1,0)])
        rates=pd.DataFrame([{'league':league,'event_name':'win','base_rate':.5} for league in ('A','B')])
        with active_cache(cache):
            first=_cached_beliefs(events,rates,15,ctx)
            second=_cached_beliefs(events,rates,15,ctx)
            assert first==second and cache.hits==2 and cache.misses==2
            rates.loc[rates.league=='A','base_rate']=.7
            changed=_cached_beliefs(events,rates,15,ctx)
            assert cache.hits==3 and cache.misses==3
            assert first[0].posterior!=changed[0].posterior
            assert first[1]==changed[1]
    finally:conn.rollback();conn.close()
