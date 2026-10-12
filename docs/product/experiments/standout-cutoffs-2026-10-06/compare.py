"""Re-run from repository root. Uses frozen inputs; does not query or mutate DB."""
import copy
import gzip
import csv
import json
import math
from collections import Counter
from pathlib import Path
import engine.eval.consensus_slips as engine

HERE = Path(__file__).resolve().parent
SECTIONS = ['strength', 'goals', 'patterns']

def run():
    source = json.loads(gzip.decompress((HERE / 'source.json.gz').read_bytes()))
    context = json.loads((HERE / 'strength-context.json').read_text())
    fixtures = source['fixture_analysis']
    assert len({f['fixture_id'] for f in fixtures}) == len(fixtures)
    rates = {tuple(r['key']): r['value'] for r in context['rates']}
    shortlist = {r['fixture_id']: r for r in source['elo_gap_shortlist']}
    winners, rows = [], []
    dates = sorted({f['kickoff'][:10] for f in fixtures})
    for f in fixtures:
        candidates = engine.discover_candidates(f, shortlist.get(f['fixture_id']), context['group'], rates)
        fav = engine._fixture_favourite(f, shortlist.get(f['fixture_id']))
        for c in candidates:
            if c['list'] == 'patterns':
                assert 0 <= c['k'] <= c['n'] and 0 <= c['base_rate'] <= 1
                expected = (c['k']/c['n'] - c['base_rate']) * c['n']/(c['n']+15)
                assert math.isclose(expected, c['score'], abs_tol=1e-12)
            elif c['list'] == 'goals':
                assert math.isclose(c['score'], (c['probability']-c['league_rate'])*0.76, abs_tol=0.0001)
        ranked = sorted(candidates, key=lambda c:(c['score'], -engine._DISCOVER_PRIORITY[c['list']], c.get('team')==fav), reverse=True)
        meta = {k:f.get(k) for k in ['fixture_id','home','away','league','country','kickoff']}
        for c in candidates:
            rows.append({**meta, **c, 'score_pp':c['score']*100})
        if ranked and ranked[0]['score'] > 0:
            winners.append({**meta, **ranked[0], 'score_pp':ranked[0]['score']*100})
    winners.sort(key=lambda r:(-r['score'], r['fixture_id']))
    summary, daily = [], []
    for cutoff in (5,10,15):
        selected = [r for r in winners if r['score_pp'] >= cutoff]
        counts = Counter(r['list'] for r in selected)
        qualifying = {s:len({r['fixture_id'] for r in rows if r['list']==s and r['score_pp']>=cutoff}) for s in SECTIONS}
        summary.append({'cutoff_pp':cutoff,'unique_fixtures':len(selected),**{s:counts[s] for s in SECTIONS},'qualifying_before_cross_section_selection':qualifying})
        for date in dates:
            chosen=[r for r in selected if r['kickoff'][:10]==date]
            daily.append({'cutoff_pp':cutoff,'date_utc':date,'available_fixtures':sum(f['kickoff'][:10]==date for f in fixtures),'standouts':len(chosen),**{s:sum(r['list']==s for r in chosen) for s in SECTIONS}})
    assert all(summary[i]['unique_fixtures'] >= summary[i+1]['unique_fixtures'] for i in range(2))
    for s in summary: assert sum(s[k] for k in SECTIONS)==s['unique_fixtures']
    variants=[]
    # Isolate prior change versus removing the rating gate on identical inputs.
    original_prior=engine.DISCOVER_PATTERN_PRIOR_MATCHES
    original_cutoff=engine.STANDOUT_MIN_SCORE
    engine.STANDOUT_MIN_SCORE=0  # Replay all three experimental cutoffs, not deployed default.
    try:
        for prior,gate in [(20,True),(15,True),(15,False)]:
            fs=copy.deepcopy(fixtures)
            if gate:
                for f in fs: f['bayesian']=[p for p in f.get('bayesian',[]) if p.get('display_eligible')]
            engine.DISCOVER_PATTERN_PRIOR_MATCHES=prior
            engine.attach_discover_picks(fs,copy.deepcopy(source['elo_gap_shortlist']),context['group'],rates)
            variants.append({'prior':prior,'prediction_gate':gate,'counts':{str(c):sum(bool(f['discover_pick']) and f['discover_pick']['score'] >= c/100 for f in fs) for c in (5,10,15)}})
    finally:
        engine.DISCOVER_PATTERN_PRIOR_MATCHES=original_prior
        engine.STANDOUT_MIN_SCORE=original_cutoff
    result={'source_generated':source['generated'],'fixtures':len(fixtures),'date_range':[dates[0],dates[-1]],'summary':summary,'variants':variants,'daily':daily,'top15':winners[:15]}
    (HERE/'results.json').write_text(json.dumps(result,indent=2))
    (HERE/'candidates.json.gz').write_bytes(gzip.compress(json.dumps(rows).encode(), mtime=0))
    fields=['fixture_id','home','away','league','country','kickoff','list','event','market','team','context','k','n','base_rate','probability','league_rate','score_pp']
    with (HERE/'ranked-fixtures.csv').open('w') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(winners)
    print(json.dumps({k:result[k] for k in ['source_generated','fixtures','date_range','summary','variants']},indent=2))
    return result

if __name__ == '__main__': run()
