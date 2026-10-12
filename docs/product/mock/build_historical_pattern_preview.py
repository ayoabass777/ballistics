import json
from pathlib import Path
source=Path('engine/eval/consensus_slips_2026-10-02.json')
d=json.loads(source.read_text());out=[]
labels={'score_1goal':'Scored at least once','score_2goals':'Scored 2+ goals','concede_1':'Conceded at least once','concede_2':'Conceded 2+ goals','clean_sheet':'Kept a clean sheet','goalless':'Failed to score','win':'Won','loss':'Lost','unbeaten':'Unbeaten','winless':'Winless','draw':'Drew','BTTS':'Both teams scored'}
for kind in ['Over','Under']:
 for n in [1,2,3,4]:labels[f'{kind}_{n}_5']=f'{kind} {n}.5 total goals'
pairs=[('loss','unbeaten'),('win','winless'),('concede_1','clean_sheet'),('score_1goal','goalless')]+[(f'Over_{n}_5',f'Under_{n}_5') for n in [1,2,3,4]]
# The first event implies the second; suppress weaker statement only if counts match.
implications=[('score_2goals','score_1goal'),('score_2goals','Over_1_5'),('win','unbeaten'),('concede_2','concede_1')]+[(f'Over_{n+1}_5',f'Over_{n}_5') for n in [1,2,3]]+[(f'Under_{n}_5',f'Under_{n+1}_5') for n in [1,2,3]]
def walk(x):
 if isinstance(x,dict):
  if x.get('fixture_id') in [81805,85316] and 'bayesian' in x:
   rows={r['event']:r.copy() for r in x['bayesian'] if r.get('side')=='home' and r.get('primary') and r['n']>=6 and r['event'] in labels}
   removed=[]
   for a,b in pairs:
    if a in rows and b in rows:
     ra,rb=rows[a],rows[b]
     if (ra['context'],ra['n'])==(rb['context'],rb['n']):
      loser=b if ra['k']>=rb['k'] else a;removed.append(loser);del rows[loser]
   for a,b in implications:
    if a in rows and b in rows and (rows[a]['k'],rows[a]['n'],rows[a]['context'])==(rows[b]['k'],rows[b]['n'],rows[b]['context']):
     removed.append(b);del rows[b]
   ranked=sorted(rows.values(),key=lambda r:(-r['k']/r['n'],-(r['k']/r['n']-r['base_rate']),r['event']))
   for r in ranked:
    gap=r['k']/r['n']-r['base_rate'];r['label']=labels[r['event']];r['gap']=gap
    r['comparison']='Much more frequent than the league norm' if gap>=.15 else 'More frequent than the league norm' if gap>=.05 else 'Typical for this league' if gap>-.05 else 'Less frequent than the league norm' if gap>-.15 else 'Much less frequent than the league norm'
   out.append({'fixture_id':x['fixture_id'],'home':x['home'],'away':x['away'],'selected':ranked[:6],'remaining_events':[r['event'] for r in ranked[6:]],'suppressed_events':removed});return
  for v in x.values():walk(v)
 elif isinstance(x,list):
  for v in x:walk(v)
walk(d)
p=Path('docs/product/mock/historical-pattern-preview-2026-10-05.json');p.write_text(json.dumps({'source':str(source),'scope':'Two-fixture home-team primary-context preview; no posterior gate; n>=6; rate descending then signed raw league gap; max6. Not production selection.','fixtures':out},indent=2))
print(json.dumps(out))
