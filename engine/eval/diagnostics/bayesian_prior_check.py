"""Read-only chronological comparison of raw Bayesian prior strengths.
Run: PYTHONPATH=. python -m engine.eval.diagnostics.bayesian_prior_check
No production parameters or calibration maps are changed.
"""
import json, os, shlex, hashlib
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd
from engine.eval.db import get_conn

OUT = Path('docs/product/experiments/bayesian-prior-2026-10-05')
EVENTS = ['score_1goal','score_2goals','concede_1','clean_sheet','Over_1_5','Over_2_5','Over_3_5','Under_2_5','Under_3_5','win','loss','unbeaten','BTTS']
MODELS = ['fixed_3','fixed_5','fixed_10','fixed_15','half_n','league_only','team_only']
# No exact complementary pairs in this sensitivity subset.
UNIQUE = ['score_1goal','score_2goals','concede_1','Over_1_5','Over_2_5','Over_3_5','win','loss','BTTS']

def flags(gf, ga):
 t=gf+ga
 return np.array([gf>=1,gf>=2,ga>=1,ga==0,t>1.5,t>2.5,t>3.5,t<2.5,t<3.5,gf>ga,gf<ga,gf>=ga,gf>0 and ga>0], dtype=float)

def predict(k,n,b):
 return np.stack([(k+s*b)/(n+s) for s in [3,5,10,15,n/2]] + [b,k/n],axis=-1)

def metrics(d):
 y=d.y.to_numpy()[:,None];p=d[MODELS].to_numpy();pc=np.clip(p,1e-6,1-1e-6)
 bs=((p-y)**2).mean(axis=0);ll=(-(y*np.log(pc)+(1-y)*np.log(1-pc))).mean(axis=0)
 return [{'model':m,'brier':float(bs[i]),'log_loss':float(ll[i]),'mean_probability':float(p[:,i].mean()),'observed_rate':float(y.mean()),'rows':len(d)} for i,m in enumerate(MODELS)]

def main():
 # Only DB settings loaded; credentials never printed or persisted.
 for line in Path('.env').read_text().splitlines():
  if '=' in line and not line.lstrip().startswith('#'):
   k,v=line.split('=',1)
   if k.strip().startswith('DB_'):
    a=shlex.split(v);os.environ.setdefault(k.strip(),a[0] if a else '')
 conn=get_conn();conn.set_session(readonly=True)
 sql="""SELECT fixture_id,api_league_id,season,kickoff_utc,home_team_id,away_team_id,
 home_team_fulltime_goal,away_team_fulltime_goal FROM raw.raw_fixtures
 WHERE fixture_status='FT' AND kickoff_utc>='2020-01-01' AND kickoff_utc<'2026-10-05'
 AND home_team_fulltime_goal IS NOT NULL AND away_team_fulltime_goal IS NOT NULL
 ORDER BY kickoff_utc,fixture_id"""
 with conn.cursor() as q:
  q.execute(sql);data=q.fetchall()
 conn.close();OUT.mkdir(parents=True,exist_ok=True)
 raw=pd.DataFrame(data,columns=['fixture_id','league','season','kickoff','home','away','hg','ag'])
 if raw.fixture_id.duplicated().any():raise ValueError('Duplicate fixture IDs')
 if raw[['league','season','home','away']].isna().any().any():raise ValueError('Missing identity')
 if ((raw.hg<0)|(raw.ag<0)|(raw.home==raw.away)).any():raise ValueError('Invalid score/team pair')
 raw['date']=pd.to_datetime(raw.kickoff,utc=True).dt.strftime('%Y-%m-%d')
 raw.to_csv(OUT/'source-fixtures.csv.gz',index=False,compression='gzip')
 teams=defaultdict(lambda:[0,np.zeros(len(EVENTS))]);leagues=defaultdict(lambda:[0,np.zeros(len(EVENTS))]);records=[]
 # All matches on a UTC date are predicted before any of that day's results update history.
 for day,group in raw.groupby('date',sort=True):
  pending=[]
  for r in group.itertuples(index=False):
   for team,venue,gf,ga in [(r.home,'home',r.hg,r.ag),(r.away,'away',r.ag,r.hg)]:
    y=flags(gf,ga);key=(r.league,r.season,team)
    ctx=venue if teams[key+(venue,)][0]>=6 else 'all'
    n,k=teams[key+(ctx,)];ln,lk=leagues[(r.league,r.season,ctx)]
    if day>='2024-01-01' and n>=3 and ln>=20:
     b=lk/ln;p=predict(k,n,b)
     for i,event in enumerate(EVENTS):
      records.append([r.fixture_id,r.league,r.season,day,team,ctx,n,event,y[i],b[i],*p[i]])
    pending.append((key,venue,y))
  for key,venue,y in pending:
   for ctx in ['all',venue]:
    tk=key+(ctx,);lk=key[:2]+(ctx,)
    teams[tk][0]+=1;teams[tk][1]+=y;leagues[lk][0]+=1;leagues[lk][1]+=y
 d=pd.DataFrame(records,columns=['fixture_id','league','season','date','team','context','n','event','y','base_rate',*MODELS])
 d['split']=np.where(d.date<'2025-01-01','validation','test')
 d['sample']=pd.cut(d.n,[2,5,9,19,float('inf')],labels=['3-5','6-9','10-19','20+'])
 d.to_csv(OUT/'predictions.csv.gz',index=False,compression='gzip')
 summary={s:metrics(g) for s,g in d.groupby('split')}
 candidates=summary['validation'][:5]
 selected=min(candidates,key=lambda x:(x['brier'],x['log_loss']))['model']
 cuts=[]
 for split,sd in d.groupby('split'):
  for dim in ['event','sample','context','league']:
   for value,g in sd.groupby(dim,observed=True):
    cuts += [dict(split=split,dimension=dim,value=str(value),**m) for m in metrics(g)]
 pd.DataFrame(cuts).to_csv(OUT/'metric-slices.csv',index=False)
 bins=[]
 for split,sd in d.groupby('split'):
  for m in MODELS:
   for b,g in sd.groupby(np.minimum((sd[m]*10).astype(int),9)):
    bins.append(dict(split=split,model=m,bin=int(b),rows=len(g),predicted=float(g[m].mean()),observed=float(g.y.mean())))
 pd.DataFrame(bins).to_csv(OUT/'calibration-bins.csv',index=False)
 t=d[d.split=='test'].copy();clusters=t.groupby(['league','season'])
 rng=np.random.default_rng(20261005);intervals={}
 for m in ['half_n',selected]:
  delta=(t[m]-t.y)**2-(t.fixed_15-t.y)**2
  cl=pd.DataFrame({'league':t.league,'season':t.season,'delta':delta}).groupby(['league','season']).delta.agg(['sum','count']).to_numpy()
  ix=rng.integers(0,len(cl),size=(2000,len(cl)));res=cl[ix,0].sum(1)/cl[ix,1].sum(1)
  intervals[m]={'brier_delta_vs_15':float(delta.mean()),'cluster_bootstrap_95pct':np.quantile(res,[.025,.975]).tolist(),'clusters':len(cl)}
 result={'source_rows':len(raw),'source_leagues':int(raw.league.nunique()),'source_start':raw.date.min(),'source_end':raw.date.max(),'source_hash':hashlib.sha256(raw.to_csv(index=False).encode()).hexdigest(),'selected_on_validation':selected,'summary':summary,'holdout_intervals':intervals,'unique_events_test':metrics(t[t.event.isin(UNIQUE)]),'coverage':{s:{'fixtures':int(g.fixture_id.nunique()),'team_contexts':int(len(g)//len(EVENTS)),'leagues':int(g.league.nunique()),'start':g.date.min(),'end':g.date.max()} for s,g in d.groupby('split')}}
 (OUT/'results.json').write_text(json.dumps(result,indent=2));(OUT/'source-query.sql').write_text(sql)
 print(json.dumps(result,indent=2))

if __name__=='__main__':main()
