"""Independently reconstruct sampled replay predictions using dataframe filters."""
from pathlib import Path
import json
import pandas as pd
import numpy as np
P=Path('docs/product/experiments/bayesian-prior-2026-10-05')
r=pd.read_csv(P/'source-fixtures.csv.gz')
d=pd.read_csv(P/'predictions.csv.gz')
h=r[['fixture_id','league','season','date','home','hg','ag']].rename(columns={'home':'team','hg':'gf','ag':'ga'}).assign(venue='home')
a=r[['fixture_id','league','season','date','away','ag','hg']].rename(columns={'away':'team','ag':'gf','hg':'ga'}).assign(venue='away')
x=pd.concat([h,a],ignore_index=True)
e={
 'score_1goal':lambda z:z.gf>=1,'score_2goals':lambda z:z.gf>=2,
 'concede_1':lambda z:z.ga>=1,'clean_sheet':lambda z:z.ga==0,
 'Over_1_5':lambda z:z.gf+z.ga>=2,'Over_2_5':lambda z:z.gf+z.ga>=3,
 'Over_3_5':lambda z:z.gf+z.ga>=4,'Under_2_5':lambda z:z.gf+z.ga<=2,
 'Under_3_5':lambda z:z.gf+z.ga<=3,'win':lambda z:z.gf>z.ga,
 'loss':lambda z:z.gf<z.ga,'unbeaten':lambda z:z.gf>=z.ga,
 'BTTS':lambda z:(z.gf>0)&(z.ga>0)}
assert not d.duplicated(['fixture_id','team','event']).any()
assert d.groupby(['fixture_id','team']).event.nunique().eq(13).all()
assert d.n.ge(3).all()
for row in d.sample(130,random_state=42).itertuples():
 hist=x[(x.league==row.league)&(x.season==row.season)&(x.date<row.date)]
 actual=x[(x.fixture_id==row.fixture_id)&(x.team==row.team)]
 assert len(actual)==1
 teamhist=hist[hist.team==row.team];venue=actual.venue.iloc[0]
 ctx=venue if (teamhist.venue==venue).sum()>=6 else 'all'
 assert ctx==row.context
 if ctx!='all':hist=hist[hist.venue==ctx];teamhist=teamhist[teamhist.venue==ctx]
 n=len(teamhist);k=e[row.event](teamhist).sum();base=e[row.event](hist).mean()
 assert n==row.n and np.isclose(base,row.base_rate)
 assert int(e[row.event](actual).iloc[0])==row.y
 for name,s in [('fixed_3',3),('fixed_5',5),('fixed_10',10),('fixed_15',15),('half_n',n/2)]:
  assert np.isclose(getattr(row,name),(k+s*base)/(n+s))
 assert np.isclose(row.half_n,(2/3)*(k/n)+(1/3)*base)
result={'sampled_predictions_reconstructed':130,'uniqueness':'passed','event_completeness':'passed','minimum_sample':'passed','strict_prior_date_and_season_filters':'passed','context_selection':'passed','outcomes_and_probability_formulas':'passed'}
(P/'verification.json').write_text(json.dumps(result,indent=2));print(result)
