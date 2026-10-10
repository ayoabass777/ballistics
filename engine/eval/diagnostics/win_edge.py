"""Wins: DC vs Mismatch vs league baseline, and whether DC adds information.

Diagnostic for docs/product/numbers-audit-2026-10-03.md (§5).
Input: raw walk-forward dump from
    python -m engine.eval.calibration_wf --no-save-maps --dump-raw data/raw_preds.json
Run:
    python -m engine.eval.diagnostics.win_edge [data/raw_preds.json]
"""
import sys
RAW = sys.argv[1] if len(sys.argv) > 1 else "data/raw_preds.json"
import json
from collections import defaultdict
from engine.eval.calibration_wf import _fit_map, _rows_for_key
from engine.eval.calibration_maps import apply_points, fit_league_logit_adjustments, apply_logit_adjustment
P=[dict(p,actual=float(p['actual'])) for p in json.load(open(RAW)) if p.get('actual') is not None]
FIT=lambda p:p['fold']<=4
# --- every fixture with a known score -> league home/away win baseline (fit folds only)
fx={}
for p in P:
    if p.get('score') and p['fixture_id'] not in fx:
        h,a=map(int,p['score'].split('-')); fx[p['fixture_id']]=dict(lg=p.get('api_league_id'),home=p['home'],away=p['away'],hw=h>a,aw=a>h,fold=p['fold'])
fitfx=[f for f in fx.values() if f['fold']<=4]
g_h=sum(f['hw'] for f in fitfx)/len(fitfx); g_a=sum(f['aw'] for f in fitfx)/len(fitfx)
L=defaultdict(lambda:[0,0,0])
for f in fitfx: L[f['lg']][0]+=1; L[f['lg']][1]+=f['hw']; L[f['lg']][2]+=f['aw']
K=20
def base(fid,team):
    f=fx[fid]; n,hw,aw=L[f['lg']]
    return (hw+K*g_h)/(n+K) if team==f['home'] else (aw+K*g_a)/(n+K)
print(f"fixtures with scores: {len(fx)} (fit folds {len(fitfx)}); global home win {g_h:.1%}, away win {g_a:.1%}\n")
win=lambda p:p['market'].endswith(' Win')
# --- DC: per-market map fitted on fit folds
dc_map=_fit_map(_rows_for_key([p for p in P if FIT(p)],'dixon_coles_team_win'),'dixon_coles_team_win')
# --- Mismatch: map + league offsets fitted on fit folds (as live engine)
mm_fit=[p for p in P if FIT(p) and p['model']=='mismatch']
mm_map=_fit_map(mm_fit,'mismatch')
mm_adj=fit_league_logit_adjustments(mm_fit,mm_map,min_n=30,prior_strength=200)
def mm_cal(p,league=True):
    c=apply_points(mm_map,p['predicted_prob_raw'])
    if league and p.get('api_league_id') is not None: c=apply_logit_adjustment(c,mm_adj.get(str(int(p['api_league_id']))))
    return c
ev=[p for p in P if not FIT(p) and win(p) and p['fixture_id'] in fx]
DC={(p['fixture_id'],p['market']):dict(y=p['actual'],m=apply_points(dc_map,p['predicted_prob_raw']),raw=p['predicted_prob_raw'],b=base(p['fixture_id'],p['market'][:-4])) for p in ev if p['model']=='dixon_coles'}
MM={(p['fixture_id'],p['market']):dict(y=p['actual'],m=mm_cal(p),m0=mm_cal(p,False),raw=p['predicted_prob_raw'],b=base(p['fixture_id'],p['market'][:-4])) for p in ev if p['model']=='mismatch'}
def auc(rows,k):
    pos=[r[k] for r in rows if r['y']]; neg=[r[k] for r in rows if not r['y']]
    if not pos or not neg: return float('nan')
    s=sum((x>z)+0.5*(x==z) for x in pos for z in neg); return s/(len(pos)*len(neg))
def rep(name,rows,k='m'):
    n=len(rows); bm=sum((r[k]-r['y'])**2 for r in rows)/n; bb=sum((r['b']-r['y'])**2 for r in rows)/n
    sh=sum(r[k] for r in rows)/n; ac=sum(r['y'] for r in rows)/n; bs=sum(r['b'] for r in rows)/n
    print(f"  {name:34s} n={n:4d} actual {ac:5.1%} | model {sh:5.1%} (gap {100*(sh-ac):+5.1f}) Brier {bm:.4f} AUC {auc(rows,k):.3f} | league baseline {bs:5.1%} Brier {bb:.4f} AUC {auc(rows,'b'):.3f} | skill vs baseline {100*(1-bm/bb):+5.1f}%")
print("A. Each model on everything it priced (held-out folds 5-9)")
rep("DC win, all",list(DC.values()))
rep("Mismatch win, map only",list(MM.values()),'m0')
rep("Mismatch win, map + league offset",list(MM.values()))
print("\nB. What users would see")
rep("DC win shown >=65%",[r for r in DC.values() if r['m']>=0.65])
rep("Mismatch win raw>=70% (map only)",[r for r in MM.values() if r['raw']>=0.70],'m0')
rep("Mismatch win raw>=70% (+league)",[r for r in MM.values() if r['raw']>=0.70])
both=sorted(set(DC)&set(MM))
print(f"\nC. Same fixtures (n={len(both)}): does DC add anything beyond Mismatch?")
rows=[dict(y=DC[k]['y'],dc=DC[k]['m'],mm=MM[k]['m'],b=DC[k]['b']) for k in both]
rep("DC",[dict(r,m=r['dc']) for r in rows]); rep("Mismatch (+league)",[dict(r,m=r['mm']) for r in rows])
# incremental: within Mismatch bands, split by DC above/below band median
bands=[(0,.70),(.70,.78),(.78,1.01)]
print("  Within Mismatch bands, win rate when DC is higher vs lower than the band median:")
for lo,hi in bands:
    sub=[r for r in rows if lo<=r['mm']<hi]
    if len(sub)<30: continue
    med=sorted(r['dc'] for r in sub)[len(sub)//2]
    hi_=[r for r in sub if r['dc']>=med]; lo_=[r for r in sub if r['dc']<med]
    print(f"    Mismatch {lo:.0%}-{min(hi,1):.0%} n={len(sub):3d}: DC higher → {sum(r['y'] for r in hi_)/len(hi_):5.1%} (n={len(hi_)}), DC lower → {sum(r['y'] for r in lo_)/len(lo_):5.1%} (n={len(lo_)})")
print(f"\nMismatch league offsets fitted: {len(mm_adj)} leagues")
