"""Mismatch calibration by market: live pooled map + league offsets vs per-market maps.

Diagnostic for docs/product/numbers-audit-2026-10-03.md (§5).
Input: raw walk-forward dump from
    python -m engine.eval.calibration_wf --no-save-maps --dump-raw data/raw_preds.json
Run:
    python -m engine.eval.diagnostics.mismatch_markets [data/raw_preds.json]
"""
import sys
RAW = sys.argv[1] if len(sys.argv) > 1 else "data/raw_preds.json"
import json
from collections import defaultdict
from engine.eval.calibration_maps import apply_points, fit_pava_map, fit_league_logit_adjustments, apply_logit_adjustment
P=[dict(p,actual=float(p['actual'])) for p in json.load(open(RAW)) if p.get('actual') is not None and p['model']=='mismatch']
fam=lambda m:'win' if m.endswith(' Win') else ('over_1_5' if m=='Match Over 1.5' else ('over_2_5' if m=='Match Over 2.5' else ('team_score' if m.endswith('Over 0.5') else 'team_over_1_5')))
fit=[p for p in P if p['fold']<=4]; ev=[p for p in P if p['fold']>4]
fp=lambda rows,mb: fit_pava_map([r['predicted_prob_raw'] for r in rows],[r['actual'] for r in rows],min_bin_n=mb)
pooled=fp(fit,50); padj=fit_league_logit_adjustments(fit,pooled,min_n=30,prior_strength=200)
per={f:(fp([r for r in fit if fam(r['market'])==f],150) or pooled) for f in {fam(p['market']) for p in P}}
print('fit sizes',{f:sum(1 for r in fit if fam(r['market'])==f) for f in per})
for f in ['win','over_1_5','over_2_5','team_score']:
    rows=[r for r in ev if fam(r['market'])==f and r['predicted_prob_raw']>=0.70]
    if not rows: continue
    a=sum(r['actual'] for r in rows)/len(rows)
    s1=sum(apply_logit_adjustment(apply_points(pooled,r['predicted_prob_raw']),padj.get(str(int(r['api_league_id'])) if r.get('api_league_id') is not None else '')) for r in rows)/len(rows)
    s2=sum(apply_points(per[f],r['predicted_prob_raw']) for r in rows)/len(rows)
    print(f"  {f:11s} shown set n={len(rows):4d} actual {a:5.1%} | live (pooled+league) {s1:5.1%} gap {100*(s1-a):+5.1f} | per-market map {s2:5.1%} gap {100*(s2-a):+5.1f}")
