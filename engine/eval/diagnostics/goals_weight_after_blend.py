"""D71 goals weight re-check after the D127 league blend (2026-10-17).

Walk-forward: calibration maps and blend weights fitted on earlier folds only;
league rate from prior fixtures only. Weight = realised gap / claimed gap
(through the origin, positive claims), bootstrap by fixture.
Run from repo root: python -m engine.eval.diagnostics.goals_weight_after_blend
"""
import sys, collections, numpy as np
sys.path.insert(0, '.')
src = open('engine/eval/diagnostics/discover_weight_check.py').read().split('# ── PATTERNS')[0]
src = src.replace("goals.append(dict(claim=prob - base,", "goals.append(dict(mk=p['market'], claim=prob - base,")
src = src.replace("if prob < 0.65: continue", "if prob < 0.60: continue")
exec(src)
from engine.eval.calibration_maps import dc_calibration_key
from engine.eval.goals_league_blend import BLENDED_KEYS, fit_weights, blend_probability

R = [dict(r, y=r['real'] + r['base'], key=dc_calibration_key(r['mk'])) for r in goals]
folds = sorted({r['fold'] for r in R}); ev = []
for fo in folds:
    tr = [r for r in R if r['fold'] < fo]; te = [r for r in R if r['fold'] == fo]
    if len(tr) < 1500: continue
    W = {}
    for k in BLENDED_KEYS:
        w = fit_weights([dict(prob=r['prob'], league=r['base'], y=r['y']) for r in tr if r['key'] == k])
        if w: W[k] = w
    for r in te:
        b = blend_probability(r['mk'], r['prob'], r['base'], W)
        r['adj'] = b if b is not None else r['prob']; ev.append(r)
print(f"rows {len(ev)} (folds {sorted({r['fold'] for r in ev})})")

def weight(rows, key):
    rows = [r for r in rows if r[key] - r['base'] > 0]
    if len(rows) < 50: return None
    c = np.array([r[key] - r['base'] for r in rows]); y = np.array([r['y'] - r['base'] for r in rows])
    s = (c * y).sum() / (c * c).sum()
    f = np.array([r['fid'] for r in rows]); uf = np.unique(f); ix = {u: np.where(f == u)[0] for u in uf}
    rng = np.random.default_rng(7); bs = []
    for _ in range(300):
        p = np.concatenate([ix[u] for u in rng.choice(uf, len(uf))]); bs.append((c[p] * y[p]).sum() / (c[p] ** 2).sum())
    lo, hi = np.percentile(bs, [5, 95]); return s, lo, hi, len(rows), c.mean() * 100, y.mean() * 100

def fam(m): return m if m.startswith('Match') else ('Team 1+' if m.endswith('Over 0.5') else 'Team 2+')
for bar in (0.65, 0.70):
    print(f"\n== chance >= {bar:.0%} (standout goals bar is 70%)")
    for key, lab in (('prob', 'current (no blend)'), ('adj', 'with blend')):
        rows = [r for r in ev if r[key] >= bar]
        w = weight(rows, key)
        print(f"  {lab:18s} weight {w[0]:.2f} [90% {w[1]:.2f}-{w[2]:.2f}]  n={w[3]}  claimed {w[4]:+.1f} realised {w[5]:+.1f}")
        # standout-sized: claimed gap big enough to matter
        big = [r for r in rows if r[key] - r['base'] >= 0.15]
        wb = weight(big, key)
        if wb: print(f"  {'':18s} gap>=15 only: weight {wb[0]:.2f} [{wb[1]:.2f}-{wb[2]:.2f}] n={wb[3]} claimed {wb[4]:+.1f} realised {wb[5]:+.1f}")
    print("  by market (with blend):")
    for fm in sorted({fam(r['mk']) for r in ev}):
        w = weight([r for r in ev if fam(r['mk']) == fm and r['adj'] >= bar], 'adj')
        if w: print(f"    {fm:16s} weight {w[0]:.2f} [{w[1]:.2f}-{w[2]:.2f}] n={w[3]}")
