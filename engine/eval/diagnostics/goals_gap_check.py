"""Goals gap check (2026-10-17): at the same calibrated chance, do high-gap picks hit more or less than claimed?

Walk-forward, prior-only league rates; reuses discover_weight_check setup.
Run from repo root: python -m engine.eval.diagnostics.goals_gap_check
"""
import sys, numpy as np, collections
sys.path.insert(0,'.')
src=open('engine/eval/diagnostics/discover_weight_check.py').read()
src=src.split('# ── PATTERNS')[0].replace("goals.append(dict(claim=prob - base,","goals.append(dict(mk=p['market'], claim=prob - base,")
exec(src)
R=[dict(r, y=r['real']+r['base']) for r in goals]
for r in R:
    m=r['mk']; r['fam']= m if m.startswith('Match') else ('Team 1+' if m.endswith('Over 0.5') else 'Team 2+')
print(f"rows={len(R)} fixtures={len({r['fid'] for r in R})}  (walk-forward calibrated, prob>=65%, prior-only league rate, >=20 league games)")
def band(g): return 'gap <5' if g<.05 else 'gap 5-15' if g<.15 else 'gap 15+'
B=['gap <5','gap 5-15','gap 15+']
print("\nSame chance, split by gap: hit rate vs claimed (residual = hit - claimed)")
for lo,hi in [(.65,.70),(.70,.75),(.75,.80),(.80,.85),(.85,1.01)]:
    s=[r for r in R if lo<=r['prob']<hi]
    line=f" chance {lo:.0%}-{min(hi,1):.0%}: "
    for b in B:
        t=[r for r in s if band(r['claim'])==b]
        if len(t)>=30: line+=f"| {b}: n={len(t):5d} claim {np.mean([r['prob'] for r in t]):.1%} hit {np.mean([r['y'] for r in t]):.1%} res {100*(np.mean([r['y'] for r in t])-np.mean([r['prob'] for r in t])):+.1f} "
        else: line+=f"| {b}: n={len(t)} "
    print(line)
# pooled residual by gap band
print("\nPooled across chance bins:")
for b in B:
    t=[r for r in R if band(r['claim'])==b]
    print(f" {b}: n={len(t)} avg claim {np.mean([r['prob'] for r in t]):.1%} hit {np.mean([r['y'] for r in t]):.1%} residual {100*(np.mean([r['y'] for r in t])-np.mean([r['prob'] for r in t])):+.1f} pts")
# logistic: y ~ logit(prob) + gap ; bootstrap by fixture
def fitlog(X,y,it=50):
    w=np.zeros(X.shape[1])
    for _ in range(it):
        p=1/(1+np.exp(-X@w)); W=p*(1-p); H=X.T@(X*W[:,None])+1e-6*np.eye(X.shape[1]); w+=np.linalg.solve(H,X.T@(y-p))
    return w
lp=lambda p: np.log(np.clip(p,1e-4,1-1e-4)/(1-np.clip(p,1e-4,1-1e-4)))
X=np.c_[np.ones(len(R)), lp(np.array([r['prob'] for r in R])), np.array([r['claim'] for r in R])]
y=np.array([r['y'] for r in R]); w=fitlog(X,y)
fids=np.array([r['fid'] for r in R]); uf=np.unique(fids); idx={f:np.where(fids==f)[0] for f in uf}
rng=np.random.default_rng(7); bs=[]
for _ in range(200):
    pk=np.concatenate([idx[f] for f in rng.choice(uf,len(uf))]); bs.append(fitlog(X[pk],y[pk])[2])
lo,hi=np.percentile(bs,[5,95])
print(f"\nLogistic: hit ~ chance + gap.  gap coefficient {w[2]:+.2f} (90% CI {lo:+.2f} to {hi:+.2f}); chance slope {w[1]:.2f}")
print(" (gap coef > 0 with CI above 0 = gap adds information beyond the chance; ~0 = gap is ranking only)")
print("\nBy market family (residual by gap band):")
for fam in sorted({r['fam'] for r in R}):
    s=[r for r in R if r['fam']==fam]; out=f" {fam:16s} n={len(s):5d} "
    for b in B:
        t=[r for r in s if band(r['claim'])==b]
        out+=f"| {b}: " + (f"n={len(t)} res {100*(np.mean([r['y'] for r in t])-np.mean([r['prob'] for r in t])):+.1f}" if len(t)>=30 else f"n={len(t)}") + " "
    print(out)
print("\nAbove-league check (claimed gap vs realised gap):")
for b in B:
    t=[r for r in R if band(r['claim'])==b]
    print(f" {b}: claimed {100*np.mean([r['claim'] for r in t]):+.1f} pts  realised {100*np.mean([r['real'] for r in t]):+.1f} pts")
