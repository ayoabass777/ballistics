"""Gap-aware goals calibration proposal (2026-10-17): blend calibrated chance with league rate in logit space,
fitted per market on earlier folds only, scored on later folds. Diagnostic only; not used by the engine.
Run from repo root: python -m engine.eval.diagnostics.goals_gap_calibration
"""
import sys, numpy as np, collections
sys.path.insert(0,'.')
src=open('engine/eval/diagnostics/discover_weight_check.py').read()
src=src.split('# ── PATTERNS')[0].replace("goals.append(dict(claim=prob - base,","goals.append(dict(mk=p['market'], claim=prob - base,")
exec(src)
R=[dict(r, y=r['real']+r['base']) for r in goals]
for r in R:
    m=r['mk']; r['fam']= m if m.startswith('Match') else ('Team 1+' if m.endswith('Over 0.5') else 'Team 2+')
lg=lambda p: np.log(np.clip(p,1e-4,1-1e-4)/(1-np.clip(p,1e-4,1-1e-4)))
sig=lambda z: 1/(1+np.exp(-z))
def fitlog(X,y,l2=1.0,it=60):
    w=np.zeros(X.shape[1]); w[1]=1.0
    for _ in range(it):
        p=sig(X@w); W=p*(1-p); H=X.T@(X*W[:,None])+l2*np.eye(X.shape[1]); H[0,0]-=l2*0.999
        g=X.T@(y-p)-l2*(w-np.r_[0,1,0]); g[0]+=l2*0.999*w[0]*0  # prior: slope 1, league 0
        w+=np.linalg.solve(H,g)
    return w
feat=lambda rs: np.c_[np.ones(len(rs)), lg(np.array([r['prob'] for r in rs])), lg(np.array([r['base'] for r in rs]))]
folds=sorted({r['fold'] for r in R}); ev=[]
coefs=collections.defaultdict(list)
for fo in folds:
    tr=[r for r in R if r['fold']<fo]; te=[r for r in R if r['fold']==fo]
    if len(tr)<1500: continue
    pooled=fitlog(feat(tr),np.array([r['y'] for r in tr]))
    for fam in {r['fam'] for r in te}:
        t2=[r for r in tr if r['fam']==fam]; e2=[r for r in te if r['fam']==fam]
        w=fitlog(feat(t2),np.array([r['y'] for r in t2])) if len(t2)>=300 else pooled
        coefs[fam].append(w)
        for r,p in zip(e2, sig(feat(e2)@w)): r['adj']=float(p); ev.append(r)
print(f"evaluated rows={len(ev)} folds={sorted({r['fold'] for r in ev})}")
print("\nFitted blend per market (last fold): adj = sigmoid(a + b·logit(chance) + c·logit(league))")
for fam,ws in sorted(coefs.items()):
    w=ws[-1]; print(f"  {fam:16s} b(chance)={w[1]:.2f}  c(league)={w[2]:+.2f}")
def scores(key):
    y=np.array([r['y'] for r in ev]); p=np.array([r[key] for r in ev])
    br=np.mean((p-y)**2); ll=-np.mean(y*np.log(p)+(1-y)*np.log(1-p)); return br,ll
for k in ('prob','adj'): b,l=scores(k); print(f"{'current' if k=='prob' else 'gap-aware':10s} Brier {b:.4f}  log loss {l:.4f}")
band=lambda g: 'gap <5' if g<.05 else 'gap 5-15' if g<.15 else 'gap 15+'
print("\nResidual (hit - claimed) by ORIGINAL gap band:")
for b in ['gap <5','gap 5-15','gap 15+']:
    t=[r for r in ev if band(r['claim'])==b]; y=np.mean([r['y'] for r in t])
    print(f"  {b:9s} n={len(t):5d}  current claim {np.mean([r['prob'] for r in t]):.1%} res {100*(y-np.mean([r['prob'] for r in t])):+.1f} | gap-aware claim {np.mean([r['adj'] for r in t]):.1%} res {100*(y-np.mean([r['adj'] for r in t])):+.1f}")
print("\nBy market at gap 15+ (residual current -> gap-aware):")
for fam in sorted({r['fam'] for r in ev}):
    t=[r for r in ev if r['fam']==fam and r['claim']>=.15]
    if len(t)>=30: y=np.mean([r['y'] for r in t]); print(f"  {fam:16s} n={len(t):4d}  {100*(y-np.mean([r['prob'] for r in t])):+.1f} -> {100*(y-np.mean([r['adj'] for r in t])):+.1f}")
# Discover simulation: per fixture, best gap among rows with chance >= bar
print("\nDiscover-style pick per fixture (max gap, chance >= bar):")
for bar in (.65,.75):
  for k,lab in (('prob','current'),('adj','gap-aware')):
    by=collections.defaultdict(list)
    for r in ev:
        if r[k]>=bar: by[r['fid']].append(r)
    P=[max(v,key=lambda r:r[k]-r['base']) for v in by.values()]
    top=sorted(P,key=lambda r:-(r[k]-r['base']))[:max(1,len(P)//10)]
    for nm,S in (('all picks',P),('top 10%',top)):
        cl=np.mean([r[k] for r in S]); hit=np.mean([r['y'] for r in S]); gc=np.mean([r[k]-r['base'] for r in S]); gr=np.mean([r['y']-r['base'] for r in S])
        print(f"  {bar:.0%} {lab:9s} {nm:9s} n={len(S):5d} claim {cl:.1%} hit {hit:.1%} (res {100*(hit-cl):+.1f}) | gap claimed {100*gc:+.1f} realised {100*gr:+.1f}")
