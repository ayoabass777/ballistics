"""Team strength clustering v2 (2026-10-18): gap rule vs
  (2) data-chosen tiers  : per league, k-means on Elo with k in 3..5 picked by silhouette;
                           label fav in top cluster v opp in bottom cluster
  (3) multi-feature tiers: k-means k=3 on standardised [Elo, goals for/game, goals
                           against/game, last-8 points/game]; tiers ordered by composite
  (5) cluster distance   : k=3 Elo clusters; label when (fav cluster centre - opp cluster
                           centre) / league Elo SD >= d, for several d
Every rule is trust-gated like today (league's own prior record for that rule, >= 30
matches, Wilson 90% lower bound >= 60%; tier rules per venue). Walk-forward, earlier data only.
Run from repo root: python -m engine.eval.diagnostics.strength_clusters_v2
"""
import csv, collections, math
import numpy as np

Z = 1.645
def wl(k, n):
    if n == 0: return 0.0
    p = k / n; d = 1 + Z * Z / n
    return (p + Z * Z / (2 * n) - Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))) / d

def kmeans(X, k, iters=40):
    X = np.asarray(X, float)
    if X.ndim == 1: X = X[:, None]
    order = np.argsort(X[:, 0]); idx = order[np.linspace(0, len(X) - 1, k).astype(int)]
    C = X[idx].copy()
    for _ in range(iters):
        D = ((X[:, None, :] - C[None]) ** 2).sum(-1); lab = D.argmin(1)
        newC = np.array([X[lab == i].mean(0) if (lab == i).any() else C[i] for i in range(k)])
        if np.allclose(newC, C): break
        C = newC
    return lab, C

def silhouette(x, lab):
    x = np.asarray(x, float); k = lab.max() + 1
    if k < 2: return -1
    D = np.abs(x[:, None] - x[None]); s = []
    for i in range(len(x)):
        same = lab == lab[i]; same[i] = False
        if not same.any(): s.append(0); continue
        a = D[i, same].mean()
        b = min(D[i, lab == c].mean() for c in range(k) if c != lab[i] and (lab == c).any())
        s.append((b - a) / max(a, b))
    return float(np.mean(s))

rows = list(csv.DictReader(open('data/elo_before.csv')))
fx = collections.defaultdict(dict)
for r in rows:
    fx[r['fixture_id']]['home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'] = r
M = []
for fid, s in fx.items():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        M.append(dict(fid=fid, ko=h['kickoff_utc'], day=h['kickoff_utc'][:10], lg=(h['league_name'], h['country']),
                      top=h['tier'] == '1', ls=h['league_season_id'], ht=h['team_id'], at=a['team_id'],
                      he=float(h['rating_before']), ae=float(a['rating_before']),
                      hg=int(h['goals_for']), ag=int(h['goals_against'])))
M.sort(key=lambda m: m['ko'])

elo = collections.defaultdict(dict)                         # ls -> team -> elo
stats = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0, 0]))   # ls -> team -> [gf, ga, games]
form = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.deque(maxlen=8)))
rec = collections.defaultdict(lambda: [0, 0])               # (rule, league, venue) -> [fav wins, n]
labels = collections.defaultdict(list)                       # rule -> rows
cache = {}
DS = (1.5, 2.0, 2.5)

def tiers_for(ls, day):
    key = (ls, day)
    if key in cache: return cache[key]
    teams = list(elo[ls]); out = None
    if len(teams) >= 9:
        e = np.array([elo[ls][t] for t in teams]); sd = e.std() or 1.0
        # (2) k chosen by silhouette
        best = None
        for k in (3, 4, 5):          # silhouette picks k=2 almost always; top v bottom of 2 halves is too broad
            lab, C = kmeans(e, k); sc = silhouette(e, lab)
            if best is None or sc > best[0]: best = (sc, lab, C[:, 0])
        _, lab2, C2 = best; rank2 = np.argsort(np.argsort(C2)); t2 = {t: (rank2[l], len(C2)) for t, l in zip(teams, lab2)}
        # (5) k=3 Elo centres
        lab3, C3 = kmeans(e, 3); cen = {t: C3[l, 0] for t, l in zip(teams, lab3)}
        # (3) multi-feature k=3 (needs >= 3 games each)
        t3 = None
        ok = [t for t in teams if stats[ls][t][2] >= 3]
        if len(ok) >= 9:
            F = np.array([[elo[ls][t], stats[ls][t][0] / stats[ls][t][2], -stats[ls][t][1] / stats[ls][t][2],
                           np.mean(form[ls][t]) if form[ls][t] else 1.3] for t in ok])
            Fz = (F - F.mean(0)) / (F.std(0) + 1e-9)
            lab, C = kmeans(Fz, 3); comp = C.sum(1); rk = np.argsort(np.argsort(comp))
            t3 = {t: rk[l] for t, l in zip(ok, lab)}
        out = dict(t2=t2, cen=cen, sd=sd, t3=t3)
    cache[key] = out
    return out

for m in M:
    L, ls = m['lg'], m['ls']
    fav_home = m['he'] >= m['ae']; gap = abs(m['he'] - m['ae'])
    fav, opp = (m['ht'], m['at']) if fav_home else (m['at'], m['ht'])
    won = (m['hg'] > m['ag']) if fav_home else (m['ag'] > m['hg'])
    venue = 'home' if fav_home else 'away'
    hits = {}
    hits['gap>250'] = gap > 250
    T = tiers_for(ls, m['day'])
    if T:
        if fav in T['t2'] and opp in T['t2']:
            (rf, k), (ro, _) = T['t2'][fav], T['t2'][opp]
            hits['2 data-chosen tiers'] = rf == k - 1 and ro == 0 and k >= 3
        if T['t3'] and fav in T['t3'] and opp in T['t3']:
            hits['3 multi-feature tiers'] = T['t3'][fav] == 2 and T['t3'][opp] == 0
        if fav in T['cen'] and opp in T['cen']:
            dist = (T['cen'][fav] - T['cen'][opp]) / T['sd']
            for d in DS: hits[f'5 cluster distance>={d}'] = dist >= d
    for rule, hit in hits.items():
        if not hit: continue
        key = (rule, L) if rule.startswith(('gap', '5')) else (rule, L, venue)
        k_, n_ = rec[key]
        if n_ >= 30 and wl(k_, n_) >= 0.60:
            labels[rule].append(dict(fid=m['fid'], won=won, claim=wl(k_, n_), top=m['top'], lg=L))
        rec[key][0] += won; rec[key][1] += 1
    # update state after the match
    for t, e, gf, ga in ((m['ht'], m['he'], m['hg'], m['ag']), (m['at'], m['ae'], m['ag'], m['hg'])):
        elo[ls][t] = e; s = stats[ls][t]; s[0] += gf; s[1] += ga; s[2] += 1
        form[ls][t].append(3 if gf > ga else 1 if gf == ga else 0)

print(f"matches: {len(M)}\n\ntop flight, trust-gated, earlier data only")
print(f"{'rule':26s} {'labelled':>8s} {'leagues':>7s} {'fav won':>8s} {'claimed':>8s}")
base = {r['fid'] for r in labels['gap>250'] if r['top']}
for rule in ['gap>250', '2 data-chosen tiers', '3 multi-feature tiers'] + [f'5 cluster distance>={d}' for d in DS]:
    rs = [r for r in labels[rule] if r['top']]
    if not rs: print(f"{rule:26s} {0:8d}"); continue
    print(f"{rule:26s} {len(rs):8d} {len({r['lg'] for r in rs}):7d} {np.mean([r['won'] for r in rs]):8.1%} {np.mean([r['claim'] for r in rs]):8.1%}")
print("\nversus gap>250 (top flight): matches only this rule labels, and how they did")
for rule in ['2 data-chosen tiers', '3 multi-feature tiers'] + [f'5 cluster distance>={d}' for d in DS]:
    rs = {r['fid']: r for r in labels[rule] if r['top']}
    only = [r for f, r in rs.items() if f not in base]; both = [r for f, r in rs.items() if f in base]
    print(f"  {rule:26s} only-this n={len(only):5d} won {np.mean([r['won'] for r in only]) if only else float('nan'):.1%} | both n={len(both):5d} won {np.mean([r['won'] for r in both]) if both else float('nan'):.1%}")
