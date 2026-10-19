"""Team strength: Elo-gap Big dog rule vs Elo tier clusters (2026-10-18).

Walk-forward over data/elo_before.csv (scripts/export_elo_before.sh). For each
match, using only earlier information:
  gap rule   : favourite Elo - opponent Elo > 250, league's prior Big dog record
               >= 30 matches with Wilson 90% lower bound >= 60% (D74/D81)
  tier rule  : per league, k-means (k=3) on current team Elo; favourite in top
               tier vs opponent in bottom tier, that league's prior top-v-bottom
               record at the same venue >= 30 matches, lower bound >= 60%
Claim = the gating lower bound (what the strength score uses). Reports matches
labelled, favourite win rate, claim, and overlap. Top flight (tier 1) and all.
Run from repo root: python -m engine.eval.diagnostics.strength_clusters
"""
import csv, collections, math, sys
import numpy as np

Z = 1.645
def wilson_low(k, n):
    if n == 0: return 0.0
    p = k / n; d = 1 + Z * Z / n
    return (p + Z * Z / (2 * n) - Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))) / d

def kmeans3(x):
    x = np.sort(np.asarray(x, float))
    c = np.percentile(x, [17, 50, 83])
    for _ in range(50):
        lab = np.argmin(np.abs(x[:, None] - c[None, :]), axis=1)
        new = np.array([x[lab == i].mean() if (lab == i).any() else c[i] for i in range(3)])
        if np.allclose(new, c): break
        c = new
    c = np.sort(c)
    return (c[0] + c[1]) / 2, (c[1] + c[2]) / 2       # cut points low|mid, mid|top

rows = list(csv.DictReader(open('data/elo_before.csv')))
fx = collections.defaultdict(dict)
for r in rows:
    side = 'home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'
    fx[r['fixture_id']][side] = r
matches = []
for fid, s in fx.items():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        matches.append(dict(fid=fid, ko=h['kickoff_utc'], lg=(h['league_name'], h['country']), tier=h['tier'],
                            ls=h['league_season_id'], ht=h['team_id'], at=a['team_id'],
                            he=float(h['rating_before']), ae=float(a['rating_before']),
                            hg=int(h['goals_for']), ag=int(h['goals_against'])))
matches.sort(key=lambda m: m['ko'])
print(f"matches with both sides and a result: {len(matches)}  ({matches[0]['ko'][:10]} .. {matches[-1]['ko'][:10]})")

latest = collections.defaultdict(dict)                # league_season -> team -> elo
gap_rec = collections.defaultdict(lambda: [0, 0])      # league -> [fav wins, n] for gap > 250
tier_rec = collections.defaultdict(lambda: [0, 0])     # (league, venue of fav) -> top v bottom
out = {'gap': [], 'tier': [], 'tier_ungated': []}
for m in matches:
    L = m['lg']; ls = m['ls']
    fav_home = m['he'] >= m['ae']; gap = abs(m['he'] - m['ae'])
    fav_won = (m['hg'] > m['ag']) if fav_home else (m['ag'] > m['hg'])
    venue = 'home' if fav_home else 'away'
    # gap rule (prior record)
    gk, gn = gap_rec[L]
    if gap > 250 and gn >= 30 and wilson_low(gk, gn) >= 0.60:
        out['gap'].append(dict(m, won=fav_won, claim=wilson_low(gk, gn)))
    # tier rule
    elos = latest[ls]
    tiers = None
    if len(elos) >= 9:
        lo, hi = kmeans3(list(elos.values()))
        tf = lambda e: 2 if e > hi else 0 if e < lo else 1
        fe, oe = (m['he'], m['ae']) if fav_home else (m['ae'], m['he'])
        tiers = (tf(fe), tf(oe))
    if tiers == (2, 0):
        tk, tn = tier_rec[(L, venue)]
        out['tier_ungated'].append(dict(m, won=fav_won, claim=tk / tn if tn else None))
        if tn >= 30 and wilson_low(tk, tn) >= 0.60:
            out['tier'].append(dict(m, won=fav_won, claim=wilson_low(tk, tn)))
    # update after the match
    if gap > 250: gap_rec[L][0] += fav_won; gap_rec[L][1] += 1
    if tiers == (2, 0): tier_rec[(L, venue)][0] += fav_won; tier_rec[(L, venue)][1] += 1
    latest[ls][m['ht']] = m['he']; latest[ls][m['at']] = m['ae']

def report(label, sel):
    for scope, f in (('top flight', lambda r: r['tier'] == '1'), ('all leagues', lambda r: True)):
        rs = [r for r in sel if f(r)]
        if not rs: print(f"  {label:22s} {scope:11s} n=0"); continue
        won = np.mean([r['won'] for r in rs]); cl = [r['claim'] for r in rs if r['claim'] is not None]
        lgs = len({r['lg'] for r in rs})
        print(f"  {label:22s} {scope:11s} n={len(rs):5d} leagues={lgs:3d}  fav won {won:.1%}  claimed {np.mean(cl):.1%}" if cl else
              f"  {label:22s} {scope:11s} n={len(rs):5d} leagues={lgs:3d}  fav won {won:.1%}")
print("\nlabelled matches (favourite = higher Elo), only earlier data used for every label")
report('gap > 250 (today)', out['gap'])
report('tier top v bottom', out['tier'])
report('tier, no trust gate', out['tier_ungated'])
g = {r['fid'] for r in out['gap']}; t = {r['fid'] for r in out['tier']}
print(f"\noverlap: both {len(g & t)}, gap only {len(g - t)}, tier only {len(t - g)}")
for lab, ids in (('gap only', g - t), ('tier only', t - g), ('both', g & t)):
    rs = [r for r in out['gap'] + out['tier'] if r['fid'] in ids]
    seen = {}; [seen.setdefault(r['fid'], r) for r in rs]
    if seen: print(f"  {lab:9s} fav won {np.mean([r['won'] for r in seen.values()]):.1%}  (n={len(seen)})")
