"""Big dog Elo-gap threshold sweep 200..300 (2026-10-18). Walk-forward over
data/elo_before.csv; each threshold has its own league trust gate (league's prior
record above that threshold >= 30 matches, Wilson 90% lower bound >= 60%).
Top flight. Run from repo root: python -m engine.eval.diagnostics.strength_gap_sweep
"""
import csv, collections, math
import numpy as np
Z = 1.645
def wl(k, n):
    if n == 0: return 0.0
    p = k / n; d = 1 + Z * Z / n
    return (p + Z * Z / (2 * n) - Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))) / d
fx = collections.defaultdict(dict)
for r in csv.DictReader(open('data/elo_before.csv')):
    fx[r['fixture_id']]['home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'] = r
M = []
for s in fx.values():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        M.append((h['kickoff_utc'], (h['league_name'], h['country']), h['tier'] == '1',
                  float(h['rating_before']), float(a['rating_before']), int(h['goals_for']), int(h['goals_against'])))
M.sort()
TH = list(range(200, 301, 10))
rec = {t: collections.defaultdict(lambda: [0, 0]) for t in TH}
lab = {t: [] for t in TH}
for ko, L, top, he, ae, hg, ag in M:
    gap = abs(he - ae); won = (hg > ag) if he >= ae else (ag > hg)
    for t in TH:
        if gap <= t: continue
        k, n = rec[t][L]
        if top and n >= 30 and wl(k, n) >= 0.60: lab[t].append((gap, won, wl(k, n), L))
        rec[t][L][0] += won; rec[t][L][1] += 1
print(f"top flight, trust-gated, {len(M)} matches\n")
print(f"{'gap >':>6s} {'labelled':>8s} {'leagues':>7s} {'fav won':>8s} {'claimed':>8s} {'won-claim':>9s} | band below 250 only: n  won")
for t in TH:
    r = lab[t]; w = np.mean([x[1] for x in r]); c = np.mean([x[2] for x in r])
    low = [x for x in r if x[0] <= 250]
    lw = f"{len(low):5d}  {np.mean([x[1] for x in low]):.1%}" if low else "    0"
    print(f"{t:6d} {len(r):8d} {len({x[3] for x in r}):7d} {w:8.1%} {c:8.1%} {100*(w-c):+8.1f}  | {lw}")
print("\nwin rate by gap band (all trust-gated labels at threshold 200):")
r = lab[200]
for lo, hi in ((200, 225), (225, 250), (250, 275), (275, 300), (300, 400), (400, 9999)):
    s = [x for x in r if lo < x[0] <= hi]
    if s: print(f"  {lo}-{hi if hi < 9999 else '+'}: n={len(s):5d} won {np.mean([x[1] for x in s]):.1%}")
