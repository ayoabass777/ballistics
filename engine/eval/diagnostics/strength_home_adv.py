"""Big dog with home advantage in the Elo gap (2026-10-18).
adjusted gap = (home Elo + H) - away Elo; Big dog when |adjusted| > threshold.
Walk-forward, top flight, own league trust gate per rule (>=30, Wilson90 >= 60%).
python -m engine.eval.diagnostics.strength_home_adv
"""
import csv, collections, json, math, os
COL = os.environ.get('ELO_COL', 'rating_before')   # production Big dogs use rating_before_flat
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
                  float(h[COL]), float(a[COL]), int(h['goals_for']), int(h['goals_against'])))
M.sort()
RULES = [(0, 250), (50, 250), (65, 250), (70, 250), (80, 250), (65, 200), (65, 275), (65, 300)]
rec = {r: collections.defaultdict(lambda: [0, 0]) for r in RULES}
lab = {r: [] for r in RULES}
for ko, L, top, he, ae, hg, ag in M:
    for H, T in RULES:
        adj = he + H - ae
        if abs(adj) <= T: continue
        home = adj > 0
        won = (hg > ag) if home else (ag > hg)
        k, n = rec[(H, T)][L]
        if top and n >= 30 and wl(k, n) >= .60:
            lab[(H, T)].append((won, home, abs(adj)))
        rec[(H, T)][L][0] += won; rec[(H, T)][L][1] += 1
print("top flight, trust-gated, walk-forward\n")
print(f"{'home adv':>8s} {'gap >':>6s} {'Big dogs':>8s} {'won':>6s} | {'home favs':>14s} | {'away favs':>14s} | away 1st band")
for (H, T), r in lab.items():
    hm = [x for x in r if x[1]]; aw = [x for x in r if not x[1]]
    a1 = [x for x in aw if x[2] <= T + 50]
    f = lambda s: f"{len(s):5d} {np.mean([x[0] for x in s]):6.1%}" if s else "    0"
    print(f"{H:8d} {T:6d} {len(r):8d} {np.mean([x[0] for x in r]):6.1%} | {f(hm)} | {f(aw)} | {f(a1)}")
print("\nH=65, gap>200: win rate by adjusted gap band")
r = lab[(65, 200)]
for lo, hi in ((200, 225), (225, 250), (250, 275), (275, 300), (300, 400), (400, 9999)):
    s_ = [x for x in r if lo < x[2] <= hi]; hm = [x[0] for x in s_ if x[1]]; aw = [x[0] for x in s_ if not x[1]]
    if s_: print(f"  {lo}-{hi if hi < 9999 else '+'}: n={len(s_):5d} won {np.mean([x[0] for x in s_]):.1%}  home {len(hm)} {np.mean(hm) if hm else 0:.1%} / away {len(aw)} {np.mean(aw) if aw else 0:.1%}")
import sys; sys.exit()
# current slate under H=65 / 80
d = json.load(open('engine/eval/consensus_slips_2026-10-18.json'))
print("\ncurrent Big dogs: adjusted gap with H=65 / H=80 (out if <= 250)")
for e in d['elo_gap_shortlist']:
    home = e['favourite'] == e['home']; g = e['elo_gap']
    a65, a80 = (g + 65, g + 80) if home else (g - 65, g - 80)
    if a65 <= 250 or a80 <= 250:
        print(f"  {e['match']:40s} {'H' if home else 'A'} gap {g}  -> {a65} / {a80}")
