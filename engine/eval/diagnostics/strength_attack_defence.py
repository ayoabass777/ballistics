"""Big dog: Elo gap vs adding attack/defence (2026-10-18). Walk-forward on
data/elo_before.csv. Attack/defence = team's goals for/against per game relative
to its league, last 20 games, shrunk toward league (k=6). Win chance from two
Poissons (home lambda = league home avg * att_h * def_a). Every rule gets its own
league trust gate (>=30 prior labelled matches in the league, Wilson90 low >=60%).
Top flight only. python -m engine.eval.diagnostics.strength_attack_defence
"""
import csv, collections, math
import numpy as np
Z = 1.645; K = 6; WIN = 20
def wl(k, n):
    if n == 0: return 0.0
    p = k / n; d = 1 + Z * Z / n
    return (p + Z * Z / (2 * n) - Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))) / d
POIS = [None]
def pwin(l1, l2):
    a = [math.exp(-l1) * l1 ** i / math.factorial(i) for i in range(11)]
    b = [math.exp(-l2) * l2 ** i / math.factorial(i) for i in range(11)]
    return sum(a[i] * b[j] for i in range(11) for j in range(i))
fx = collections.defaultdict(dict)
for r in csv.DictReader(open('data/elo_before.csv')):
    fx[r['fixture_id']]['home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'] = r
M = []
for s in fx.values():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        M.append((h['kickoff_utc'], (h['league_name'], h['country']), h['tier'] == '1', h['team_id'], a['team_id'],
                  float(h['rating_before']), float(a['rating_before']), int(h['goals_for']), int(h['goals_against'])))
M.sort()
hist = collections.defaultdict(lambda: collections.deque(maxlen=WIN))   # team -> (gf, ga)
lg = collections.defaultdict(lambda: collections.deque(maxlen=400))      # league -> (hg, ag)
RULES = {
    'Elo > 250 (today)':            lambda g, p, agree: g > 250,
    'Elo > 250 + A/D win >= 55%':   lambda g, p, agree: g > 250 and agree and p >= .55,
    'Elo > 250 + A/D win >= 60%':   lambda g, p, agree: g > 250 and agree and p >= .60,
    'Elo > 200 + A/D win >= 60%':   lambda g, p, agree: g > 200 and agree and p >= .60,
    'Elo > 200 + A/D win >= 65%':   lambda g, p, agree: g > 200 and agree and p >= .65,
    'A/D alone win >= 60%':         lambda g, p, agree: p >= .60,
    'A/D alone win >= 65%':         lambda g, p, agree: p >= .65,
    'A/D alone win >= 70%':         lambda g, p, agree: p >= .70,
}
rec = {k: collections.defaultdict(lambda: [0, 0]) for k in RULES}
lab = {k: [] for k in RULES}
for ko, L, top, ht, at, he, ae, hg, ag in M:
    lgr = lg[L]
    if len(lgr) >= 50 and len(hist[ht]) >= 5 and len(hist[at]) >= 5:
        mh = np.mean([x[0] for x in lgr]); ma = np.mean([x[1] for x in lgr]); m = (mh + ma) / 2
        def ad(t):
            h = hist[t]; n = len(h)
            att = (sum(x[0] for x in h) + K * m) / (n + K) / m
            de = (sum(x[1] for x in h) + K * m) / (n + K) / m
            return att, de
        (ah, dh), (aa, da) = ad(ht), ad(at)
        lh, la = mh * ah * da, ma * aa * dh
        ph, pa = pwin(lh, la), pwin(la, lh)
        elo_fav_home = he >= ae; gap = abs(he - ae)
        ad_fav_home = ph >= pa; p = max(ph, pa)
        agree = elo_fav_home == ad_fav_home
        for k, f in RULES.items():
            use_ad = k.startswith('A/D alone')
            if not f(gap, p, agree): continue
            fav_home = ad_fav_home if use_ad else elo_fav_home
            won = (hg > ag) if fav_home else (ag > hg)
            n0 = rec[k][L]
            if top and n0[1] >= 30 and wl(*n0) >= .60:
                lab[k].append((won, L, gap, p))
            n0[0] += won; n0[1] += 1
    hist[ht].append((hg, ag)); hist[at].append((ag, hg)); lgr.append((hg, ag))
base = {(x[1], x[2]) for x in lab['Elo > 250 (today)']}
print(f"top flight, trust-gated, walk-forward, {len(M)} matches\n")
print(f"{'rule':30s} {'Big dogs':>8s} {'leagues':>7s} {'fav won':>8s}")
for k, r in lab.items():
    print(f"{k:30s} {len(r):8d} {len({x[1] for x in r}):7d} {np.mean([x[0] for x in r]):8.1%}")
r = lab['Elo > 250 (today)']
print("\ntoday's Big dogs split by whether attack/defence agrees:")
for lo, hi in ((0, .5), (.5, .55), (.55, .6), (.6, .65), (.65, .7), (.7, 1.01)):
    s = [x for x in r if lo <= x[3] < hi]
    if s: print(f"  A/D fav win {lo:.0%}-{min(hi,1):.0%}: n={len(s):5d} won {np.mean([x[0] for x in s]):.1%}")
