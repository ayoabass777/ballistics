"""Do league tiers add context inside the slight-favourite band? (2026-10-18)
Slight favourite = venue-adjusted gap (home +65) in (100, 250]. Tiers = k=3 Elo
per league-season from earlier matches. Top flight.
python -m engine.eval.diagnostics.tier_slight_fav
"""
import csv, collections, os
import numpy as np
COL = os.environ.get('ELO_COL', 'rating_before'); HE = 65
N = ['Bottom', 'Middle', 'Top']
def kmeans(x, iters=40):
    x = np.asarray(x, float); C = np.quantile(x, [1/6, 1/2, 5/6])
    for _ in range(iters):
        lab = np.abs(x[:, None] - C[None]).argmin(1)
        nC = np.array([x[lab == i].mean() if (lab == i).any() else C[i] for i in range(3)])
        if np.allclose(nC, C): break
        C = nC
    return lab, C
fx = collections.defaultdict(dict)
for r in csv.DictReader(open('data/elo_before.csv')):
    fx[r['fixture_id']]['home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'] = r
M = []
for s in fx.values():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        M.append((h['kickoff_utc'], h['league_season_id'], h['tier'] == '1', h['team_id'], a['team_id'],
                  float(h[COL]), float(a[COL]), int(h['goals_for']), int(h['goals_against'])))
M.sort()
elo = collections.defaultdict(dict); games = collections.defaultdict(collections.Counter); cache = {}
by = collections.defaultdict(lambda: [0, 0, 0])      # key -> won, drew, n
for ko, ls, top, ht, at, he, ae, hg, ag in M:
    day = (ls, ko[:10])
    if day not in cache:
        cache.clear(); teams = [t for t in elo[ls] if games[ls][t] >= 3]
        if len(teams) >= 9:
            lab, C = kmeans([elo[ls][t] for t in teams]); rk = np.argsort(np.argsort(C))
            cache[day] = {t: int(rk[l]) for t, l in zip(teams, lab)}
        else: cache[day] = None
    T = cache[day]; adj = he + HE - ae
    if top and T and ht in T and at in T and 100 < abs(adj) <= 250:
        home = adj > 0; fav, dog = (ht, at) if home else (at, ht)
        won = (hg > ag) if home else (ag > hg)
        band = '100-175' if abs(adj) <= 175 else '175-250'
        for key in ((band, N[T[fav]], N[T[dog]]), ('all', N[T[fav]], N[T[dog]]), (band, '*', '*'), ('all', 'venue', 'home' if home else 'away')):
            c = by[key]; c[0] += won; c[1] += hg == ag; c[2] += 1
    for t, e in ((ht, he), (at, ae)): elo[ls][t] = e; games[ls][t] += 1
print("slight favourites (adjusted gap 100-250), top flight\n")
for band in ('100-175', '175-250'):
    c = by[(band, '*', '*')]; print(f"gap {band}: n={c[2]} fav won {c[0]/c[2]:.0%} drew {c[1]/c[2]:.0%}")
    for f in ('Top', 'Middle', 'Bottom'):
        for d in ('Top', 'Middle', 'Bottom'):
            c = by[(band, f, d)]
            if c[2] >= 100: print(f"   fav {f:6s} v {d:6s}  n={c[2]:5d}  won {c[0]/c[2]:.0%}  drew {c[1]/c[2]:.0%}")
for v in ('home', 'away'):
    c = by[('all', 'venue', v)]; print(f"{v} slight favourites: n={c[2]} won {c[0]/c[2]:.0%}")
