"""League tiers as match context for users (2026-10-18, exploration).
Per league-season, k-means k=3 on teams' current Elo (as of each match day,
earlier matches only) -> Top / Middle / Bottom. Reports what happened in each
tier matchup (home tier x away tier) and a preview of current tiers.
python -m engine.eval.diagnostics.tier_context
"""
import csv, collections, os
import numpy as np
COL = os.environ.get('ELO_COL', 'rating_before')
NAMES = ['Bottom', 'Middle', 'Top']
def kmeans(x, k=3, iters=40):
    x = np.asarray(x, float); C = np.quantile(x, [1/6, 1/2, 5/6])
    for _ in range(iters):
        lab = np.abs(x[:, None] - C[None]).argmin(1)
        newC = np.array([x[lab == i].mean() if (lab == i).any() else C[i] for i in range(k)])
        if np.allclose(newC, C): break
        C = newC
    return lab, C
fx = collections.defaultdict(dict)
for r in csv.DictReader(open('data/elo_before.csv')):
    fx[r['fixture_id']]['home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'] = r
M = []
for s in fx.values():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        M.append((h['kickoff_utc'], h['league_season_id'], (h['league_name'], h['country']), h['tier'] == '1',
                  h['team_id'], a['team_id'], float(h[COL]), float(a[COL]), int(h['goals_for']), int(h['goals_against'])))
M.sort()
elo = collections.defaultdict(dict); games = collections.defaultdict(lambda: collections.Counter())
cache = {}; tab = collections.defaultdict(lambda: [0, 0, 0, 0, 0])   # (ht, at) -> H, D, A, goals, n
sizes = collections.Counter()
for ko, ls, L, top, ht, at, he, ae, hg, ag in M:
    day = (ls, ko[:10])
    if day not in cache:
        teams = [t for t in elo[ls] if games[ls][t] >= 3]
        cache.clear()
        if len(teams) >= 9:
            lab, C = kmeans([elo[ls][t] for t in teams]); rk = np.argsort(np.argsort(C))
            cache[day] = {t: int(rk[l]) for t, l in zip(teams, lab)}
        else:
            cache[day] = None
    T = cache[day]
    if top and T and ht in T and at in T:
        c = tab[(T[ht], T[at])]
        c[0 if hg > ag else 1 if hg == ag else 2] += 1; c[3] += hg + ag; c[4] += 1
    for t, e in ((ht, he), (at, ae)):
        elo[ls][t] = e; games[ls][t] += 1
print(f"top flight, {sum(v[4] for v in tab.values())} matches, tiers from earlier matches only\n")
print(f"{'home v away':18s} {'n':>6s} {'home win':>9s} {'draw':>6s} {'away win':>9s} {'goals':>6s}")
for hti in (2, 1, 0):
    for ati in (2, 1, 0):
        H, D, A, g, n = tab[(hti, ati)]
        print(f"{NAMES[hti]+' v '+NAMES[ati]:18s} {n:6d} {H/n:9.0%} {D/n:6.0%} {A/n:9.0%} {g/n:6.2f}")
# tier sizes now + preview for a few leagues (latest season per league)
latest = {}
for ko, ls, L, *_ in M: latest[L] = ls
print("\ncurrent tiers (latest rating per team, this season):")
for L in [('Bundesliga', 'Germany'), ('Premier League', 'England'), ('Serie A', 'Italy'), ('Primeira Liga', 'Portugal'), ('La Liga', 'Spain')]:
    ls = latest.get(L)
    if not ls: continue
    teams = list(elo[ls]); lab, C = kmeans([elo[ls][t] for t in teams]); rk = np.argsort(np.argsort(C))
    names = {}
    for ko, l2, L2, top, ht, at, he, ae, hg, ag in M: pass
    groups = collections.defaultdict(list)
    for t, l in zip(teams, lab): groups[NAMES[int(rk[l])]].append(t)
    print(f"  {L[0]}: " + " | ".join(f"{g} {len(groups[g])}" for g in ('Top', 'Middle', 'Bottom')) + f"  centres {sorted(np.round(C).astype(int))[::-1]}")
