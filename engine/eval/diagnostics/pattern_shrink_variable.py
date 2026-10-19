"""Variable pattern shrink (2026-10-18): fixed prior vs per-league-per-event
empirical-Bayes prior, with a cap sweep. Walk-forward, prior-only tallies.

prior M for (league, event) = p(1-p)/tau^2 - 1, tau^2 = observed variance of team
rates minus luck variance (mean p(1-p)/n_i), clamped to [FLOOR, cap]; fallback 15
when fewer than MIN_TEAMS teams have MIN_GAMES games.
Run from repo root: python -m engine.eval.diagnostics.pattern_shrink_variable
"""
import json, collections, sys, time
import numpy as np
sys.path.insert(0, '.')
for _ in range(6):
    try: P = json.load(open('data/raw_preds.json')); break
    except OSError: time.sleep(2)

fx = {}
for p in P:
    if p.get('score') and p['fixture_id'] not in fx:
        h, a = map(int, p['score'].split('-'))
        fx[p['fixture_id']] = dict(fid=p['fixture_id'], ko=p['kickoff'], lg=(p['api_league_id'], p['league']),
                                   home=p['home'], away=p['away'], hg=h, ag=a)
order = sorted(fx.values(), key=lambda f: f['ko'])
import csv, os
ELO = {}   # (fixture_id, 'home'|'away') -> rating_before
if os.path.exists('data/elo_before.csv'):
    for r in csv.DictReader(open('data/elo_before.csv')):
        ELO[(int(r['fixture_id']), 'home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away')] = float(r['rating_before'])
latest = collections.defaultdict(dict)                       # league -> team -> elo
tier_tally = collections.defaultdict(lambda: [0, 0])         # (league, tier, event) -> [k, n]
def tier_of(L, team):
    elos = latest[L]
    if team not in elos or len(elos) < 9: return None
    v = elos[team]; s_ = sorted(elos.values()); rank = sum(1 for x in s_ if x < v) / len(s_)
    return 0 if rank < 1/3 else 1 if rank < 2/3 else 2
print(f"elo rows loaded: {len(ELO)}")
MATCH_EV = {'Over_1_5': lambda t: t > 1, 'Over_2_5': lambda t: t > 2, 'Over_3_5': lambda t: t > 3,
            'Under_2_5': lambda t: t < 3, 'Under_3_5': lambda t: t < 4, 'Under_4_5': lambda t: t < 5}
TEAM_EV = {'score_1goal': lambda f, a: f >= 1, 'score_2goals': lambda f, a: f >= 2, 'concede_1': lambda f, a: a >= 1,
           'win': lambda f, a: f > a, 'loss': lambda f, a: f < a, 'unbeaten': lambda f, a: f >= a,
           'winless': lambda f, a: f <= a, 'clean_sheet': lambda f, a: a == 0}
FLOOR, MIN_TEAMS, MIN_GAMES, FALLBACK = 5, 10, 5, 15
CAPS = (15, 20, 30, 40, 60)
FLOORS = (12, 15)

# league -> event -> team -> [k, n]   (team's own games, 'all' venues)
tally = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0])))

def prior_M(L, ev, cap, floor=FLOOR):
    teams = [v for v in tally[L][ev].values() if v[1] >= MIN_GAMES]
    if len(teams) < MIN_TEAMS: return None
    k = sum(v[0] for v in teams); n = sum(v[1] for v in teams); p = k / n
    if p <= 0 or p >= 1: return None
    rates = np.array([v[0] / v[1] for v in teams]); ns = np.array([v[1] for v in teams])
    tau2 = rates.var(ddof=1) - np.mean(p * (1 - p) / ns)
    if tau2 <= 0: return cap
    return float(min(max(p * (1 - p) / tau2 - 1, floor), cap))

rows = []   # one per qualifying record going into a match
for f in order:
    L, t = f['lg'], f['hg'] + f['ag']
    sides = (('home', f['hg'], f['ag']), ('away', f['ag'], f['hg']))
    for side, gf, ga in sides:
        team = f[side]
        for evs, outcome in ((TEAM_EV, lambda fn: fn(gf, ga)), (MATCH_EV, lambda fn: fn(t))):
            for ev, fn in evs.items():
                k, n = tally[L][ev][team]
                lk = sum(v[0] for v in tally[L][ev].values()); ln = sum(v[1] for v in tally[L][ev].values())
                if n < 6 or ln < 40 or k / n < 0.75: continue
                base = lk / ln
                Ms = {f'fixed{m}': m for m in (15, 20)}
                for fl in FLOORS:
                    for cap in CAPS:
                        if cap < fl: continue
                        v = prior_M(L, ev, cap, fl); Ms[f'var_{fl}-{cap}'] = FALLBACK if v is None else v
                tr = tier_of(L, team); tt = tier_tally[(L, tr, ev)] if tr is not None else [0, 0]
                tbase = tt[0] / tt[1] if tt[1] >= 40 else None
                v15 = prior_M(L, ev, 30, 15); vM = FALLBACK if v15 is None else v15
                rows.append(dict(fid=f['fid'], k=k, n=n, base=base, y=float(outcome(fn)), Ms=Ms,
                                 tbase=tbase, vM=vM))
    tiers = {side: tier_of(L, f[side]) for side, _, _ in sides}
    for side, gf, ga in sides:
        team = f[side]; tr = tiers[side]
        for ev, fn in TEAM_EV.items():
            c = tally[L][ev][team]; c[0] += fn(gf, ga); c[1] += 1
            if tr is not None: c2 = tier_tally[(L, tr, ev)]; c2[0] += fn(gf, ga); c2[1] += 1
        for ev, fn in MATCH_EV.items():
            if tr is not None: c2 = tier_tally[(L, tr, ev)]; c2[0] += fn(t); c2[1] += 1
        e = ELO.get((f['fid'], side))
        if e is not None: latest[L][team] = e
        for ev, fn in MATCH_EV.items():
            c = tally[L][ev][team]; c[0] += fn(t); c[1] += 1

print(f"records evaluated: {len(rows)} (team record >= 75% on >= 6 games, league >= 40 team-games)\n")
print(f"{'variant':12s} {'median M':>8s} | {'all: weight':>11s} | standout-size (claim >= 15): n  claimed  realised  ratio")
for key in ['fixed15', 'fixed20'] + [f'var_{fl}-{c}' for fl in FLOORS for c in CAPS if c >= fl]:
    cl, re, Ms, big = [], [], [], []
    for r in rows:
        M = r['Ms'][key]; sh = (r['k'] + M * r['base']) / (r['n'] + M); c = sh - r['base']
        if c <= 0: continue
        cl.append(c); re.append(r['y'] - r['base']); Ms.append(M)
        if c >= 0.15: big.append((c, r['y'] - r['base'], r['fid']))
    cl, re = np.array(cl), np.array(re)
    w = (cl * re).sum() / (cl * cl).sum()
    bc = np.array([b[0] for b in big]); br = np.array([b[1] for b in big])
    # bootstrap ratio for standout-size
    fids = np.array([b[2] for b in big]); uf = np.unique(fids); ix = {u: np.where(fids == u)[0] for u in uf}
    rng = np.random.default_rng(7); bs = []
    for _ in range(300):
        p = np.concatenate([ix[u] for u in rng.choice(uf, len(uf))]); bs.append(br[p].mean() / bc[p].mean())
    lo, hi = np.percentile(bs, [5, 95])
    print(f"{key:13s} {np.median(Ms):8.1f} | {w:11.2f} | {len(big):5d}  {bc.mean()*100:+6.1f}  {br.mean()*100:+7.1f}   {br.mean()/bc.mean():.2f} [{lo:.2f}-{hi:.2f}]")

if ELO:
    print("\nVersion C: shrink toward the team's Elo tier (thirds of the league); claim still vs league")
    for lab, getM in (("C fixed15", lambda r: 15), ("C fixed20", lambda r: 20), ("C var 15-30", lambda r: r['vM'])):
        cl, re, big = [], [], []
        for r in rows:
            tgt = r['tbase'] if r['tbase'] is not None else r['base']; M = getM(r)
            c = (r['k'] + M * tgt) / (r['n'] + M) - r['base']
            if c <= 0: continue
            cl.append(c); re.append(r['y'] - r['base'])
            if c >= 0.15: big.append((c, r['y'] - r['base']))
        cl, re = np.array(cl), np.array(re); bc = np.array([b[0] for b in big]); br = np.array([b[1] for b in big])
        cover = sum(1 for r in rows if r['tbase'] is not None) / len(rows)
        print(f"{lab:13s} tier coverage {cover:.0%} | all weight {(cl*re).sum()/(cl*cl).sum():.2f} | standout-size n={len(big)} claimed {bc.mean()*100:+.1f} realised {br.mean()*100:+.1f} ratio {br.mean()/bc.mean():.2f}")
