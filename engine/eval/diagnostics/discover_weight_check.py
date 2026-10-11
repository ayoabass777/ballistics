"""D71 weight check: do claimed gaps above the league come true? Walk-forward, prior-only.

Run from repo root: python -m engine.eval.diagnostics.discover_weight_check
"""
import json, collections, time, sys
import numpy as np
sys.path.insert(0, '.')
from engine.eval.calibration_maps import dc_calibration_key, apply_points, fit_pava_map
for i in range(6):
    try: P = json.load(open('data/raw_preds.json')); break
    except OSError: time.sleep(2)

# ── fixtures with results, in kickoff order ──
fx = {}
for p in P:
    if p.get('score') and p['fixture_id'] not in fx:
        h, a = map(int, p['score'].split('-'))
        fx[p['fixture_id']] = dict(fid=p['fixture_id'], ko=p['kickoff'], lg=(p['api_league_id'], p['league']),
                                   home=p['home'], away=p['away'], hg=h, ag=a)
order = sorted(fx.values(), key=lambda f: f['ko'])

MATCH_EV = {'Over_1_5': lambda t: t > 1, 'Over_2_5': lambda t: t > 2, 'Over_3_5': lambda t: t > 3,
            'Under_2_5': lambda t: t < 3, 'Under_3_5': lambda t: t < 4, 'Under_4_5': lambda t: t < 5,
            'goals_1_4': lambda t: 1 <= t <= 4}
# team events from that team's perspective: (gf, ga)
TEAM_EV = {'score_1goal': lambda f, a: f >= 1, 'score_2goals': lambda f, a: f >= 2, 'concede_1': lambda f, a: a >= 1,
           'win': lambda f, a: f > a, 'loss': lambda f, a: f < a, 'unbeaten': lambda f, a: f >= a,
           'winless': lambda f, a: f <= a, 'clean_sheet': lambda f, a: a == 0}
BTTS = lambda h, a: h >= 1 and a >= 1

# running tallies (prior-only snapshots taken before each fixture)
lg_match = collections.defaultdict(lambda: [0, 0])            # (lg, ev) -> [hits, n]
lg_team = collections.defaultdict(lambda: [0, 0])             # (lg, ev, venue) -> [hits, n]
tm = collections.defaultdict(lambda: [0, 0])                  # (team, ev) -> [hits, n]  all games
tm_match = collections.defaultdict(lambda: [0, 0])            # (team, match_ev) -> team's games
snap = {}
for f in order:
    L = f['lg']; t = f['hg'] + f['ag']
    snap[f['fid']] = dict(
        match={ev: tuple(lg_match[(L, ev)]) for ev in list(MATCH_EV) + ['BTTS']},
        team={(ev, v): tuple(lg_team[(L, ev, v)]) for ev in TEAM_EV for v in ('home', 'away', 'all')},
        rec={(side, ev): tuple(tm[(f[side], ev)]) for side in ('home', 'away') for ev in TEAM_EV},
        recm={(side, ev): tuple(tm_match[(f[side], ev)]) for side in ('home', 'away') for ev in list(MATCH_EV) + ['BTTS']},
    )
    for ev, fn in MATCH_EV.items():
        c = lg_match[(L, ev)]; c[0] += fn(t); c[1] += 1
        for side in ('home', 'away'):
            c = tm_match[(f[side], ev)]; c[0] += fn(t); c[1] += 1
    c = lg_match[(L, 'BTTS')]; c[0] += BTTS(f['hg'], f['ag']); c[1] += 1
    for side in ('home', 'away'):
        c = tm_match[(f[side], 'BTTS')]; c[0] += BTTS(f['hg'], f['ag']); c[1] += 1
    for side, gf, ga in (('home', f['hg'], f['ag']), ('away', f['ag'], f['hg'])):
        for ev, fn in TEAM_EV.items():
            hit = fn(gf, ga)
            for v in (side, 'all'):
                c = lg_team[(L, ev, v)]; c[0] += hit; c[1] += 1
            c = tm[(f[side], ev)]; c[0] += hit; c[1] += 1

rate = lambda c: c[0] / c[1] if c[1] else None
MIN_LG = 20   # fixtures

# ── GOALS: walk-forward calibrated DC vs prior league rate ──
DC_MATCH = {'Match Over 1.5': 'Over_1_5', 'Match Over 2.5': 'Over_2_5', 'Match Over 3.5': 'Over_3_5',
            'Match Under 2.5': 'Under_2_5', 'Match Under 3.5': 'Under_3_5', 'Match Goals 1-4': 'goals_1_4',
            'BTTS Yes': 'BTTS'}
dc = [p for p in P if p['model'] == 'dixon_coles' and p.get('actual') is not None and p['fixture_id'] in snap]
folds = sorted({p['fold'] for p in dc})
goals = []
for fo in folds[2:]:
    fit = collections.defaultdict(lambda: ([], []))
    for p in dc:
        if p['fold'] < fo:
            k = dc_calibration_key(p['market']); fit[k][0].append(p['predicted_prob_raw']); fit[k][1].append(float(p['actual']))
    maps = {k: fit_pava_map(*v) for k, v in fit.items()}
    for p in dc:
        if p['fold'] != fo or p['market'].endswith(' Win'): continue
        pts = maps.get(dc_calibration_key(p['market']))
        if not pts: continue
        prob = apply_points(pts, p['predicted_prob_raw'])
        if prob < 0.65: continue
        s = snap[p['fixture_id']]; f = fx[p['fixture_id']]
        if p['market'] in DC_MATCH:
            c = s['match'][DC_MATCH[p['market']]]
            if c[1] < MIN_LG: continue
        else:
            team, ev = (p['market'][:-9], 'score_1goal') if p['market'].endswith(' Over 0.5') else \
                       (p['market'][:-9], 'score_2goals') if p['market'].endswith(' Over 1.5') else (None, None)
            side = 'home' if team == f['home'] else 'away' if team == f['away'] else None
            if not side: continue
            c = s['team'][(ev, side)]
            if c[1] < MIN_LG: continue
        base = rate(c)
        goals.append(dict(claim=prob - base, real=float(p['actual']) - base, prob=prob, base=base, fid=p['fixture_id'], fold=fo))

# ── PATTERNS: team record going in, shrunk with a 10-match prior ──
M = 10; pats = []
for f in order:
    s = snap[f['fid']]; t = f['hg'] + f['ag']
    for side, gf, ga in (('home', f['hg'], f['ag']), ('away', f['ag'], f['hg'])):
        for ev, fn in TEAM_EV.items():
            k, n = s['rec'][(side, ev)]; lb = s['team'][(ev, 'all')]
            if n < 6 or lb[1] < 2 * MIN_LG or k / n < 0.75: continue
            base = rate(lb); sh = (k + M * base) / (n + M)
            if sh - base <= 0: continue
            pats.append(dict(claim=sh - base, raw=k / n - base, real=float(fn(gf, ga)) - base, fid=f['fid'], kind='team', n=n))
        for ev in list(MATCH_EV) + ['BTTS']:
            k, n = s['recm'][(side, ev)]; lb = s['match'][ev]
            if n < 6 or lb[1] < MIN_LG or k / n < 0.75: continue
            base = rate(lb); sh = (k + M * base) / (n + M)
            if sh - base <= 0: continue
            hit = BTTS(f['hg'], f['ag']) if ev == 'BTTS' else MATCH_EV[ev](t)
            pats.append(dict(claim=sh - base, raw=k / n - base, real=float(hit) - base, fid=f['fid'], kind='match', n=n))

def report(name, rows, key='claim'):
    rows = [r for r in rows if r[key] > 0]
    c = np.array([r[key] for r in rows]); y = np.array([r['real'] for r in rows])
    slope = float((c * y).sum() / (c * c).sum())
    # bootstrap by fixture
    fids = np.array([r['fid'] for r in rows]); uf = np.unique(fids); idx = {f: np.where(fids == f)[0] for f in uf}
    rng = np.random.default_rng(7); bs = []
    for _ in range(300):
        pick = np.concatenate([idx[f] for f in rng.choice(uf, len(uf))])
        cc, yy = c[pick], y[pick]; bs.append((cc * yy).sum() / (cc * cc).sum())
    lo, hi = np.percentile(bs, [5, 95])
    print(f"\n{name}: n={len(rows)} fixtures={len(uf)}  claimed avg {c.mean()*100:+.1f} pts  realised avg {y.mean()*100:+.1f} pts  "
          f"weight (realised/claimed) {slope:.2f}  [90% CI {lo:.2f}–{hi:.2f}]")
    for a, b in ((0, .05), (.05, .10), (.10, .15), (.15, .25), (.25, 1)):
        m = (c > a) & (c <= b)
        if m.sum() >= 20: print(f"   claimed {a*100:>3.0f}–{b*100:<3.0f} pts  n={m.sum():5d}  claimed {c[m].mean()*100:+5.1f}  realised {y[m].mean()*100:+5.1f}")
    return slope

print('fixtures', len(order), 'folds', folds)
report('GOALS (calibrated DC ≥65% minus league)', goals)
report('PATTERNS shrunk (k/n ≥75%, n ≥6)', pats)
report('PATTERNS raw gap (old D57 scoring)', pats, key='raw')

print('\n── pattern prior strength sweep ──')
for MM in (10, 20, 30, 40, 60):
    rows = []
    rows = []
    for f in order:
        s = snap[f['fid']]; t = f['hg'] + f['ag']
        for side, gf, ga in (('home', f['hg'], f['ag']), ('away', f['ag'], f['hg'])):
            for ev, fn in TEAM_EV.items():
                k, n = s['rec'][(side, ev)]; lb = s['team'][(ev, 'all')]
                if n < 6 or lb[1] < 2 * MIN_LG or k / n < 0.75: continue
                base = rate(lb); sh = (k + MM * base) / (n + MM)
                rows.append((sh - base, float(fn(gf, ga)) - base))
            for ev in list(MATCH_EV) + ['BTTS']:
                k, n = s['recm'][(side, ev)]; lb = s['match'][ev]
                if n < 6 or lb[1] < MIN_LG or k / n < 0.75: continue
                base = rate(lb); sh = (k + MM * base) / (n + MM)
                hit = BTTS(f['hg'], f['ag']) if ev == 'BTTS' else MATCH_EV[ev](t)
                rows.append((sh - base, float(hit) - base))
    c = np.array([r[0] for r in rows if r[0] > 0]); y = np.array([r[1] for r in rows if r[0] > 0])
    top = c > np.percentile(c, 90)
    print(f"  prior {MM:3d} matches: weight {(c*y).sum()/(c*c).sum():.2f}  top-10% claimed {c[top].mean()*100:+.1f} realised {y[top].mean()*100:+.1f}")
# goals top decile
g = np.array([[r['claim'], r['real']] for r in goals if r['claim'] > 0]); top = g[:, 0] > np.percentile(g[:, 0], 90)
print(f"\ngoals top-10%: claimed {g[top,0].mean()*100:+.1f} realised {g[top,1].mean()*100:+.1f}; ×0.76 → {0.76*g[top,0].mean()*100:+.1f}")
