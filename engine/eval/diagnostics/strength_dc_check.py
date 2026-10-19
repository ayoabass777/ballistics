"""Big dog: Elo gap + production Dixon-Coles win chance (2026-10-18).
DC win chances from data/raw_preds.json (calibration_wf walk-forward dump,
Jun-Oct 2026; win markets only dumped when >= 55%). Elo from data/elo_before.csv.
League set = leagues trusted under today's rule at kickoff (walk-forward, all
history), same set for every rule, top flight.
python -m engine.eval.diagnostics.strength_dc_check
"""
import csv, collections, json, math
Z = 1.645
def wl(k, n):
    if n == 0: return 0.0
    p = k / n; d = 1 + Z * Z / n
    return (p + Z * Z / (2 * n) - Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))) / d
def wh(k, n):
    p = k / n; d = 1 + Z * Z / n
    return (p + Z * Z / (2 * n) + Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))) / d
fx = collections.defaultdict(dict)
for r in csv.DictReader(open('data/elo_before.csv')):
    fx[r['fixture_id']]['home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'] = r
M = []
for fid, s in fx.items():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        M.append((h['kickoff_utc'], fid, (h['league_name'], h['country']), h['tier'] == '1',
                  float(h['rating_before']), float(a['rating_before']), int(h['goals_for']), int(h['goals_against'])))
M.sort()
rec = collections.defaultdict(lambda: [0, 0]); trusted = {}
for ko, fid, L, top, he, ae, hg, ag in M:
    trusted[fid] = top and rec[L][1] >= 30 and wl(*rec[L]) >= .60
    if abs(he - ae) > 250:
        rec[L][0] += (hg > ag) if he >= ae else (ag > hg); rec[L][1] += 1
info = {m[1]: m for m in M}
preds = json.load(open('data/raw_preds.json'))
home = {}; dcw = {}
for p in preds:
    home[str(p['fixture_id'])] = p['home']
    if p['model'] == 'dixon_coles' and p['market'].endswith(' Win'):
        dcw[str(p['fixture_id'])] = (p['market'][:-4] == p['home'], p['predicted_prob_raw'])
period = [f for f in home if f in info]
print(f"fixtures in DC dump: {len(home)}, matched to Elo: {len(period)}, in trusted top-flight leagues: {sum(trusted[f] for f in period)}\n")
RULES = {
    'Elo > 250 (today)':          lambda g, d: g > 250,
    'Elo > 250 + DC >= 60%':      lambda g, d: g > 250 and d >= .60,
    'Elo > 250 + DC >= 65%':      lambda g, d: g > 250 and d >= .65,
    'Elo > 200 + DC >= 60%':      lambda g, d: g > 200 and d >= .60,
    'Elo > 200 + DC >= 65%':      lambda g, d: g > 200 and d >= .65,
    'DC alone >= 65%':            lambda g, d: d >= .65,
    'DC alone >= 70%':            lambda g, d: d >= .70,
}
print(f"{'rule':24s} {'Big dogs':>8s} {'fav won':>8s}   90% range")
for k, f in RULES.items():
    n = w = 0
    for fid in period:
        if not trusted[fid]: continue
        _, _, L, top, he, ae, hg, ag = info[fid]
        elo_home = he >= ae; d = 0.0
        if fid in dcw:
            dh, prob = dcw[fid]
            if k.startswith('DC alone') or dh == elo_home: d = prob
            fav_home = dh if k.startswith('DC alone') else elo_home
        else:
            fav_home = elo_home
        if not f(abs(he - ae), d): continue
        n += 1; w += (hg > ag) if fav_home else (ag > hg)
    print(f"{k:24s} {n:8d} {w/n:8.1%}   {wl(w,n):.0%}-{wh(w,n):.0%}")
print("\ntoday's Big dogs by DC chance for the same favourite:")
for lo, hi in ((0, .55), (.55, .60), (.60, .65), (.65, .70), (.70, 1.01)):
    n = w = 0
    for fid in period:
        _, _, L, top, he, ae, hg, ag = info[fid]
        if not trusted[fid] or abs(he - ae) <= 250: continue
        elo_home = he >= ae
        d = dcw[fid][1] if fid in dcw and dcw[fid][0] == elo_home else 0.0
        if lo <= d < hi:
            n += 1; w += (hg > ag) if elo_home else (ag > hg)
    if n: print(f"  DC {('<55%' if lo == 0 else f'{lo:.0%}-{min(hi,1):.0%}'):8s} n={n:4d} won {w/n:.1%}")
