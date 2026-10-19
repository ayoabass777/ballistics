"""Big dog: Elo + 65 home edge first, Dixon-Coles as the last check (2026-10-18).
Window = DC dump (Jun-Oct 2026). Leagues = trusted under adjusted gap > 250 at
kickoff (walk-forward, all history), same set for every rule, top flight.
DC win chance must be for the same favourite (dump keeps win chances >= 55%).
python -m engine.eval.diagnostics.strength_home_dc
"""
import csv, collections, json, math
Z = 1.645; HE = 65
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
    adj = he + HE - ae
    if abs(adj) > 250:
        rec[L][0] += (hg > ag) if adj > 0 else (ag > hg); rec[L][1] += 1
info = {m[1]: m for m in M}
preds = json.load(open('data/raw_preds.json'))
names, dcw = {}, {}
for p in preds:
    names[str(p['fixture_id'])] = (p['home'], p['away'])
    if p['model'] == 'dixon_coles' and p['market'].endswith(' Win'):
        dcw[str(p['fixture_id'])] = (p['market'][:-4] == p['home'], p['predicted_prob_raw'])
period = [f for f in names if f in info and trusted[f]]
RULES = {
    'flat gap > 250 (today)':     (0, 250, None),
    'home edge, gap > 250':       (HE, 250, None),
    'home edge, gap > 250 + DC65': (HE, 250, .65),
    'home edge, gap > 200':       (HE, 200, None),
    'home edge, gap > 200 + DC60': (HE, 200, .60),
    'home edge, gap > 200 + DC65': (HE, 200, .65),
    'home edge, gap > 200 + DC70': (HE, 200, .70),
}
print(f"window Jun-Oct 2026, {len(period)} matches in trusted top-flight leagues\n")
print(f"{'rule':30s} {'Big dogs':>8s} {'won':>6s}  90% range   home / away")
sets = {}
for name, (H, T, dmin) in RULES.items():
    n = w = 0; hm = [0, 0]; aw = [0, 0]; ids = set()
    for fid in period:
        _, _, L, top, he, ae, hg, ag = info[fid]
        adj = he + H - ae
        if abs(adj) <= T: continue
        home = adj > 0
        if dmin is not None:
            if fid not in dcw or dcw[fid][0] != home or dcw[fid][1] < dmin: continue
        won = (hg > ag) if home else (ag > hg)
        n += 1; w += won; ids.add(fid)
        (hm if home else aw)[0] += won; (hm if home else aw)[1] += 1
    sets[name] = ids
    f = lambda x: f"{x[1]} {x[0]/x[1]:.0%}" if x[1] else "0"
    print(f"{name:30s} {n:8d} {w/n:6.1%}  {wl(w,n):.0%}-{wh(w,n):.0%}   {f(hm)} / {f(aw)}")
base = sets['home edge, gap > 250']
for name in ('home edge, gap > 200 + DC65', 'home edge, gap > 200 + DC70'):
    add = sets[name] - base; drop = base - sets[name]
    def rate(ids):
        if not ids: return "0"
        k = 0
        for fid in ids:
            _, _, L, top, he, ae, hg, ag = info[fid]; home = he + HE - ae > 0
            k += (hg > ag) if home else (ag > hg)
        return f"{len(ids)} won {k/len(ids):.0%}"
    print(f"\n{name} vs home edge > 250: added {rate(add)} | dropped {rate(drop)}")
    for lbl, ids in (('added', add), ('dropped', drop)):
        c = collections.Counter()
        for fid in ids:
            _, _, L, top, he, ae, hg, ag = info[fid]; h, a = names[fid]
            c[h if he + HE - ae > 0 else a] += 1
        print(f"  {lbl}: " + ", ".join(f"{t} {k}" for t, k in c.most_common(12)))
