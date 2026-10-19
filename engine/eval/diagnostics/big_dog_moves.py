"""Who joins / leaves Big dogs under the two cross-check rules (2026-10-18).
  rough: Elo > 200 and rough attack/defence backs the same favourite >= 60%
  dc:    Elo > 250 and Dixon-Coles backs the same favourite >= 65%
Backtest window = the DC dump (Jun-Oct 2026), top flight, leagues trusted under
today's rule. Writes engine/eval/diagnostics/out/big_dog_moves.csv and prints
the upcoming slate from the latest consensus export.
python -m engine.eval.diagnostics.big_dog_moves
"""
import csv, collections, glob, json, math
from pathlib import Path
import numpy as np
Z = 1.645; K = 6; WIN = 20
def wl(k, n):
    if n == 0: return 0.0
    p = k / n; d = 1 + Z * Z / n
    return (p + Z * Z / (2 * n) - Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))) / d
def pwin(l1, l2):
    a = [math.exp(-l1) * l1 ** i / math.factorial(i) for i in range(11)]
    b = [math.exp(-l2) * l2 ** i / math.factorial(i) for i in range(11)]
    return sum(a[i] * b[j] for i in range(11) for j in range(i))
fx = collections.defaultdict(dict)
for r in csv.DictReader(open('data/elo_before.csv')):
    fx[r['fixture_id']]['home' if r['is_home'] in ('t', 'true', 'True', '1') else 'away'] = r
M = []
for fid, s in fx.items():
    if 'home' in s and 'away' in s and s['home']['goals_for'] != '':
        h, a = s['home'], s['away']
        M.append((h['kickoff_utc'], fid, (h['league_name'], h['country']), h['tier'] == '1', h['team_id'], a['team_id'],
                  float(h['rating_before']), float(a['rating_before']), int(h['goals_for']), int(h['goals_against'])))
M.sort()
preds = json.load(open('data/raw_preds.json'))
names = {}; dcw = {}
for p in preds:
    names[str(p['fixture_id'])] = (p['home'], p['away'])
    if p['model'] == 'dixon_coles' and p['market'].endswith(' Win'):
        dcw[str(p['fixture_id'])] = (p['market'][:-4] == p['home'], p['predicted_prob_raw'])
hist = collections.defaultdict(lambda: collections.deque(maxlen=WIN))
lg = collections.defaultdict(lambda: collections.deque(maxlen=400))
rec = collections.defaultdict(lambda: [0, 0])
rows = []
for ko, fid, L, top, ht, at, he, ae, hg, ag in M:
    trusted = top and rec[L][1] >= 30 and wl(*rec[L]) >= .60
    gap = abs(he - ae); elo_home = he >= ae
    if trusted and fid in names:
        rough = None
        lgr = lg[L]
        if len(lgr) >= 50 and len(hist[ht]) >= 5 and len(hist[at]) >= 5:
            mh = np.mean([x[0] for x in lgr]); ma = np.mean([x[1] for x in lgr]); m = (mh + ma) / 2
            def ad(t):
                h = hist[t]; n = len(h)
                return ((sum(x[0] for x in h) + K * m) / (n + K) / m, (sum(x[1] for x in h) + K * m) / (n + K) / m)
            (ah, dh), (aa, da) = ad(ht), ad(at)
            ph, pa = pwin(mh * ah * da, ma * aa * dh), pwin(ma * aa * dh, mh * ah * da)
            rough = ph if elo_home else pa
        dc = dcw[fid][1] if fid in dcw and dcw[fid][0] == elo_home else None   # None = DC below 55% for this favourite
        today = gap > 250
        r_in = gap > 200 and rough is not None and rough >= .60
        d_in = gap > 250 and dc is not None and dc >= .65
        if today or r_in or d_in:
            home, away = names[fid]
            fav, dog = (home, away) if elo_home else (away, home)
            won = (hg > ag) if elo_home else (ag > hg)
            res = 'won' if won else ('draw' if hg == ag else 'lost')
            rows.append(dict(kickoff=ko[:10], league=f"{L[0]} ({L[1]})", favourite=fav, underdog=dog,
                             score=f"{hg}-{ag}", elo_gap=round(gap), rough_win=None if rough is None else round(rough, 3),
                             dc_win=dc and round(dc, 3), today=today, rough_rule=r_in, dc_rule=d_in, result=res))
    if abs(he - ae) > 250:
        rec[L][0] += (hg > ag) if he >= ae else (ag > hg); rec[L][1] += 1
    if lg is not None:
        hist[ht].append((hg, ag)); hist[at].append((ag, hg)); lg[L].append((hg, ag))
out = Path('engine/eval/diagnostics/out'); out.mkdir(exist_ok=True)
with open(out / 'big_dog_moves.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
def tally(s):
    return f"{len(s):3d}  won {sum(r['result']=='won' for r in s)/len(s):.0%}" if s else "  0"
for rule in ('rough_rule', 'dc_rule'):
    added = [r for r in rows if r[rule] and not r['today']]
    dropped = [r for r in rows if r['today'] and not r[rule]]
    kept = [r for r in rows if r['today'] and r[rule]]
    print(f"\n== {rule}: kept {tally(kept)} | added {tally(added)} | dropped {tally(dropped)}")
    for lbl, s in (('ADDED', added), ('DROPPED', dropped)):
        c = collections.Counter(r['favourite'] for r in s)
        print(f"  {lbl} by favourite: " + ", ".join(f"{t} {n}" for t, n in c.most_common(15)))
# upcoming slate
latest = sorted(glob.glob('engine/eval/consensus_slips_2026-*.json'))[-1]
d = json.load(open(latest)); fa = {f['fixture_id']: f for f in d['fixture_analysis']}
print(f"\n== upcoming Big dogs in {Path(latest).name}: DC win chance for the favourite")
for e in d['elo_gap_shortlist']:
    f = fa.get(e['fixture_id']); dc = None
    if f and f['dixon_coles']:
        x = f['dixon_coles'][0]; lh, la = x['lambda_home'], x['lambda_away']
        dc = pwin(lh, la) if e['favourite'] == e['home'] else pwin(la, lh)
    tag = '' if dc is None else ('keep' if dc >= .65 else 'DROP')
    print(f"  {e['kickoff'][:10]} {e['favourite']:26s} v {e['underdog']:24s} gap {e['elo_gap']:4d}  DC {('-' if dc is None else f'{dc:.0%}'):>4s} {tag}")
