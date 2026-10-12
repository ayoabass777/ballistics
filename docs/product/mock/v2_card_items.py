"""Featured (card) item per fixture for the D119 v2 pages, from the Oct 7 export.
Card wording from the Discover cards; values from the export so card and page agree."""
import json, sys
sys.path.insert(0, 'docs/product/mock')
from build_v2_rollout import PAT, GOAL, EV, pct
S = json.load(open('engine/eval/consensus_slips_2026-10-07.json'))
fa = {f['fixture_id']: f for f in S['fixture_analysis']}
GOALS = {66421: 'Birmingham Legion Over 1.5', 79912: 'Match Under 3.5', 81805: 'Kairat Almaty Over 1.5', 86064: 'Amed Over 1.5',
         107534: 'Barnet Over 0.5', 110189: 'Reims Over 1.5', 110194: 'Saint Etienne Over 1.5', 124383: 'Universitatea Craiova Over 0.5',
         128054: 'Kryvbas KR Over 1.5'}
PATS = {40116: ('home', 'halftime_win', 'all'), 42665: ('away', 'score_2goals', 'all'), 45064: ('away', 'score_2goals', 'away'),
        45065: ('home', 'concede_2', 'all'), 55123: ('away', 'goalless', 'away'), 63542: ('away', 'win', 'all'),
        63543: ('home', 'clean_sheet', 'all'), 63547: ('away', 'concede_2', 'all'), 67724: ('away', 'concede_2', 'all'), 119643: ('home', 'loss', 'all')}
MIRROR = {'loss': 'win', 'win': 'loss', 'concede_2': 'score_2goals', 'score_2goals': 'concede_2', 'clean_sheet': 'goalless', 'goalless': 'clean_sheet', 'halftime_win': None}
EVX = dict(EV, halftime_win='Led at half-time')
out = {}
for fid, mk in GOALS.items():
    d = next((x for x in fa[fid]['dixon_coles'] if x['market'] == mk), None)
    if not d: out[fid] = {'missing': mk}; continue
    ctx = d.get('league_rate_context', 'all')
    lab = (GOAL.get(d.get('league_comparison')) or 'League comparison unavailable') + (f" · league {pct(d['league_rate'])}" + (f" ({ctx})" if ctx != 'all' else '') if d.get('league_rate') is not None else '')
    out[fid] = {'kind': 'goal', 'market': mk.replace(' Over ', ' over ').replace(' Under ', ' under '), 'value': pct(d['probability']), 'label': lab,
                'card_headline': None, 'card_pct': f"{pct(d['probability'])} estimated chance"}
for fid, (side, ev, ctx) in PATS.items():
    f = fa[fid]; name = f[side]; other = 'away' if side == 'home' else 'home'
    b = next((x for x in f['bayesian'] if x['side'] == side and x['event'] == ev and x['context'] == ctx), None)
    if not b: out[fid] = {'missing': (side, ev, ctx)}; continue
    def line(x): return f"{PAT.get(x.get('league_comparison'), 'Typical for this league')}\n{pct(x['k']/x['n'])} vs league {pct(x['base_rate'])}"
    me = {'team': name, 'event': EVX[ev], 'record': f"{b['k']} / {b['n']}", 'line': line(b)}
    m_ev = MIRROR.get(ev); octx = 'all' if ctx == 'all' else ('home' if ctx == 'away' else 'away')
    m = next((x for x in f['bayesian'] if m_ev and x['side'] == other and x['event'] == m_ev and x['context'] == octx and x['n'] >= 6), None) \
        or next((x for x in f['bayesian'] if x['side'] == other and x['event'] == ev and x['context'] == octx and x['n'] >= 6), None)
    them = {'team': f[other], 'event': EVX[m['event']], 'record': f"{m['k']} / {m['n']}", 'line': line(m)} if m else None
    scope = {'all': 'all venues', 'away': 'away', 'home': 'home'}[ctx]
    label = 'Results' if ev in ('win', 'loss', 'halftime_win') else (f"{f['home']} defence vs {f['away']} attack" if (side == 'home') == ev.startswith(('concede', 'clean')) else f"{f['home']} attack vs {f['away']} defence")
    pair = {'label': label + (f' · {scope}' if ctx != 'all' else ''), 'home': me if side == 'home' else them, 'away': them if side == 'home' else me}
    out[fid] = {'kind': 'pattern', 'pair': pair, 'card': f"{b['k']}/{b['n']} matches · {scope}"}
print(json.dumps(out, ensure_ascii=False, separators=(',', ':')))
