"""Discover lists from the latest atomic engine snapshot; no scoring on read."""
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from engine.api.crests import crest_url

KINDS = ('strength', 'goals', 'patterns')
PICK_FIELDS = ('list', 'favourite', 'underdog', 'team', 'event', 'context', 'k', 'n', 'market', 'probability', 'big_dog')


def snapshot(f):
    # Scores remain server-side. Preserve the engine's eligibility and tie choices.
    def pick(p):
        return {k: p.get(k) for k in (*PICK_FIELDS, 'score')} if p else None
    fixture = {k: f.get(k) for k in ('fixture_id', 'home', 'away', 'league_id', 'league', 'country', 'kickoff')}
    if fixture.get('kickoff'):
        fixture['kickoff'] = datetime.fromisoformat(str(fixture['kickoff']).replace('Z', '+00:00')).isoformat()
    return {
        'fixture': fixture,
        'standout': pick(f.get('discover_pick')),
        'sections': {k: pick(v) for k, v in (f.get('discover_sections') or {}).items() if k in KINDS},
    }


def window(zone, scope, day=None, now=None, end_day=None):
    now = now or datetime.now(timezone.utc)
    try:
        tz = ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError('Unknown timezone') from exc
    today = now.astimezone(tz).date()
    start_day = day if scope in ('date', 'range') else today + timedelta(days=scope == 'tomorrow')
    if start_day is None:
        raise ValueError('Choose a date')
    if scope == 'range' and (end_day is None or end_day < start_day):
        raise ValueError('Choose a valid date range')
    start = datetime.combine(start_day, time.min, tzinfo=tz)
    end = datetime.combine((end_day + timedelta(days=1)) if scope == 'range' else (start_day + timedelta(days=7 if scope == 'week' else 1)), time.min, tzinfo=tz)
    return max(start, now), end


def assemble(rows, *, zone='UTC', scope='week', day=None, end_day=None, leagues=(), now=None):
    now = now or datetime.now(timezone.utc)
    start, end = window(zone, scope, day, now, end_day)
    lists = {k: [] for k in ('standouts', *KINDS)}
    matches = []
    directory = {}
    missing = 0
    for row in rows:
        data = row['payload'].get('discovery')
        if data is None:
            missing += 1
            continue
        f = {**data['fixture'],
             'home_crest_url': crest_url('teams', row.get('home_logo_file_id')),
             'away_crest_url': crest_url('teams', row.get('away_logo_file_id')),
             'league_logo_url': crest_url('leagues', row.get('league_logo_file_id'))}
        if f.get('league_id') is not None:
            directory[f['league_id']] = {k: f[k] for k in ('league_id', 'league', 'country', 'league_logo_url')}
        try:
            kickoff = datetime.fromisoformat(str(f['kickoff']).replace('Z', '+00:00'))
            inside = start <= kickoff < end and kickoff > now
        except (ValueError, TypeError):
            continue
        if not inside or (leagues and f.get('league_id') not in leagues):
            continue
        # Matches stays chronological. Its optional preview uses the strongest
        # already-qualified section pick; private scores never leave the API.
        sections = data.get('sections') or {}
        candidates = [sections[k] for k in KINDS if sections.get(k) and isinstance(sections[k].get('score'), (int, float))]
        leading = max(candidates, key=lambda p: p['score'], default=None)
        matches.append({**f, 'pick': None,
                        'preview': {k: leading.get(k) for k in PICK_FIELDS} if leading else None})
        for name, p in [('standouts', data.get('standout')), *data.get('sections', {}).items()]:
            if name in lists and p:
                lists[name].append({**f, 'pick': {k: p.get(k) for k in PICK_FIELDS}, '_score': p['score']})
    for entries in lists.values():
        entries.sort(key=lambda f: (-f['_score'], f['kickoff'], f['fixture_id']))
        for entry in entries:
            del entry['_score']
    return {'matches': sorted(matches, key=lambda f: (f['kickoff'], f['fixture_id'])), 'lists': lists, 'leagues': sorted(directory.values(), key=lambda x: (x['country'] or '', x['league'] or '')),
            'coverage': 'needs_refresh' if missing else 'ready', 'timezone': zone,
            'window': {'from': start.isoformat(), 'to': end.isoformat()}}


def build_discover(cur, zone='UTC', scope='week', day=None, end_day=None, leagues=()):
    # Validate before touching the database.
    window(zone, scope, day, end_day=end_day)
    cur.execute("SELECT to_regclass('raw.engine_runs') IS NOT NULL AS ready")
    if not cur.fetchone()['ready']:
        return {**assemble([], zone=zone, scope=scope, day=day, end_day=end_day, leagues=leagues), 'run_ds': None, 'run_finished_at': None, 'coverage': 'needs_refresh'}
    cur.execute("""WITH latest AS (SELECT run_ds, finished_at FROM raw.engine_runs
        WHERE status = 'complete' ORDER BY run_ds DESC LIMIT 1)
        SELECT latest.run_ds, latest.finished_at, m.payload,
               f.home_logo_file_id, f.away_logo_file_id, l.logo_file_id AS league_logo_file_id
        FROM latest
        LEFT JOIN raw.match_analysis m USING (run_ds)
        LEFT JOIN raw.fact_fixtures f ON f.fixture_id = m.fixture_id
        LEFT JOIN dim.dim_leagues l ON l.league_id = f.league_id""")
    rows = cur.fetchall()
    return {**assemble([r for r in rows if r['payload']], zone=zone, scope=scope, day=day, end_day=end_day, leagues=leagues),
            **({'coverage': 'needs_refresh'} if not rows else {}),
            'run_ds': str(rows[0]['run_ds']) if rows else None,
            'run_finished_at': rows[0]['finished_at'].isoformat() if rows else None}
