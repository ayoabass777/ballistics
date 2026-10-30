import unittest
from datetime import datetime, date, timedelta, timezone
from engine.api.discover import snapshot, assemble, window, build_discover

NOW = datetime(2026, 10, 14, 12, tzinfo=timezone.utc)
def row(fid=1, league=1, kickoff='2026-10-15T18:00:00Z', score=.2):
    pick={'list':'patterns','score':score,'team':'Home','event':'win','k':8,'n':10,'shrunk_rate':.9}
    return {'payload':{'discovery':snapshot(dict(fixture_id=fid, home='Home', away='Away', league_id=league, league='League', country='Country', kickoff=kickoff, discover_pick=pick, discover_sections={'patterns':pick}))}}

class DiscoverTests(unittest.TestCase):
    def test_live_status_filters_postponed_fixture_from_snapshot(self):
        kickoff = (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=1)).isoformat()
        active = row(1, kickoff=kickoff)
        postponed = row(2, kickoff=kickoff)
        records = [
            {**active, 'run_ds': date(2026, 10, 24), 'finished_at': datetime.now(timezone.utc), 'live_status': 'NS'},
            {**postponed, 'run_ds': date(2026, 10, 24), 'finished_at': datetime.now(timezone.utc), 'live_status': 'PST'},
        ]
        class Cursor:
            def execute(self, sql):
                self.sql = sql
            def fetchone(self):
                return {'ready': True}
            def fetchall(self):
                return records
        result = build_discover(Cursor())
        self.assertEqual([x['fixture_id'] for x in result['matches']], [1])
        self.assertEqual([x['fixture_id'] for x in result['lists']['standouts']], [1])

    def test_rank_and_filter_no_private_fields(self):
        result=assemble([row(1,1,score=.2),row(2,2,score=.5),row(3,1,score=.3)], leagues=(1,),now=NOW)
        self.assertEqual([r['fixture_id'] for r in result['lists']['standouts']],[3,1])
        self.assertEqual(len(result['leagues']),2)
        self.assertNotIn('score',str(result))
        self.assertNotIn('shrunk_rate',str(result))
    def test_matches_include_unranked_and_order_by_kickoff(self):
        unranked = row(3, kickoff='2026-10-15T10:00:00Z')
        unranked['payload']['discovery']['standout'] = None
        unranked['payload']['discovery']['sections'] = {}
        result = assemble([row(1), unranked, row(2, league=2)], leagues=(1,), now=NOW)
        self.assertEqual([x['fixture_id'] for x in result['matches']], [3, 1])
        self.assertTrue(all(x['pick'] is None for x in result['matches']))
        self.assertIsNone(result['matches'][0]['preview'])
        self.assertEqual(result['matches'][1]['preview']['list'], 'patterns')

    def test_matches_preview_uses_highest_section_score_without_leaking_score(self):
        item = row()
        item['payload']['discovery']['sections'] = {
            'strength': {'list': 'strength', 'score': .3, 'favourite': 'Home'},
            'goals': {'list': 'goals', 'score': .5, 'market': 'Match Over 1.5', 'probability': .76},
            'patterns': {'list': 'patterns', 'score': .4, 'team': 'Away', 'event': 'win', 'k': 8, 'n': 10}}
        preview = assemble([item], now=NOW)['matches'][0]['preview']
        self.assertEqual((preview['list'], preview['market'], preview['probability']), ('goals', 'Match Over 1.5', .76))
        self.assertNotIn('score', preview)

    def test_logo_urls_follow_fixture_assets_without_exposing_file_ids(self):
        item = row()
        item.update(home_logo_file_id='home-file', away_logo_file_id=None, league_logo_file_id='league-file')
        result = assemble([item], now=NOW)
        for card in [result['matches'][0], result['lists']['standouts'][0]]:
            self.assertTrue(card['home_crest_url'].endswith('/crests/teams/home-file'))
            self.assertIsNone(card['away_crest_url'])
            self.assertTrue(card['league_logo_url'].endswith('/crests/leagues/league-file'))
            self.assertNotIn('home_logo_file_id', card)

    def test_calendar_window_dst(self):
        start,end=window('America/New_York','week',now=datetime(2026,10,31,18,tzinfo=timezone.utc))
        self.assertEqual(end.isoformat(),'2026-11-07T00:00:00-05:00')
        self.assertEqual(start.hour,18)
    def test_selected_day_includes_midnight_excludes_next_day(self):
        rows=[row(1,kickoff='2026-10-15T00:00:00Z'),row(2,kickoff='2026-10-16T00:00:00Z'),row(3,kickoff='2026-10-14T12:00:00Z')]
        result=assemble(rows,scope='date',day=date(2026,10,15),now=NOW)
        self.assertEqual([x['fixture_id'] for x in result['lists']['standouts']],[1])
    def test_custom_range_includes_both_calendar_days_and_validates_order(self):
        rows=[row(1,kickoff='2026-10-15T00:00:00Z'),row(2,kickoff='2026-10-16T23:00:00Z'),row(3,kickoff='2026-10-17T00:00:00Z')]
        result=assemble(rows,scope='range',day=date(2026,10,15),end_day=date(2026,10,16),now=NOW)
        self.assertEqual([x['fixture_id'] for x in result['matches']],[1,2])
        with self.assertRaises(ValueError):
            window('UTC','range',day=date(2026,10,16),end_day=date(2026,10,15),now=NOW)
    def test_missing_snapshot_is_explicit(self):
        self.assertEqual(assemble([{'payload':{}}],now=NOW)['coverage'],'needs_refresh')
    def test_invalid_zone(self):
        with self.assertRaises(ValueError): window('not-a-zone','week')


class DiscoverCacheTests(unittest.TestCase):
    """discover_json: per-process cache keyed on the latest complete run."""

    def setUp(self):
        from engine.api import discover as d
        self.d = d
        d.clear_cache()
        self.addCleanup(d.clear_cache)
        self.run = {'run_ds': date(2026, 10, 29), 'finished_at': datetime(2026, 10, 30, 0, 14, tzinfo=timezone.utc)}
        kickoff = (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=1)).isoformat()
        self.rows = [{**row(1, kickoff=kickoff), **self.run, 'live_status': 'NS'}]
        self.row_queries = 0
        test = self

        class Cursor:
            def execute(self, sql, params=None):
                self.sql = sql
            def fetchone(self):
                return {'ready': True} if 'to_regclass' in self.sql else test.run
            def fetchall(self):
                test.row_queries += 1
                return test.rows
        self.cur = Cursor()

    def get(self, **kw):
        import json
        return json.loads(self.d.discover_json(self.cur, zone='UTC', **kw))

    def test_repeat_request_is_served_from_cache(self):
        first = self.get()
        self.assertEqual(self.get(), first)
        self.assertEqual(self.row_queries, 1)
        self.assertEqual([x['fixture_id'] for x in first['lists']['standouts']], [1])

    def test_new_run_is_picked_up_on_the_next_request(self):
        self.get()
        self.run = {**self.run, 'run_ds': date(2026, 10, 30)}
        self.rows = [{**r, **self.run} for r in self.rows]        # the query reads the new run's rows
        self.assertEqual(self.get()['run_ds'], '2026-10-30')
        self.assertEqual(self.row_queries, 2)

    def test_parameters_share_the_rows_but_not_the_response(self):
        week, today = self.get(), self.get(scope='today')
        self.assertEqual(self.row_queries, 1)
        self.assertNotEqual(week['window'], today['window'])

    def test_response_expires_but_rows_stay(self):
        from unittest.mock import patch
        self.get()
        later = self.d.clock.monotonic() + self.d.RESPONSE_TTL + 1
        with patch.object(self.d.clock, 'monotonic', return_value=later):
            self.get()
        self.assertEqual(self.row_queries, 1)
        with patch.object(self.d.clock, 'monotonic', return_value=later + self.d.ROWS_TTL):
            self.get()
        self.assertEqual(self.row_queries, 2)

    def test_response_cache_is_bounded(self):
        from unittest.mock import patch
        with patch.object(self.d, 'RESPONSE_MAX', 3):
            for league in range(1, 6):
                self.get(leagues=(league,))
            self.assertEqual(len(self.d._responses), 3)

    def test_same_payload_as_build_discover(self):
        import json
        cached = self.get()
        uncached = json.loads(json.dumps(build_discover(self.cur, zone='UTC'), default=str))
        for doc in (cached, uncached):
            doc['window'].pop('from')
        self.assertEqual(cached, uncached)
