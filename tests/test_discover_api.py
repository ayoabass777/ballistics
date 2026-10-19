import unittest
from datetime import datetime, date, timezone
from engine.api.discover import snapshot, assemble, window

NOW = datetime(2026, 10, 14, 12, tzinfo=timezone.utc)
def row(fid=1, league=1, kickoff='2026-10-15T18:00:00Z', score=.2):
    pick={'list':'patterns','score':score,'team':'Home','event':'win','k':8,'n':10,'shrunk_rate':.9}
    return {'payload':{'discovery':snapshot(dict(fixture_id=fid, home='Home', away='Away', league_id=league, league='League', country='Country', kickoff=kickoff, discover_pick=pick, discover_sections={'patterns':pick}))}}

class DiscoverTests(unittest.TestCase):
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
    def test_missing_snapshot_is_explicit(self):
        self.assertEqual(assemble([{'payload':{}}],now=NOW)['coverage'],'needs_refresh')
    def test_invalid_zone(self):
        with self.assertRaises(ValueError): window('not-a-zone','week')
