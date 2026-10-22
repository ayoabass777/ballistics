"""Reject isolation for incremental fixtures, standings and replay."""
import unittest
from unittest.mock import MagicMock, patch
from etl.src import dlq, fixtures_dlq as fd, standings_dlq as sd
from etl.src.update_standings import extract_standings
from etl.src.update_fixtures_main import quarantine_update_rows

class RemainingDLQTests(unittest.TestCase):
    def test_malformed_standings_and_invalid_numbers_remain_rejected(self):
        rows = extract_standings([{'league': {'standings': [[None, {'team': {'id': 1}, 'all': {'goals': None}}]]}}])
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(sd.standings_row_problem(r) for r in rows))
        for value in ('bad', 1.5, True, 2**40):
            self.assertTrue(sd.standings_row_problem({'api_team_id': 1, 'points': value}))
        self.assertIsNone(sd.standings_row_problem({'api_team_id': 1, 'points': -3}))

    def test_updates_park_before_dedupe_and_keep_existing_quarantine(self):
        good = {'api_fixture_id': 1, 'kickoff_utc': '2026-10-21T00:00:00Z',
                'fixture_status': 'NS', 'home_team_id': 2, 'away_team_id': 3}
        bad = {'api_fixture_id': 1, 'api_league_id': 39, 'season': 2026}
        old = {'api_fixture_id': 9, '_dlq_reason': 'null_team_id'}
        conn = MagicMock()
        with patch('etl.src.update_fixtures_main.get_db_connection', return_value=conn), \
             patch.object(dlq, 'read_json', return_value={'rows': [old]}), \
             patch.object(fd, 'park_fixture_rows', return_value=('key', {}, False)) as park, \
             patch.object(fd, 'set_status'):
            self.assertEqual(quarantine_update_rows([good, bad]), [good])
        self.assertEqual(len(park.call_args.args[0]), 2)

    def test_unassigned_update_retained(self):
        with patch('etl.src.update_fixtures_main.get_db_connection', return_value=MagicMock()), \
             patch.object(dlq, 'write_json') as write:
            self.assertEqual(quarantine_update_rows([{'_dlq_reason': 'malformed'}]), [])
        self.assertTrue(write.call_args.args[1]['requires_manual_review'])

    def test_standings_load_failure_does_not_stop_next_entry(self):
        entry = {'league_season_id': 1, 'api_league_id': 39, 'season': 2026}
        with patch.object(dlq, 'list_keys', return_value=['a/open.json', 'b/open.json']), \
             patch.object(dlq, 'read_json', return_value=entry), \
             patch.object(dlq, 'is_due', return_value=True), \
             patch('etl.src.update_standings.fetch_standings', return_value=[{}]), \
             patch('etl.src.update_standings.extract_standings', return_value=[{}]), \
             patch.object(sd, 'refresh_league', side_effect=[ValueError('bad SQL value'), (1, [])]), \
             patch.object(sd, '_failed_attempt') as failed:
            result = sd.replay_standings_dlq()
        self.assertEqual((result['still_bad'], result['resolved']), (1, 1))
        failed.assert_called_once()

    def test_fixture_landing_failure_does_not_stop_next_entry(self):
        entry = {'api_league_id': 39, 'season': 2026}
        conn = MagicMock()
        with patch.object(dlq, 'list_keys', return_value=['a/open.json', 'b/open.json']), \
             patch.object(dlq, 'read_json', return_value=entry), \
             patch.object(dlq, 'is_due', return_value=True), \
             patch('etl.src.extract_fixtures.fetch_fixtures', return_value=[]), \
             patch('etl.src.extract_fixtures.extract_fixtures_field', return_value=[{}]), \
             patch('etl.src.s3_landing.write_fixtures_to_s3', side_effect=[OSError('S3'), 'key']), \
             patch('etl.src.s3_landing.load_fixtures_from_s3', return_value=(1, [])), \
             patch('etl.src.extract_metadata.get_db_connection', return_value=conn), \
             patch.object(fd, '_failed_attempt') as failed:
            result = fd.replay_fixture_rows_dlq()
        self.assertEqual((result['still_bad'], result['resolved']), (1, 1))
        failed.assert_called_once()
        conn.commit.assert_called_once()
