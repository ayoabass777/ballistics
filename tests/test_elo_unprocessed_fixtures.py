from unittest.mock import MagicMock

from engine.elo.elo_service import fetch_unprocessed_fixtures


def test_unprocessed_fixtures_skip_played_rows_without_a_score():
    cur = MagicMock()
    cur.fetchall.return_value = []
    fetch_unprocessed_fixtures(cur)
    sql = " ".join(cur.execute.call_args[0][0].split())
    assert "f.goals_for IS NOT NULL" in sql
    assert "f.goals_against IS NOT NULL" in sql
