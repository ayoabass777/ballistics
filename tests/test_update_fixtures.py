from __future__ import annotations

from unittest.mock import patch

from etl.src import update_fixtures


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executions = []

    def execute(self, query, params=None):
        self.executions.append((query, params))

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows):
        self.cursor_obj = FakeCursor(rows)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_fixture_id_selector_includes_recently_ended_seasons():
    conn = FakeConnection([(42, 1532879)])

    with patch.object(update_fixtures, "get_db_connection", return_value=conn):
        result = update_fixtures.to_update_fixture_ids()

    query, params = conn.cursor_obj.executions[0]
    assert result == [(42, "1532879")]
    assert "ls.is_current" in query
    assert "ls.end_date >= CURRENT_DATE" in query
    assert params == (update_fixtures.FIXTURE_CATCHUP_GRACE_DAYS,)
    assert conn.closed


def test_batch_selector_includes_recently_ended_seasons():
    conn = FakeConnection([])

    with patch.object(update_fixtures, "get_db_connection", return_value=conn):
        result = update_fixtures.batch_update_fixtures()

    query, params = conn.cursor_obj.executions[0]
    assert result == []
    assert "ls.is_current" in query
    assert "ls.end_date >= CURRENT_DATE" in query
    assert params == (update_fixtures.FIXTURE_CATCHUP_GRACE_DAYS,)
    assert conn.closed
