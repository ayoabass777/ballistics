"""Standings keep API groups (conferences, split rounds) as separate tables."""
from etl.src import update_standings as us


def _row(team_id, rank, group, description=None):
    return {"rank": rank, "team": {"id": team_id, "name": f"T{team_id}"},
            "points": 10, "goalsDiff": 1, "form": "WWD", "group": group,
            "description": description,
            "all": {"played": 5, "win": 3, "draw": 1, "lose": 1,
                    "goals": {"for": 8, "against": 7}}}


def _payload(*tables):
    return [{"league": {"standings": list(tables)}}]


def test_conferences_keep_their_group_and_order():
    rows = us.extract_standings(_payload(
        [_row(1, 1, "Eastern Conference"), _row(2, 2, "Eastern Conference")],
        [_row(3, 1, "Western Conference")],
    ))
    assert [(r["api_team_id"], r["rank"], r["group_name"], r["group_order"])
            for r in rows] == [(1, 1, "Eastern Conference", 0),
                               (2, 2, "Eastern Conference", 0),
                               (3, 1, "Western Conference", 1)]


def test_split_season_team_is_kept_in_both_tables():
    rows = us.extract_standings(_payload(
        [_row(7, 1, "Regular Season"), _row(8, 2, "Regular Season")],
        [_row(7, 1, "Championship Round", "Promotion - Champions League")],
    ))
    for r in rows:
        r["league_season_id"] = 42
    kept = us.dedupe_rows(rows)
    assert len(kept) == 3          # team 7 appears in both groups
    assert kept[2]["description"] == "Promotion - Champions League"


def test_missing_group_label_falls_back_to_table_position():
    rows = us.extract_standings(_payload(
        [_row(5, 1, None)], [_row(6, 1, None)],
    ))
    assert [r["group_name"] for r in rows] == ["Table 1", "Table 2"]
    for r in rows:
        r["league_season_id"] = 1
    assert len(us.dedupe_rows(rows)) == 2   # unlabeled tables don't collide


def test_exact_repeat_in_the_same_group_is_deduped_last_wins():
    a = {"league_season_id": 1, "group_name": "G", "api_team_id": 9, "rank": 3}
    b = dict(a, rank=2)
    assert us.dedupe_rows([a, b]) == [b]


def test_upsert_conflict_and_columns_include_group():
    assert us.UPSERT_CONFLICT == "(league_season_id, group_name, api_team_id)"
    assert {"group_name", "group_order", "description"} <= set(us.COLUMNS)


def test_repeated_label_across_tables_gets_table_suffix():
    rows = us.extract_standings(_payload(
        [_row(1, 1, "USL Championship"), _row(2, 2, "USL Championship")],
        [_row(3, 1, "USL Championship")],
    ))
    assert [r["group_name"] for r in rows] == [
        "USL Championship - Table 1", "USL Championship - Table 1",
        "USL Championship - Table 2"]


class _Cur:
    def __init__(self):
        self.calls = []
        self.rowcount = 2

    def execute(self, sql, params):
        self.calls.append((sql, params))


def test_prune_deletes_only_refreshed_seasons_keys_not_in_payload():
    cur = _Cur()
    rows = [{"league_season_id": 6, "group_name": "Group A", "api_team_id": 1},
            {"league_season_id": 6, "group_name": "Group B", "api_team_id": 2}]
    assert us.prune_stale_rows(cur, rows) == 2
    (sql, params), = cur.calls
    assert "DELETE" in sql and "NOT IN" in sql
    assert params == (6, (("Group A", 1), ("Group B", 2)))


def test_prune_does_nothing_for_empty_payload():
    cur = _Cur()
    assert us.prune_stale_rows(cur, []) == 0
    assert cur.calls == []


def test_rows_without_team_id_are_split_out():
    rows = us.extract_standings(_payload(
        [_row(1, 1, "Clausura"), _row(None, 2, "Clausura"), _row(3, 3, "Clausura")],
    ))
    kept, missing = us.drop_rows_without_team_id(rows)
    assert [r["api_team_id"] for r in kept] == [1, 3]
    assert [r["rank"] for r in missing] == [2]
