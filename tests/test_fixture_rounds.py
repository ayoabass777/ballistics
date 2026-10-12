"""Fixture rounds: extracted from the API, carried by every loader, backfilled."""
from etl.src import backfill_fixture_rounds as bf
from etl.src import s3_landing, update_fixtures_main
from etl.src.extract_fixtures import extract_fixtures_field


def _fixture(fid, rnd):
    return {"fixture": {"id": fid, "date": "2026-10-01T18:00:00+00:00",
                        "status": {"short": "FT"}},
            "league": {"id": 253, "season": 2026, "round": rnd},
            "teams": {"home": {"id": 1, "name": "A"}, "away": {"id": 2, "name": "B"}},
            "score": {"halftime": {"home": 0, "away": 0},
                      "fulltime": {"home": 1, "away": 0}},
            "goals": {"home": 1, "away": 0}}


def test_extract_keeps_round():
    rows = extract_fixtures_field([_fixture(10, "Play-offs"), _fixture(11, None)])
    assert [r["round"] for r in rows] == ["Play-offs", None]


def test_loaders_write_round():
    assert "round" in s3_landing.RAW_COLUMNS
    assert "round" in update_fixtures_main.RAW_COLUMNS


def test_backfill_pairs_skip_missing_ids_and_rounds():
    payload = [_fixture(10, "Final"), _fixture(11, None),
               {"fixture": {}, "league": {"round": "Final"}}, _fixture(10, "Final")]
    assert bf.rounds_from_payload(payload) == [(10, "Final")]


def test_backfill_only_writes_round():
    sql = bf.UPDATE_SQL.split("SET", 1)[1].split("FROM", 1)[0]
    assert {c.split("=")[0].strip() for c in sql.split(",")} == {"round", "updated_at"}
    assert "IS DISTINCT FROM" in bf.UPDATE_SQL
