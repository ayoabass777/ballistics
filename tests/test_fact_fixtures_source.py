"""The engine reads fixtures from dbt (raw.fact_fixtures, warehouse ids) only.

Guards the 2026-10-11 lineage change: no engine module may read
raw.raw_fixtures (API-Football team ids) or rebuild an API -> warehouse
team-id map from it.
"""

import re
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from engine.eval import db
from engine.eval import predict_mismatch as pm
from engine.eval.consensus_slips import _warehouse_ids

ENGINE = Path("engine")
DBT_MODELS = Path("dbt/football_pipeline/models")


def _engine_sources():
    for path in ENGINE.rglob("*.py"):
        if path.name == "db.py" or path.name.endswith("_lineage_check.py"):
            continue  # db.py comment; lineage audits compare raw vs fact on purpose
        yield path, path.read_text()


def test_engine_never_reads_raw_fixtures():
    # raw.raw_fixtures_status (DLQ flags, D148) is not fixture data
    offenders = [str(p) for p, src in _engine_sources() if re.search(r"raw\.raw_fixtures\b", src)]
    assert offenders == []


def test_no_team_id_translation_maps_left():
    markers = ("_build_team_id_map", "_load_id_maps", "AS rf_id")
    offenders = [f"{p}: {m}" for p, src in _engine_sources() for m in markers if m in src]
    assert offenders == []


def test_fixtures_table_is_the_dbt_fact():
    # League play only (knockouts excluded); full table kept for id lookups.
    assert db.FIXTURES_TABLE == "raw.engine_fixtures"
    assert db.ALL_FIXTURES_TABLE == "raw.fact_fixtures"


def test_staging_renames_api_team_ids():
    stg = (DBT_MODELS / "staging/stg_raw_fixtures.sql").read_text()
    assert "home_team_id AS home_api_team_id" in stg
    assert "away_team_id AS away_api_team_id" in stg


def test_fact_fixtures_outputs_warehouse_team_ids_only():
    sql = (DBT_MODELS / "facts/fact_fixtures.sql").read_text()
    # The final select (CTEs above it read staging for phase / stage).
    select_list = sql.split("\nfrom fixtures as rf")[0].split("\nselect\n")[-1]
    assert "ht.team_id                      as home_team_id" in select_list
    assert "at.team_id                      as away_team_id" in select_list
    assert "api_team_id" not in select_list


def test_export_ids_are_warehouse_ints_or_none():
    row = pd.Series({"home_team_id": 41.0, "away_team_id": float("nan"),
                     "league_id": 7})
    assert _warehouse_ids(row) == {"home_team_id": 41, "away_team_id": None,
                                   "league_id": 7}


def test_h2h_records_skip_unmapped_teams_without_borrowing_ids():
    played = pd.DataFrame({"team_id": [1, 1], "opponent_id": [2, 2],
                           "goals_for": [2, 0], "goals_against": [0, 0]})
    with patch.object(pm.pd, "read_sql", return_value=played) as read_sql:
        out = pm._load_h2h_records(object(), [(1, 2), (1, None)])
    assert out[(1, 2)] == (1, 1, 0)
    assert out[(1, None)] == (0, 0, 0)
    assert "IN (1,2)" in read_sql.call_args.args[0]
