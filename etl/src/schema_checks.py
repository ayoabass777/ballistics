"""
Database schema checks for ETL bootstrap/update flows.

These checks fail fast when expected warehouse tables/columns are missing or
misconfigured, which is especially useful in new environments.
"""
from __future__ import annotations

from typing import Optional

import psycopg2.extensions


def _fetch_one_value(cur: psycopg2.extensions.cursor, sql: str, params: tuple = ()) -> Optional[object]:
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def assert_raw_fixtures_fixture_id_contract(cur: psycopg2.extensions.cursor) -> None:
    """
    Validate that raw.raw_fixtures exists and fixture_id behaves as a generated PK.

    Required contract:
    - table raw.raw_fixtures exists
    - column fixture_id exists
    - fixture_id is NOT NULL
    - fixture_id is the primary key
    - fixture_id is auto-generated (SERIAL/IDENTITY style)
    """
    table_exists = _fetch_one_value(cur, "select to_regclass('raw.raw_fixtures') is not null")
    if not table_exists:
        raise RuntimeError(
            "Missing table raw.raw_fixtures. Apply the bootstrap DDL (e.g. raw.raw_fixtures.sql) before running ETL."
        )

    cur.execute(
        """
        select
            c.is_nullable,
            c.column_default,
            c.identity_generation
        from information_schema.columns c
        where c.table_schema = 'raw'
          and c.table_name = 'raw_fixtures'
          and c.column_name = 'fixture_id'
        """
    )
    column_row = cur.fetchone()
    if not column_row:
        raise RuntimeError(
            "Missing column raw.raw_fixtures.fixture_id. Recreate/update raw.raw_fixtures with the expected schema."
        )

    is_nullable, column_default, identity_generation = column_row
    if str(is_nullable).upper() != "NO":
        raise RuntimeError("raw.raw_fixtures.fixture_id must be NOT NULL.")

    cur.execute(
        """
        select exists (
            select 1
            from pg_constraint con
            join pg_class rel on rel.oid = con.conrelid
            join pg_namespace nsp on nsp.oid = rel.relnamespace
            join unnest(con.conkey) with ordinality as cols(attnum, ord) on true
            join pg_attribute att on att.attrelid = rel.oid and att.attnum = cols.attnum
            where con.contype = 'p'
              and nsp.nspname = 'raw'
              and rel.relname = 'raw_fixtures'
              and att.attname = 'fixture_id'
        )
        """
    )
    is_primary_key = bool(cur.fetchone()[0])
    if not is_primary_key:
        raise RuntimeError("raw.raw_fixtures.fixture_id must be the PRIMARY KEY.")

    default_text = (column_default or "").strip()
    identity_text = (identity_generation or "").strip()
    if not default_text.startswith("nextval(") and not identity_text:
        raise RuntimeError(
            "raw.raw_fixtures.fixture_id must be auto-generated (SERIAL/IDENTITY). "
            "Expected a nextval(...) default or identity generation."
        )


def ensure_raw_fixtures_bootstrap_ready(cur: psycopg2.extensions.cursor) -> None:
    """
    Public wrapper for fixture ETL entrypoints.
    """
    assert_raw_fixtures_fixture_id_contract(cur)


def _assert_table_has_columns(
    cur: psycopg2.extensions.cursor,
    schema_name: str,
    table_name: str,
    required_columns: list[str],
) -> None:
    table_exists = _fetch_one_value(cur, f"select to_regclass('{schema_name}.{table_name}') is not null")
    if not table_exists:
        raise RuntimeError(f"Missing table {schema_name}.{table_name}. Run metadata bootstrap DDL first.")

    cur.execute(
        """
        select c.column_name
        from information_schema.columns c
        where c.table_schema = %s
          and c.table_name = %s
        """,
        (schema_name, table_name),
    )
    found = {row[0] for row in cur.fetchall()}
    missing = [col for col in required_columns if col not in found]
    if missing:
        raise RuntimeError(
            f"Table {schema_name}.{table_name} is missing required columns: {', '.join(missing)}"
        )


def ensure_metadata_bootstrap_ready(cur: psycopg2.extensions.cursor) -> None:
    """
    Validate the metadata/dim tables required by metadata + logo ETL.
    """
    _assert_table_has_columns(
        cur,
        "dim",
        "dim_countries",
        ["country_id", "country_name", "logo_file_id", "created_at", "updated_at"],
    )
    _assert_table_has_columns(
        cur,
        "dim",
        "dim_leagues",
        ["league_id", "country_id", "league_name", "api_league_id", "tier", "logo_file_id", "created_at", "updated_at"],
    )
    _assert_table_has_columns(
        cur,
        "dim",
        "dim_league_seasons",
        [
            "league_season_id",
            "league_id",
            "season",
            "season_label",
            "start_date",
            "end_date",
            "fixtures_bootstrap_done",
            "fixtures_bootstrap_at",
            "created_at",
            "updated_at",
        ],
    )
    _assert_table_has_columns(
        cur,
        "dim",
        "dim_venue",
        [
            "venue_id",
            "api_venue_id",
            "venue_name",
            "venue_capacity",
            "venue_city",
            "logo_file_id",
            "country_id",
            "created_at",
            "updated_at",
        ],
    )
    _assert_table_has_columns(
        cur,
        "dim",
        "dim_teams",
        [
            "team_id",
            "api_team_id",
            "team_name",
            "team_code",
            "country_id",
            "logo_file_id",
            "venue_id",
            "created_at",
            "updated_at",
        ],
    )


if __name__ == "__main__":
    from etl.src.config import EXTRACT_FIXTURES_LOG
    from etl.src.extract_metadata import get_db_connection
    from etl.src.logger import get_logger

    logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

    try:
        with get_db_connection() as conn, conn.cursor() as cur:
            ensure_raw_fixtures_bootstrap_ready(cur)
        logger.info("PASS: raw.raw_fixtures.fixture_id contract is valid.")
    except Exception as exc:
        logger.error("FAIL: raw fixtures schema check failed: %s", exc, exc_info=True)
        raise
