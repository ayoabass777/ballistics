"""
Data quality detector events and checks.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import Json

from etl.src.data_detector.common import airflow_identity, coerce_iso, logger
from etl.src.extract_metadata import get_db_connection

RAW_FIXTURE_STALE_HOURS = 26
STANDINGS_STALE_DAYS = 7


def record_data_quality_event(
    *,
    severity: str,
    check_name: str,
    status: str,
    run_id: Optional[str] = None,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
    table_schema: Optional[str] = None,
    table_name: Optional[str] = None,
    source_s3_key: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
) -> None:
    """
    Insert one data quality event.
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_detector.data_quality_events (
                run_id,
                dag_id,
                task_id,
                severity,
                check_name,
                table_schema,
                table_name,
                source_s3_key,
                status,
                details,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                dag_id,
                task_id,
                severity,
                check_name,
                table_schema,
                table_name,
                source_s3_key,
                status,
                Json(details or {}),
                error_message,
            ),
        )
        conn.commit()


def record_data_quality_event_from_context(
    context: Dict[str, Any],
    *,
    severity: str,
    check_name: str,
    status: str,
    table_schema: Optional[str] = None,
    table_name: Optional[str] = None,
    source_s3_key: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
) -> None:
    """
    Best-effort Airflow-context wrapper for quality events.
    """
    identity = airflow_identity(context)
    try:
        record_data_quality_event(
            run_id=identity["run_id"],
            dag_id=identity["dag_id"],
            task_id=identity["task_id"],
            severity=severity,
            check_name=check_name,
            table_schema=table_schema,
            table_name=table_name,
            source_s3_key=source_s3_key,
            status=status,
            details=details,
            error_message=error_message,
        )
    except (RuntimeError, psycopg2.Error, OSError) as exc:
        logger.warning("Failed recording data quality event (%s): %s", check_name, exc)
    except Exception as exc:
        logger.warning("Unexpected detector failure while recording data quality event (%s): %s", check_name, exc)


def check_raw_fixture_stale_from_context(context: Dict[str, Any], stale_hours: int = RAW_FIXTURE_STALE_HOURS) -> None:
    """
    Warn only when current seasons exist, past unresolved fixtures exist, and
    raw.raw_fixtures has not been updated within the stale window.
    """
    identity = airflow_identity(context)
    try:
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_stg.stg_dim_league_seasons WHERE is_current")
            current_season_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM raw_stg.stg_raw_fixtures srf
                JOIN raw_stg.stg_dim_leagues l
                  ON srf.api_league_id = l.api_league_id
                JOIN raw_stg.stg_dim_league_seasons ls
                  ON l.league_id = ls.league_id
                 AND ls.season = srf.season
                WHERE srf.kickoff_utc < NOW() - INTERVAL '2 hour'
                  AND (srf.home_team_fulltime_goal IS NULL OR srf.away_team_fulltime_goal IS NULL)
                  AND ls.is_current
                """
            )
            unresolved_past_fixture_count = cur.fetchone()[0]

            cur.execute("SELECT MAX(updated_at), NOW() - MAX(updated_at) FROM raw.raw_fixtures")
            max_updated_at, update_age = cur.fetchone()

            is_stale = max_updated_at is None
            if max_updated_at is not None:
                cur.execute(
                    "SELECT MAX(updated_at) < NOW() - (%s * INTERVAL '1 hour') FROM raw.raw_fixtures",
                    (stale_hours,),
                )
                is_stale = bool(cur.fetchone()[0])

        if current_season_count and unresolved_past_fixture_count and is_stale:
            record_data_quality_event(
                run_id=identity["run_id"],
                dag_id=identity["dag_id"],
                task_id=identity["task_id"],
                severity="warning",
                check_name="raw_fixture_stale",
                table_schema="raw",
                table_name="raw_fixtures",
                status="open",
                details={
                    "stale_hours": stale_hours,
                    "current_season_count": current_season_count,
                    "unresolved_past_fixture_count": unresolved_past_fixture_count,
                    "max_updated_at": coerce_iso(max_updated_at),
                    "update_age": str(update_age) if update_age is not None else None,
                },
                error_message=f"raw.raw_fixtures has unresolved past fixtures and no update within {stale_hours}h",
            )
    except (RuntimeError, psycopg2.Error, OSError) as exc:
        logger.warning("Failed running raw_fixture_stale quality check: %s", exc)
    except Exception as exc:
        logger.warning("Unexpected detector failure while running raw_fixture_stale: %s", exc)


def check_standings_stale_from_context(context: Dict[str, Any], stale_days: int = STANDINGS_STALE_DAYS) -> None:
    """
    Warn when current league standings have not been updated within the stale window.
    """
    identity = airflow_identity(context)
    try:
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ls.league_season_id,
                    l.api_league_id,
                    ls.season,
                    MAX(rls.updated_at) AS max_updated_at,
                    NOW() - MAX(rls.updated_at) AS update_age
                FROM raw_stg.stg_dim_league_seasons ls
                JOIN raw_stg.stg_dim_leagues l
                  ON ls.league_id = l.league_id
                LEFT JOIN raw.raw_league_standings rls
                  ON rls.league_season_id = ls.league_season_id
                WHERE ls.is_current
                GROUP BY ls.league_season_id, l.api_league_id, ls.season
                HAVING MAX(rls.updated_at) IS NULL
                    OR MAX(rls.updated_at) < NOW() - (%s * INTERVAL '1 day')
                ORDER BY ls.league_season_id
                """,
                (stale_days,),
            )
            stale_rows = cur.fetchall()

        stale_leagues = [
            {
                "league_season_id": row[0],
                "api_league_id": row[1],
                "season": row[2],
                "max_updated_at": coerce_iso(row[3]),
                "update_age": str(row[4]) if row[4] is not None else None,
            }
            for row in stale_rows
        ]

        if stale_leagues:
            record_data_quality_event(
                run_id=identity["run_id"],
                dag_id=identity["dag_id"],
                task_id=identity["task_id"],
                severity="warning",
                check_name="standings_stale",
                table_schema="raw",
                table_name="raw_league_standings",
                status="open",
                details={
                    "stale_days": stale_days,
                    "stale_league_count": len(stale_leagues),
                    "stale_leagues": stale_leagues,
                },
                error_message=f"{len(stale_leagues)} current league standing(s) have no update within {stale_days} day(s)",
            )
    except (RuntimeError, psycopg2.Error, OSError) as exc:
        logger.warning("Failed running standings_stale quality check: %s", exc)
    except Exception as exc:
        logger.warning("Unexpected detector failure while running standings_stale: %s", exc)
