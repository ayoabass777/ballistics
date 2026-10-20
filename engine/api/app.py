"""Local API. Run: uvicorn engine.api.app:app --host 127.0.0.1 --port 8001."""
from datetime import date, datetime
import logging
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Response
import psycopg2

from engine.api import forecast_records as records
from engine.api import match_context
from engine.api import match_detail

app = FastAPI(title="Ballistics Forecast Record", version="0.1.0")
log = logging.getLogger(__name__)


def read(operation, response, **kwargs):
    response.headers["Cache-Control"] = "no-store"
    try:
        with records.read_cursor() as cur:
            return operation(cur, **kwargs)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    except (psycopg2.Error, RuntimeError) as exc:
        # Do not expose DSNs, credentials, schema names or raw SQL in HTTP errors/logs.
        log.warning("Forecast ledger unavailable (%s)", type(exc).__name__)
        raise HTTPException(503, "Forecast record temporarily unavailable") from exc


@app.get("/api/forecast-record")
def forecast_record(response: Response, date_from: date | None = None, date_to: date | None = None,
                    team: str = Query("", max_length=100),
                    league: str | None = Query(None, max_length=150),
                    country: str | None = Query(None, max_length=100),
                    outcome: Literal["all","settled","other","pending","hit","miss","void",
                                     "unavailable","excluded"] = "all",
                    limit: int = Query(25, ge=1, le=100),
                    offset: int = Query(0, ge=0, le=100000), as_of: datetime | None = None):
    return read(records.list_records, response, date_from=date_from,date_to=date_to,team=team,
                league=league,country=country,outcome=outcome,limit=limit,offset=offset,as_of=as_of)


@app.get("/api/forecast-record/filters")
def forecast_filters(response: Response, as_of: datetime | None = None):
    return read(records.filters,response,as_of=as_of)


@app.get("/api/forecast-record/entries/{entry_id}/history")
def forecast_history(response: Response, entry_id: int, as_of: datetime | None = None,
                     after_id: int = Query(0,ge=0), limit: int = Query(50,ge=1,le=100)):
    if entry_id < 1:
        raise HTTPException(422,"entry_id must be positive")
    result = read(records.history,response,entry_id=entry_id,as_of=as_of,after_id=after_id,limit=limit)
    if result is None:
        raise HTTPException(404,"Forecast entry not found")
    return result


@app.get("/api/match/{fixture_id}/context")
def match_context_view(response: Response, fixture_id: int):
    """Standings and head-to-head for the match page Context card."""
    if fixture_id < 1:
        raise HTTPException(422, "fixture_id must be positive")
    result = read(match_context.build_context_page, response, fixture_id=fixture_id)
    if result is None:
        raise HTTPException(404, "Fixture not found")
    return result



@app.get("/api/match/{fixture_id}")
def match_view(response: Response, fixture_id: int):
    """Full match page: Context (DB snapshot) + Strength, Patterns, Goals (latest run)."""
    if fixture_id < 1:
        raise HTTPException(422, "fixture_id must be positive")
    result = read(match_detail.build_match, response, fixture_id=fixture_id)
    if result is None:
        raise HTTPException(404, "Fixture not found")
    return result


@app.get("/api/discover")
def discover_view(response: Response, zone: str = Query("UTC", max_length=100),
                  scope: Literal["week", "today", "tomorrow", "date", "range"] = "week",
                  day: date | None = None, end_day: date | None = None, leagues: str = Query("", max_length=2000)):
    from engine.api.discover import build_discover
    try:
        ids = tuple(int(x) for x in leagues.split(",") if x)
        if any(x < 1 for x in ids):
            raise ValueError()
    except ValueError as exc:
        raise HTTPException(422, "Invalid league IDs") from exc
    if scope == 'range' and (day is None or end_day is None or end_day < day):
        raise HTTPException(422, "Choose a valid date range")
    return read(build_discover, response, zone=zone, scope=scope, day=day, end_day=end_day, leagues=ids)
