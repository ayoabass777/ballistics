"""Bounded ledger reads. No schema creation, capture, settlement or raw DB fallback."""
from contextlib import contextmanager
from datetime import datetime, timezone
import os

import psycopg2
from psycopg2.extras import RealDictCursor

from engine.eval.forecast_history import POLICY

STATUSES = ("pending", "hit", "miss", "void", "unavailable", "excluded")
OUTCOMES = {"all": list(STATUSES), "settled": ["hit", "miss"],
            "other": ["void", "unavailable", "excluded"],
            **{s: [s] for s in STATUSES}}


def connect():
    dsn = os.environ.get("FORECAST_API_DSN")
    if not dsn:
        raise RuntimeError("FORECAST_API_DSN is required")
    return psycopg2.connect(dsn, connect_timeout=5)


@contextmanager
def read_cursor(factory=None):
    conn = (factory or connect)()
    try:
        conn.set_session(readonly=True, autocommit=False)
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '5s'")
            # Multiple readers coexist. Wait for the cooperating ledger writer
            # before issuing an as_of time, so its earlier timestamps are visible.
            cur.execute("SELECT pg_advisory_xact_lock_shared(734120260)")
            yield cur
    finally:
        conn.close()


def observation_time(cur, requested):
    cur.execute("SELECT clock_timestamp() AS now")
    now = cur.fetchone()["now"]
    if requested is None:
        return now
    if requested.tzinfo is None or requested > now:
        raise ValueError("as_of must be timezone-aware and not in the future")
    return requested.astimezone(timezone.utc)


# Select before filtering: an old probability must never reappear because its
# replacement has a different outcome or metadata. All parameters are bound.
BASE = """
WITH eligible AS MATERIALIZED (
 SELECT e.*, s.recorded_at
 FROM forecast.entries e JOIN forecast.snapshots s ON s.id=e.snapshot_id
 WHERE e.policy=%(policy)s AND e.eligible AND s.recorded_at <= %(as_of)s
), selected AS (
 SELECT DISTINCT ON (fixture_id,event_key) * FROM eligible
 ORDER BY fixture_id,event_key,recorded_at DESC,snapshot_id DESC,id DESC
), cutoffs AS (
 SELECT fixture_id, min(LEAST(kickoff,
   (detail->'fixture_at_capture'->>'kickoff_utc')::timestamptz)) AS selection_cutoff
 FROM eligible GROUP BY fixture_id
), events AS MATERIALIZED (
 SELECT e.id AS entry_id,e.fixture_id,e.event_key,e.probability,e.recorded_at AS captured_at,
   e.kickoff AS captured_kickoff,e.snapshot_id,e.detail,
   COALESCE(s.status,'pending') AS status,COALESCE(s.reason,'not_yet_checked') AS reason,
   s.observed_at AS outcome_observed_at,s.evidence,s.grader
 FROM selected e LEFT JOIN LATERAL (
   SELECT * FROM forecast.settlements WHERE entry_id=e.id AND observed_at <= %(as_of)s
   ORDER BY id DESC LIMIT 1
 ) s ON true
), fixture_meta AS (
 SELECT DISTINCT ON (fixture_id) fixture_id,captured_kickoff AS kickoff,
   detail->'fixture' AS fixture
 FROM events ORDER BY fixture_id,captured_at DESC,snapshot_id DESC,entry_id DESC
), cohort AS MATERIALIZED (
 SELECT e.*,m.kickoff,m.fixture,c.selection_cutoff
 FROM events e JOIN fixture_meta m USING (fixture_id) JOIN cutoffs c USING (fixture_id)
 WHERE (%(date_from)s::date IS NULL OR m.kickoff >= %(date_from)s::date::timestamp AT TIME ZONE 'UTC')
   AND (%(date_to)s::date IS NULL OR m.kickoff < (%(date_to)s::date + 1)::timestamp AT TIME ZONE 'UTC')
   AND (%(team)s = '' OR strpos(lower(COALESCE(m.fixture->>'home','')),lower(%(team)s)) > 0
                     OR strpos(lower(COALESCE(m.fixture->>'away','')),lower(%(team)s)) > 0)
   AND (%(league)s::text IS NULL OR m.fixture->>'league' = %(league)s)
   AND (%(country)s::text IS NULL OR m.fixture->>'country' = %(country)s)
), matching AS MATERIALIZED (
 SELECT * FROM cohort WHERE status = ANY(%(statuses)s)
), page AS (
 SELECT fixture_id,max(kickoff) AS kickoff FROM matching GROUP BY fixture_id
 ORDER BY kickoff DESC,fixture_id DESC LIMIT %(limit)s OFFSET %(offset)s
)
"""

LIST_QUERY = BASE + """
SELECT
 (SELECT count(DISTINCT fixture_id) FROM cohort) AS cohort_fixtures,
 (SELECT count(*) FROM cohort) AS cohort_forecasts,
 (SELECT count(DISTINCT fixture_id) FROM matching) AS total_fixtures,
 (SELECT count(*) FROM matching) AS total_forecasts,
 (SELECT COALESCE(jsonb_object_agg(status,n),'{}') FROM
   (SELECT status,count(*) AS n FROM cohort GROUP BY status) counts) AS status_counts,
 (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.kickoff DESC,r.fixture_id DESC,r.event_key),'[]')
  FROM (SELECT m.fixture_id,m.kickoff,m.fixture,m.selection_cutoff,
     m.entry_id,m.event_key,m.probability,m.captured_at,m.captured_kickoff,
     m.detail->'forecast'->>'market' AS event_label,
     m.status,m.reason,m.outcome_observed_at,m.evidence,m.grader
    FROM matching m JOIN page p USING (fixture_id)) r) AS rows
"""


def list_records(cur, *, as_of=None, date_from=None, date_to=None, team="", league=None,
                 country=None, outcome="all", limit=25, offset=0):
    as_of = observation_time(cur, as_of)
    if date_from and date_to and date_from > date_to:
        raise ValueError("date_from must not be after date_to")
    if outcome not in OUTCOMES or not 1 <= limit <= 100 or not 0 <= offset <= 100000:
        raise ValueError("invalid outcome or pagination")
    params = dict(policy=POLICY, as_of=as_of, date_from=date_from, date_to=date_to,
                  team=team.strip(), league=league, country=country,
                  statuses=OUTCOMES[outcome], limit=limit, offset=offset)
    cur.execute(LIST_QUERY, params)
    result = dict(cur.fetchone())
    fixtures = {}
    for row in result.pop("rows"):
        fid = row.pop("fixture_id")
        kickoff, meta, cutoff = (row.pop(k) for k in ("kickoff", "fixture", "selection_cutoff"))
        fixture = fixtures.setdefault(fid, dict(fixture_id=fid, kickoff=kickoff,
            **(meta or {}), selection_cutoff=cutoff,
            selection_state="final" if as_of >= datetime.fromisoformat(cutoff)
                            else "provisional", forecasts=[]))
        row["result_observation_state"] = "observed" if row["outcome_observed_at"] else "unobserved"
        fixture["forecasts"].append(row)
    next_offset = offset + len(fixtures)
    return dict(policy=POLICY, as_of=as_of, timezone="UTC",
        population="exported DC goal events, not all model outputs",
        filters=dict(date_from=date_from,date_to=date_to,team=team.strip(),league=league,
                     country=country,outcome=outcome),
        totals=dict(fixtures=result["total_fixtures"],forecasts=result["total_forecasts"]),
        cohort=dict(fixtures=result["cohort_fixtures"],forecasts=result["cohort_forecasts"],
                    status_counts={s:result["status_counts"].get(s,0) for s in STATUSES}),
        pagination=dict(limit=limit,offset=offset,
                        next_offset=next_offset if next_offset < result["total_fixtures"] else None),
        fixtures=list(fixtures.values()))


def filters(cur, *, as_of=None):
    as_of = observation_time(cur, as_of)
    cur.execute("""WITH selected AS (
        SELECT DISTINCT ON (e.fixture_id) e.detail->'fixture' AS fixture
        FROM forecast.entries e JOIN forecast.snapshots s ON s.id=e.snapshot_id
        WHERE e.policy=%s AND e.eligible AND s.recorded_at <= %s
        ORDER BY e.fixture_id,s.recorded_at DESC,s.id DESC,e.id DESC)
        SELECT DISTINCT fixture->>'country' AS country,fixture->>'league' AS league
        FROM selected WHERE fixture->>'league' IS NOT NULL
        ORDER BY country NULLS LAST,league""",(POLICY,as_of))
    return dict(as_of=as_of,leagues=[dict(r) for r in cur.fetchall()])


def history(cur, entry_id, *, as_of=None, after_id=0, limit=50):
    as_of = observation_time(cur, as_of)
    cur.execute("""SELECT e.id,e.fixture_id,e.event_key,e.probability,s.recorded_at AS captured_at
                   FROM forecast.entries e JOIN forecast.snapshots s ON s.id=e.snapshot_id
                   WHERE e.id=%s AND e.policy=%s AND e.eligible AND s.recorded_at <= %s""",
                (entry_id,POLICY,as_of))
    entry = cur.fetchone()
    if entry is None:
        return None
    cur.execute("""SELECT id,observed_at,status,reason,evidence,grader FROM forecast.settlements
                   WHERE entry_id=%s AND observed_at <= %s AND id > %s
                   ORDER BY id LIMIT %s""", (entry_id,as_of,after_id,limit+1))
    rows = [dict(r) for r in cur.fetchall()]
    more = len(rows) > limit
    rows = rows[:limit]
    return dict(policy=POLICY,as_of=as_of,forecast=dict(entry),observations=rows,
                next_after_id=rows[-1]["id"] if more else None)
