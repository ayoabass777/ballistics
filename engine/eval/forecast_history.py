"""Append-only, prospective DC goal forecast record. See docs/forecast_history.md.

No historical-file import and no caller-supplied recorded_at. The database clock
and fixture state at capture determine eligibility, never Airflow's logical date.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import Json, RealDictCursor

from engine.eval.db import get_conn

POLICY = "latest_pre_kickoff_dc_goals_v1"
GRADER = "regulation_goals_v1"
HERE = Path(__file__).parent
DDL = """
CREATE SCHEMA IF NOT EXISTS forecast;
CREATE TABLE IF NOT EXISTS forecast.snapshots (
    id bigserial PRIMARY KEY,
    content_hash text NOT NULL UNIQUE,
    run_ds date NOT NULL,
    recorded_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    provenance jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS forecast.entries (
    id bigserial PRIMARY KEY,
    snapshot_id bigint NOT NULL REFERENCES forecast.snapshots(id),
    fixture_id bigint NOT NULL,
    event_key text NOT NULL,
    probability double precision NOT NULL CHECK (probability >= 0 AND probability <= 1),
    kickoff timestamptz NOT NULL,
    eligible boolean NOT NULL,
    eligibility_reason text NOT NULL,
    policy text NOT NULL,
    detail jsonb NOT NULL,
    UNIQUE(snapshot_id, fixture_id, event_key)
);
CREATE INDEX IF NOT EXISTS forecast_entries_fixture ON forecast.entries(fixture_id, event_key);
CREATE TABLE IF NOT EXISTS forecast.settlements (
    id bigserial PRIMARY KEY,
    entry_id bigint NOT NULL REFERENCES forecast.entries(id),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    grader text NOT NULL,
    status text NOT NULL CHECK(status IN ('pending','hit','miss','void','unavailable','excluded')),
    reason text NOT NULL,
    evidence jsonb NOT NULL,
    evidence_hash text NOT NULL
);
CREATE INDEX IF NOT EXISTS forecast_settlements_entry ON forecast.settlements(entry_id, id DESC);
CREATE OR REPLACE FUNCTION forecast.reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
    RAISE EXCEPTION 'forecast history is append-only';
END $$;
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['snapshots','entries','settlements'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'forecast_no_mutation'
                   AND tgrelid = ('forecast.' || t)::regclass) THEN
      EXECUTE format('CREATE TRIGGER forecast_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE ON forecast.%I FOR EACH STATEMENT EXECUTE FUNCTION forecast.reject_mutation()', t);
    END IF;
  END LOOP;
END $$;
CREATE OR REPLACE VIEW forecast.first_forecasts AS
SELECT DISTINCT ON (e.fixture_id, e.event_key, e.policy)
       e.*, s.recorded_at, s.run_ds
FROM forecast.entries e JOIN forecast.snapshots s ON s.id=e.snapshot_id
WHERE e.eligible
ORDER BY e.fixture_id, e.event_key, e.policy, s.recorded_at, s.id, e.id;
CREATE OR REPLACE VIEW forecast.latest_forecasts AS
SELECT DISTINCT ON (e.fixture_id, e.event_key, e.policy)
       e.*, s.recorded_at, s.run_ds
FROM forecast.entries e JOIN forecast.snapshots s ON s.id=e.snapshot_id
WHERE e.eligible
ORDER BY e.fixture_id, e.event_key, e.policy, s.recorded_at DESC, s.id DESC, e.id DESC;
"""


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def utc(value):
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("forecast timestamps must include a timezone")
    return dt.astimezone(timezone.utc)


def event_key(market, home, away):
    """Stable side-based identity; deliberately excludes win/strength signals."""
    if market == "Match Goals 1-4":
        return "match:bounds:1:4"
    if market in ("BTTS Yes", "BTTS No"):
        return "btts:" + market.split()[-1].lower()
    for label, side in (("Match", "match"), (home, "home"), (away, "away")):
        if not label:
            continue
        m = re.fullmatch(re.escape(label) + r" (Over|Under) (\d+\.5)", market)
        if m:
            return f"{side}:{m[1].lower()}:{float(m[2]):g}"
    return None


def extract_entries(payload):
    rows, seen = [], set()
    for fx in payload.get("fixture_analysis", []):
        for prediction in fx.get("dixon_coles", []):
            key = event_key(prediction["market"], fx.get("home"), fx.get("away"))
            if key is None:
                continue  # Retained in the complete snapshot, not a supported goal event.
            fid = int(fx["fixture_id"])
            if (fid, key) in seen:
                raise ValueError(f"duplicate DC forecast: {fid}/{key}")
            seen.add((fid, key))
            p = float(prediction["probability"])
            if not math.isfinite(p) or not 0 <= p <= 1:
                raise ValueError(f"invalid probability: {fid}/{key}")
            rows.append(dict(fixture_id=fid, event_key=key, probability=p,
                             kickoff=utc(fx["kickoff"]), detail={
                                 "forecast": prediction,
                                 "fixture": {k: fx.get(k) for k in ("home", "away", "league", "country")},
                                 "source": "fixture_analysis.dixon_coles"}))
    return rows


def eligibility(entry, fixture, recorded_at):
    if entry["kickoff"] <= recorded_at:
        return False, "captured_at_or_after_exported_kickoff"
    if not fixture or not fixture.get("kickoff_utc"):
        return False, "fixture_or_kickoff_unavailable"
    if utc(fixture["kickoff_utc"]) <= recorded_at:
        return False, "captured_at_or_after_current_kickoff"
    if fixture.get("fixture_status") not in ("NS", "TBD"):
        return False, "fixture_not_pre_match"
    return True, "pre_kickoff_capture"


def capture_provenance():
    # These fingerprint capture-time files, not the earlier training execution.
    files = sorted(HERE.glob("*.py")) + [HERE / "calibration_maps.json"]
    return {"capture_files_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                     for p in files if p.is_file()},
            "model_training_cutoff": None, "model_run_version": None,
            "scope": "capture-time files; training provenance unavailable"}


def init_schema(conn=None):
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(734120260)")
            cur.execute(DDL)
    finally:
        if own:
            conn.close()


def capture(payload, conn=None):
    """Capture before publishing. Failure must stop publication, not be swallowed.

    Caller must initialise the schema first. Identical payload retries return the
    original snapshot. Changed runs append; database time cannot be backdated.
    """
    entries = extract_entries(payload)
    content_hash = digest(payload)
    provenance = capture_provenance()
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Serialize capture and settlement, including same-payload retries.
            cur.execute("SELECT pg_advisory_xact_lock(734120260)")
            cur.execute("SELECT id FROM forecast.snapshots WHERE content_hash=%s", (content_hash,))
            existing = cur.fetchone()
            if existing:
                return existing["id"]
            cur.execute("SELECT clock_timestamp() AS ts")
            recorded_at = cur.fetchone()["ts"]
            ids = sorted({e["fixture_id"] for e in entries})
            cur.execute("""SELECT fixture_id, kickoff_utc, fixture_status
                           FROM raw.fact_fixtures WHERE fixture_id = ANY(%s)""", (ids,))
            fixtures = {r["fixture_id"]: dict(r) for r in cur.fetchall()}
            # Never reopen a fixture after a previously captured kickoff passes,
            # even if the provider later moves it forward or resets its status.
            cur.execute("""SELECT DISTINCT fixture_id FROM forecast.entries
                           WHERE fixture_id = ANY(%s) AND policy=%s AND eligible
                             AND LEAST(kickoff,
                               (detail->'fixture_at_capture'->>'kickoff_utc')::timestamptz) <= %s""",
                        (ids, POLICY, recorded_at))
            closed_fixtures = {r["fixture_id"] for r in cur.fetchall()}
            cur.execute("""INSERT INTO forecast.snapshots
                           (content_hash, run_ds, recorded_at, payload, provenance)
                           VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                        (content_hash, payload["run_ds"], recorded_at,
                         Json(payload, dumps=encoded), Json(provenance)))
            snapshot_id = cur.fetchone()["id"]
            for entry in entries:
                fixture = fixtures.get(entry["fixture_id"])
                eligible, reason = eligibility(entry, fixture, recorded_at)
                if entry["fixture_id"] in closed_fixtures:
                    eligible, reason = False, "previously_captured_kickoff_reached"
                detail = {**entry["detail"], "fixture_at_capture": fixture}
                cur.execute("""INSERT INTO forecast.entries
                               (snapshot_id,fixture_id,event_key,probability,kickoff,
                                eligible,eligibility_reason,policy,detail)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (snapshot_id, entry["fixture_id"], entry["event_key"],
                             entry["probability"], entry["kickoff"], eligible, reason,
                             POLICY, Json(detail, dumps=encoded)))
            return snapshot_id
    finally:
        if own:
            conn.close()


def grade(entry, fixture):
    if not fixture or not fixture.get("kickoff_utc"):
        return "unavailable", "fixture_or_kickoff_unavailable"
    if utc(fixture["kickoff_utc"]) <= utc(entry["recorded_at"]):
        return "excluded", "corrected_kickoff_precedes_capture"
    status = fixture.get("fixture_status")
    if status == "CANC":
        return "void", "fixture_cancelled"
    if status in ("NS", "TBD", "PST", "1H", "HT", "2H", "LIVE", "INT", "SUSP", "ET", "BT", "P"):
        return "pending", "awaiting_regulation_result"
    if status != "FT":
        return "unavailable", "unsupported_status_manual_review"  # ABD, AWD, WO, AET, PEN
    hg, ag = fixture.get("home_team_fulltime_goal"), fixture.get("away_team_fulltime_goal")
    if any(x is None or isinstance(x, bool) or int(x) != x or x < 0 for x in (hg, ag)):
        return "unavailable", "invalid_or_missing_score"
    key = entry["event_key"]
    if key == "match:bounds:1:4":
        hit = 1 <= hg + ag <= 4
    elif key in ("btts:yes", "btts:no"):
        hit = (hg > 0 and ag > 0) == (key == "btts:yes")
    else:
        m = re.fullmatch(r"(match|home|away):(over|under):(\d+\.5)", key)
        if not m:
            return "unavailable", "unsupported_event"
        goals = {"match": hg + ag, "home": hg, "away": ag}[m[1]]
        hit = goals > float(m[3]) if m[2] == "over" else goals < float(m[3])
    return ("hit" if hit else "miss"), "regulation_time_score"


def settle(conn=None):
    """Append changed observations, including score corrections and reversals."""
    own = conn is None
    conn = conn or get_conn()
    count = 0
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(734120260)")
            cur.execute("SELECT * FROM forecast.latest_forecasts WHERE policy=%s", (POLICY,))
            entries = list(cur.fetchall())
            ids = sorted({e["fixture_id"] for e in entries})
            cur.execute("""SELECT fixture_id, kickoff_utc, fixture_status,
                                  home_team_fulltime_goal, away_team_fulltime_goal
                           FROM raw.fact_fixtures WHERE fixture_id = ANY(%s)""", (ids,))
            fixtures = {r["fixture_id"]: dict(r) for r in cur.fetchall()}
            for entry in entries:
                fixture = fixtures.get(entry["fixture_id"])
                status, reason = grade(entry, fixture)
                evidence = {"fixture": fixture, "status": status, "reason": reason, "grader": GRADER}
                fingerprint = digest(evidence)
                cur.execute("""SELECT evidence_hash FROM forecast.settlements
                               WHERE entry_id=%s ORDER BY id DESC LIMIT 1""", (entry["id"],))
                previous = cur.fetchone()
                if previous and previous["evidence_hash"] == fingerprint:
                    continue
                cur.execute("""INSERT INTO forecast.settlements
                               (entry_id,grader,status,reason,evidence,evidence_hash)
                               VALUES (%s,%s,%s,%s,%s,%s)""",
                            (entry["id"], GRADER, status, reason, Json(evidence, dumps=encoded), fingerprint))
                count += 1
        return count
    finally:
        if own:
            conn.close()


def report(conn=None):
    """Read-only complete record, including ungraded forecasts; no success filter."""
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT e.*, COALESCE(s.status, 'pending') AS status,
                                  COALESCE(s.reason, 'not_yet_checked') AS reason,
                                  s.observed_at, s.evidence, s.grader,
                                  COALESCE((SELECT jsonb_agg(to_jsonb(h) ORDER BY h.id)
                                    FROM forecast.settlements h WHERE h.entry_id=e.id),
                                    '[]'::jsonb) AS outcome_history
                           FROM forecast.latest_forecasts e
                           LEFT JOIN LATERAL (SELECT * FROM forecast.settlements
                             WHERE entry_id=e.id ORDER BY id DESC LIMIT 1) s ON true
                           WHERE e.policy=%s ORDER BY e.kickoff DESC, e.fixture_id, e.event_key""", (POLICY,))
            rows = [dict(r) for r in cur.fetchall()]
        counts = {s: sum(r["status"] == s for r in rows)
                  for s in ("pending", "hit", "miss", "void", "unavailable", "excluded")}
        return {"policy": POLICY, "population": "exported DC goal events, not all model outputs",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "period": {"first_capture": min((r["recorded_at"] for r in rows), default=None),
                           "last_capture": max((r["recorded_at"] for r in rows), default=None)},
                "counts": counts, "records": rows}
    finally:
        if own:
            conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "settle", "report"))
    args = parser.parse_args()
    if args.action == "init":
        init_schema()
    elif args.action == "settle":
        print(f"Appended {settle()} settlement observations")
    else:
        print(json.dumps(report(), default=str, indent=2))


if __name__ == "__main__":
    main()
