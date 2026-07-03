"""
SportyBet (NG) pre-match odds scraper.

Pulls live decimal odds for the fixtures in the current consensus pool and
lands them to both S3 (raw JSON, pipeline-consistent) and Postgres
(`raw.sportybet_odds`, the join source for EV filtering).

Why this exists: the consensus engine's `_est_odds()` fabricates prices from
probability buckets, so there is no real edge/EV measurement. This module
supplies the real prices. See memory `sportybet-odds-coverage`.

Matching is the hard part — SportyBet uses its own event ids, renames leagues
(our "League One"/China is their "China League 1"), and spells teams
differently. We match on (country category + kickoff date + fuzzy team names),
never on league name.

Market-id map (verified July 3 2026):
    1  = 1X2            10 = Double Chance      18 = Match Over/Under
    19 = Home Team O/U  20 = Away Team O/U      29 = GG/NG (BTTS)

Usage:
    python -m engine.eval.scrape_sportybet                 # price today's pool
    python -m engine.eval.scrape_sportybet --pool path.json --no-s3
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import time
import unicodedata
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from engine.eval.db import get_conn

# ── SportyBet API ────────────────────────────────────────────────────────────
BASE = "https://www.sportybet.com/api/ng/factsCenter"
LIST_URL = BASE + "/pcUpcomingEvents?sportId=sr%3Asport%3A1&marketId=1&pageSize=100&pageNum={}"
EVENT_URL = BASE + "/event?eventId={}&productId=3"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Clientid": "web", "Accept": "application/json",
}
MAX_PAGES = 20
REQ_PAUSE = 0.3          # polite delay between event fetches
MATCH_THRESHOLD = 0.55   # min fuzzy team-match score to accept an event

# structural market keys we price
MARKET_ID = {"1": "1X2", "10": "DC", "18": "OU", "19": "HOME_OU", "20": "AWAY_OU", "29": "BTTS"}
DC_MAP = {"Home or Draw": "1X", "Home or Away": "12", "Draw or Away": "X2"}

# our pool country -> SportyBet category name (only where they differ)
COUNTRY_ALIAS = {"United States": "USA", "USA": "USA"}

STOP = {"fc","sc","sk","if","fk","afc","cf","ac","us","ss","club","de","the",
        "ii","b","bk","is","ff","sv","cd","aic","nk","ik","gais","kf","kfum",
        "il","fs","ca","ec","al"}


# ── http ─────────────────────────────────────────────────────────────────────

def _get(url: str, tries: int = 3) -> dict:
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


# ── team-name normalisation & matching ───────────────────────────────────────

def _norm(name: str) -> tuple[set[str], str]:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    toks = {t for t in s.split() if t not in STOP and len(t) > 1}
    return toks, s.strip()


def _sim(a: str, b: str) -> float:
    ta, sa = _norm(a)
    tb, sb = _norm(b)
    if not ta or not tb:
        return 0.0
    j = len(ta & tb) / len(ta | tb)
    if sa and sb and (sa in sb or sb in sa):
        j = max(j, 0.6)
    return j


# ── event enumeration & market extraction ────────────────────────────────────

def fetch_upcoming_events() -> list[dict]:
    """All upcoming SportyBet football events (id, teams, kickoff, country)."""
    events, page = [], 1
    while page <= MAX_PAGES:
        d = _get(LIST_URL.format(page))
        tours = d.get("data", {}).get("tournaments", [])
        if not tours:
            break
        for t in tours:
            for e in t["events"]:
                events.append({
                    "event_id": e["eventId"],
                    "home": e["homeTeamName"],
                    "away": e["awayTeamName"],
                    "kickoff_ms": e.get("estimateStartTime"),
                    "country": e.get("sport", {}).get("category", {}).get("name", ""),
                    "league": t["name"],
                })
        if page * 100 >= d["data"].get("totalNum", 0):
            break
        page += 1
        time.sleep(REQ_PAUSE)
    return events


def extract_markets(raw_markets: list[dict]) -> dict:
    """Structural odds map: {OU: {line: {Over,Under}}, 1X2: {...}, ...}."""
    out: dict = defaultdict(lambda: defaultdict(dict))
    flat: dict = {}
    for mk in raw_markets:
        key = MARKET_ID.get(str(mk.get("id")))
        if not key or mk.get("status") not in (0, None):
            continue
        line = None
        mo = re.search(r"total=([0-9.]+)", mk.get("specifier", ""))
        if mo:
            line = f"{float(mo.group(1)):g}"
        for o in mk.get("outcomes", []):
            if not o.get("isActive", 1):
                continue
            try:
                odds = float(o["odds"])
            except (KeyError, ValueError, TypeError):
                continue
            desc = o.get("desc", "").strip()
            if key in ("OU", "HOME_OU", "AWAY_OU"):
                side = desc.split()[0] if desc else ""   # "Over"/"Under"
                if line and side in ("Over", "Under"):
                    out[key][line][side] = odds
            elif key == "1X2":
                flat.setdefault("1X2", {})[desc] = odds   # Home/Draw/Away
            elif key == "DC":
                flat.setdefault("DC", {})[DC_MAP.get(desc, desc)] = odds
            elif key == "BTTS":
                flat.setdefault("BTTS", {})[desc] = odds  # Yes/No
    result = {k: {ln: dict(v) for ln, v in lines.items()} for k, lines in out.items()}
    result.update(flat)
    return result


# ── orchestration ────────────────────────────────────────────────────────────

def _match_events(fixtures: list[dict], events: list[dict]) -> dict:
    """fixture_id -> (event, score). Country + date narrowed, team-fuzzy ranked."""
    by_date: dict = defaultdict(list)
    for e in events:
        if e["kickoff_ms"]:
            dk = datetime.fromtimestamp(e["kickoff_ms"] / 1000, timezone.utc).strftime("%Y-%m-%d")
            by_date[dk].append(e)

    matched = {}
    for fx in fixtures:
        dk = str(fx["kickoff"])[:10]
        country = COUNTRY_ALIAS.get(fx.get("country", ""), fx.get("country", ""))
        cands = by_date.get(dk, [])
        if country:
            narrowed = [e for e in cands if e["country"].lower() == country.lower()]
            cands = narrowed or cands  # fall back to all-date if country tag missing
        best, best_s = None, 0.0
        for e in cands:
            s = 0.5 * _sim(fx["home"], e["home"]) + 0.5 * _sim(fx["away"], e["away"])
            if s > best_s:
                best_s, best = s, e
        if best and best_s >= MATCH_THRESHOLD:
            matched[fx["fixture_id"]] = (best, round(best_s, 3))
    return matched


def scrape_odds(fixtures: list[dict]) -> dict:
    """Return {fixture_id: {event_id, match_score, home, away, markets, captured_at}}."""
    print(f"Fetching SportyBet upcoming events...")
    events = fetch_upcoming_events()
    print(f"  {len(events)} events")
    matched = _match_events(fixtures, events)
    print(f"  matched {len(matched)}/{len(fixtures)} pool fixtures")

    now = datetime.now(timezone.utc).isoformat()
    odds = {}
    for i, (fid, (ev, score)) in enumerate(matched.items(), 1):
        try:
            d = _get(EVENT_URL.format(ev["event_id"]))
            mkts = extract_markets(d.get("data", {}).get("markets", []))
        except Exception as e:  # noqa: BLE001
            print(f"  ! event {ev['event_id']} fetch failed: {e}")
            continue
        odds[fid] = {
            "event_id": ev["event_id"], "match_score": score,
            "home": ev["home"], "away": ev["away"],
            "markets": mkts, "captured_at": now,
        }
        if i % 20 == 0:
            print(f"  priced {i}/{len(matched)}")
        time.sleep(REQ_PAUSE)
    return odds


# ── persistence ──────────────────────────────────────────────────────────────

def _flatten_rows(fid: int, entry: dict) -> list[tuple]:
    """Explode a fixture's odds into (fixture, event, market, line, selection, odds) rows."""
    rows, cap = [], entry["captured_at"]
    ev = entry["event_id"]
    ms = entry["match_score"]
    for mkey, val in entry["markets"].items():
        if mkey in ("OU", "HOME_OU", "AWAY_OU"):
            for line, sides in val.items():
                for side, o in sides.items():
                    rows.append((fid, ev, mkey, line, side, o, ms, cap))
        else:  # 1X2 / DC / BTTS
            for sel, o in val.items():
                rows.append((fid, ev, mkey, None, sel, o, ms, cap))
    return rows


def persist_pg(odds: dict) -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.sportybet_odds (
                fixture_id     BIGINT      NOT NULL,
                sporty_event_id TEXT       NOT NULL,
                market         TEXT        NOT NULL,
                line           TEXT,
                selection      TEXT        NOT NULL,
                decimal_odds   NUMERIC     NOT NULL,
                match_score    NUMERIC,
                captured_at    TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (fixture_id, market, line, selection)
            )
        """)
        rows = [r for fid, entry in odds.items() for r in _flatten_rows(fid, entry)]
        for r in rows:
            cur.execute("""
                INSERT INTO raw.sportybet_odds
                    (fixture_id, sporty_event_id, market, line, selection,
                     decimal_odds, match_score, captured_at)
                VALUES (%s,%s,%s,COALESCE(%s,''),%s,%s,%s,%s)
                ON CONFLICT (fixture_id, market, line, selection) DO UPDATE SET
                    decimal_odds = EXCLUDED.decimal_odds,
                    sporty_event_id = EXCLUDED.sporty_event_id,
                    match_score = EXCLUDED.match_score,
                    captured_at = EXCLUDED.captured_at
            """, r)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def persist_s3(odds: dict) -> str | None:
    """Best-effort raw landing to S3; no-op with a warning if AWS is unavailable."""
    try:
        import boto3
        from etl.src.s3_landing import S3_BUCKET
    except Exception:  # noqa: BLE001
        print("  (skipping S3 — boto3/etl not importable)")
        return None
    ds = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"odds/sportybet/{ds}/odds_{datetime.now(timezone.utc).strftime('%H%M%S')}.json"
    try:
        boto3.client("s3").put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=json.dumps(odds, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"  landed raw odds → s3://{S3_BUCKET}/{key}")
        return f"s3://{S3_BUCKET}/{key}"
    except Exception as e:  # noqa: BLE001
        print(f"  (skipping S3 — {type(e).__name__}: {e})")
        return None


# ── pool loading + odds lookup helper (used by consensus) ─────────────────────

def _latest_pool_path() -> Path:
    files = sorted(glob.glob(str(Path(__file__).parent / "consensus_slips_*.json")))
    if not files:
        raise FileNotFoundError("no consensus_slips_*.json — run consensus_slips first")
    return Path(files[-1])


def fixtures_from_db(days: int = 7) -> list[dict]:
    """Upcoming not-started fixtures straight from Postgres (no consensus JSON
    needed). Country is left blank — the matcher then ranks on date + team name.
    Used by the Airflow DAG, which runs independently of the local engine run.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT fixture_id, home_team_name, away_team_name, kickoff_utc
            FROM raw.raw_fixtures
            WHERE fixture_status = 'NS'
              AND kickoff_utc >= now()
              AND kickoff_utc <  now() + make_interval(days => %s)
            ORDER BY kickoff_utc
            """,
            (days,),
        )
        return [{"fixture_id": int(r[0]), "home": r[1], "away": r[2],
                 "kickoff": str(r[3]), "country": ""} for r in cur.fetchall()]
    finally:
        conn.close()


def fixtures_from_pool(pool_path: Path) -> list[dict]:
    data = json.load(open(pool_path))
    seen, fixtures = set(), []
    for p in data.get("pool", []):
        fid = p["fixture_id"]
        if fid in seen:
            continue
        seen.add(fid)
        fixtures.append({"fixture_id": fid, "home": p["home"], "away": p["away"],
                         "kickoff": p["kickoff"], "country": p.get("country", "")})
    return fixtures


def odds_for_pick(markets: dict, sporty_market: str, selection: str,
                  line, home: str, away: str) -> float | None:
    """Look up the decimal odds for one consensus pick from a fixture's markets."""
    if not markets:
        return None
    ln = f"{float(line):g}" if line is not None else None
    if sporty_market == "Over/Under":
        side = "Over" if "Over" in selection else "Under"
        return markets.get("OU", {}).get(ln, {}).get(side)
    if sporty_market == "Home Team Over/Under":
        return markets.get("HOME_OU", {}).get(ln, {}).get("Over")
    if sporty_market == "Away Team Over/Under":
        return markets.get("AWAY_OU", {}).get(ln, {}).get("Over")
    if sporty_market == "1X2":
        side = "Home" if home and home.lower() in selection.lower() else "Away"
        return markets.get("1X2", {}).get(side)
    if sporty_market == "Double Chance":
        return markets.get("DC", {}).get(selection.strip())
    if sporty_market == "Both Teams to Score":
        return markets.get("BTTS", {}).get("Yes")
    return None


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape SportyBet pre-match odds for the pool")
    ap.add_argument("--from-db", action="store_true",
                    help="load upcoming fixtures from Postgres instead of a pool JSON")
    ap.add_argument("--days", type=int, default=7, help="days ahead when using --from-db")
    ap.add_argument("--pool", type=str, default=None, help="consensus_slips_*.json (default: latest)")
    ap.add_argument("--no-s3", action="store_true", help="skip S3 landing")
    ap.add_argument("--no-pg", action="store_true", help="skip Postgres upsert")
    args = ap.parse_args()

    if args.from_db:
        print(f"Source: raw.raw_fixtures (NS, next {args.days}d)")
        fixtures = fixtures_from_db(days=args.days)
    else:
        pool_path = Path(args.pool) if args.pool else _latest_pool_path()
        print(f"Pool: {pool_path.name}")
        fixtures = fixtures_from_pool(pool_path)
    print(f"  {len(fixtures)} unique fixtures to price")

    odds = scrape_odds(fixtures)
    print(f"\nPriced {len(odds)} fixtures.")

    if not args.no_pg:
        n = persist_pg(odds)
        print(f"  upserted {n} odds rows into raw.sportybet_odds")
    if not args.no_s3:
        persist_s3(odds)


if __name__ == "__main__":
    main()
