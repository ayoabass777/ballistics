"""
Crests and flags for countries, leagues and teams, stored in S3.

Images come from the API-Sports media CDN, which serves them at fixed URLs by
provider id and does not count against the API request quota:

    https://media.api-sports.io/football/teams/<api_team_id>.png
    https://media.api-sports.io/football/leagues/<api_league_id>.png
    country flags: one /countries API call maps name -> flag URL

Each image is written to

    s3://$ASSETS_BUCKET/crests/<countries|leagues|teams>/<uuid>

with no file extension; Content-Type is set on upload. The UUID goes into the
table's logo_file_id. Public URL: $CREST_BASE_URL/crests/<kind>/<uuid>
(engine/api/crests.py). A new image always gets a new UUID, so objects are
cached as immutable.

Only rows without a logo_file_id are fetched. --refresh-venues additionally
calls /teams per team to refresh venue links (uses API quota).

Run:
    python -m etl.src.update_logos [--countries] [--leagues] [--teams] [--limit N]
    python -m etl.src.update_logos --refresh-venues [--limit N]
    python -m etl.src.update_logos --setup-bucket     # create bucket, public read on crests/*
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
import requests

import etl.src.config as config
from etl.src.logger import get_logger

logger = get_logger(__name__)

API_URL = config.API["url"]
HEADERS = config.API_HEADERS  # API-Sports direct (x-apisports-key)
MEDIA_URL = os.getenv("API_MEDIA_URL", "https://media.api-sports.io/football")

ASSETS_BUCKET = os.getenv("ASSETS_BUCKET", "ballistics-assets")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ca-central-1")
CREST_PREFIX = "crests"
KINDS = ("countries", "leagues", "teams")
CACHE_CONTROL = "public, max-age=31536000, immutable"
WORKERS = int(os.getenv("CREST_WORKERS", "8"))

_s3 = None
_http = requests.Session()


def say(msg: str, *args) -> None:
    """Log to the ETL log file and print, so shell runs show progress."""
    logger.info(msg, *args)
    print(msg % args if args else msg, flush=True)


def crest_key(kind: str, file_id: str) -> str:
    if kind not in KINDS:
        raise ValueError(f"unknown crest kind {kind!r}")
    return f"{CREST_PREFIX}/{kind}/{file_id}"


def _client():
    global _s3
    if _s3 is None:
        import boto3

        _s3 = boto3.client("s3", region_name=AWS_REGION)
    return _s3


def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(**config.DB_CONFIG)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def content_type_for(name: str, header: Optional[str] = None) -> str:
    """Image media type from the response header, else the file name, else PNG."""
    if header:
        media = header.split(";")[0].strip().lower()
        if media.startswith("image/"):
            return media
    return mimetypes.guess_type(name)[0] or "image/png"


def put_crest(kind: str, body: bytes, content_type: str, file_id: Optional[str] = None) -> str:
    """Write one image to S3; return its file id (a new UUID unless given)."""
    file_id = file_id or str(uuid.uuid4())
    _client().put_object(
        Bucket=ASSETS_BUCKET,
        Key=crest_key(kind, file_id),
        Body=body,
        ContentType=content_type,
        CacheControl=CACHE_CONTROL,
    )
    return file_id


def _store(url: str, kind: str) -> Optional[str]:
    """Download url and store it as a crest; return the file id or None (404, error)."""
    try:
        resp = _http.get(url, timeout=30)
        if resp.status_code == 404:
            logger.warning("No image at %s", url)
            return None
        resp.raise_for_status()
        if not resp.content:
            return None
    except requests.RequestException as exc:
        logger.error("Failed to download %s: %s", url, exc)
        return None
    try:
        return put_crest(kind, resp.content, content_type_for(url, resp.headers.get("Content-Type")))
    except Exception as exc:  # botocore errors; keep the batch going
        logger.error("Failed to store %s in s3://%s: %s", url, ASSETS_BUCKET, exc)
        return None


def ensure_public_bucket() -> None:
    """Create ASSETS_BUCKET if needed and allow anonymous GET on crests/* only.

    Object ACLs stay blocked; access is granted by bucket policy. Fails if the
    account-level Block Public Access setting forbids public policies.
    """
    s3 = _client()
    try:
        s3.head_bucket(Bucket=ASSETS_BUCKET)
        say("bucket %s exists", ASSETS_BUCKET)
    except Exception:
        kwargs = {"Bucket": ASSETS_BUCKET}
        if AWS_REGION != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": AWS_REGION}
        s3.create_bucket(**kwargs)
        say("created bucket %s in %s", ASSETS_BUCKET, AWS_REGION)
    s3.put_public_access_block(
        Bucket=ASSETS_BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": False,
            "RestrictPublicBuckets": False,
        },
    )
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadCrests",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{ASSETS_BUCKET}/{CREST_PREFIX}/*",
        }],
    }
    s3.put_bucket_policy(Bucket=ASSETS_BUCKET, Policy=json.dumps(policy))
    say("public read enabled on s3://%s/%s/*", ASSETS_BUCKET, CREST_PREFIX)


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

def team_logo_url(api_team_id: int) -> str:
    return f"{MEDIA_URL}/teams/{api_team_id}.png"


def league_logo_url(api_league_id: int) -> str:
    return f"{MEDIA_URL}/leagues/{api_league_id}.png"


def _api_get(endpoint: str, params: dict) -> list:
    url = f"{API_URL}/{endpoint}"
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json().get("response", [])
    except requests.RequestException as exc:
        logger.error("API request failed for %s with params %s: %s", endpoint, params, exc)
        raise


def country_flag_urls() -> Dict[str, str]:
    """{lowercased country name: flag url} from one /countries call."""
    return {
        (c.get("name") or "").lower(): c["flag"]
        for c in _api_get("countries", {})
        if c.get("flag")
    }


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _fill(kind: str, table: str, id_col: str, jobs: List[Tuple[int, str]]) -> int:
    """Download + store each (row id, url) in parallel, then write logo_file_id."""
    if not jobs:
        say("%s: nothing to fetch", kind)
        return 0
    say("%s: fetching %d", kind, len(jobs))
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        file_ids = list(pool.map(lambda j: _store(j[1], kind), jobs))
    done = [(fid, rid) for (rid, _), fid in zip(jobs, file_ids) if fid]
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(
                f"UPDATE {table} SET logo_file_id = %s, updated_at = NOW() WHERE {id_col} = %s",
                done,
            )
    finally:
        conn.close()
    say("%s: stored %d, no image %d", kind, len(done), len(jobs) - len(done))
    return len(done)


def _missing(sql: str, limit: Optional[int]) -> List[tuple]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()
    return rows[: limit or None]


def download_country_flags(limit: Optional[int] = None) -> int:
    rows = _missing(
        "SELECT country_id, country_name FROM dim.dim_countries WHERE logo_file_id IS NULL", limit)
    if not rows:
        return _fill("countries", "dim.dim_countries", "country_id", [])
    flags = country_flag_urls()
    jobs = [(cid, flags[name.lower()]) for cid, name in rows if name and name.lower() in flags]
    unmatched = len(rows) - len(jobs)
    if unmatched:
        say("countries: %d without a flag in /countries", unmatched)
    return _fill("countries", "dim.dim_countries", "country_id", jobs)


def download_league_logos(limit: Optional[int] = None) -> int:
    rows = _missing(
        "SELECT league_id, api_league_id FROM dim.dim_leagues "
        "WHERE logo_file_id IS NULL AND api_league_id IS NOT NULL", limit)
    return _fill("leagues", "dim.dim_leagues", "league_id",
                 [(lid, league_logo_url(api)) for lid, api in rows])


def download_team_logos(limit: Optional[int] = None) -> int:
    rows = _missing(
        "SELECT team_id, api_team_id FROM dim.dim_teams "
        "WHERE logo_file_id IS NULL AND api_team_id IS NOT NULL", limit)
    return _fill("teams", "dim.dim_teams", "team_id",
                 [(tid, team_logo_url(api)) for tid, api in rows])


def upsert_venue(cur, country_id: int, venue_data: Dict) -> Optional[int]:
    api_venue_id = venue_data.get("id")
    if not api_venue_id:
        return None
    cur.execute(
        """
        INSERT INTO dim.dim_venue (api_venue_id, venue_name, venue_capacity, venue_city, country_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (api_venue_id) DO UPDATE
        SET venue_name = EXCLUDED.venue_name,
            venue_capacity = EXCLUDED.venue_capacity,
            venue_city = EXCLUDED.venue_city,
            country_id = EXCLUDED.country_id,
            updated_at = NOW()
        RETURNING venue_id
        """,
        (api_venue_id, venue_data.get("name"), venue_data.get("capacity"),
         venue_data.get("city"), country_id),
    )
    return cur.fetchone()[0]


def refresh_venues(limit: Optional[int] = None) -> int:
    """One /teams call per team (API quota) to refresh venue links."""
    rows = _missing("SELECT team_id, country_id, api_team_id FROM dim.dim_teams", limit)
    linked = 0
    conn = get_db_connection()
    try:
        for team_id, country_id, api_team_id in rows:
            results = _api_get("teams", {"id": api_team_id})
            venue = (results[0].get("venue") or {}) if results else {}
            with conn, conn.cursor() as cur:
                venue_id = upsert_venue(cur, country_id, venue) if venue else None
                if venue_id:
                    cur.execute("UPDATE dim.dim_teams SET venue_id = %s WHERE team_id = %s",
                                (venue_id, team_id))
                    linked += 1
    finally:
        conn.close()
    say("venues: linked %d of %d teams", linked, len(rows))
    return linked


# Kept for etl.src.extract_metadata, which imports this name.
def download_team_logos_and_venues(limit: Optional[int] = None) -> int:
    return download_team_logos(limit=limit)


def main():
    parser = argparse.ArgumentParser(description="Store crests and flags in S3.")
    parser.add_argument("--countries", action="store_true", help="Country flags.")
    parser.add_argument("--leagues", action="store_true", help="League logos.")
    parser.add_argument("--teams", action="store_true", help="Team crests.")
    parser.add_argument("--limit", type=int, help="Max items to fetch per category.")
    parser.add_argument("--refresh-venues", action="store_true",
                        help="Refresh team venue links (one /teams API call per team).")
    parser.add_argument("--setup-bucket", action="store_true",
                        help="Create the bucket and public read on crests/*, then exit.")
    args = parser.parse_args()

    if args.setup_bucket:
        ensure_public_bucket()
        return
    if args.refresh_venues:
        refresh_venues(limit=args.limit)
        return

    run_all = not (args.countries or args.leagues or args.teams)
    if args.countries or run_all:
        download_country_flags(limit=args.limit)
    if args.leagues or run_all:
        download_league_logos(limit=args.limit)
    if args.teams or run_all:
        download_team_logos(limit=args.limit)


if __name__ == "__main__":
    main()
