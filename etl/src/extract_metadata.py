from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import os
import etl.src.config as config
from etl.src.logger import get_logger
import requests
import psycopg2
import yaml
import pprint
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
import psycopg2.extensions
import argparse
from etl.src.schema_checks import ensure_metadata_bootstrap_ready

logger = get_logger(__name__)
"""
This file is to extract the metadata of leagues, seasons and teams from the API
It connects to the database and upserts the metadata into the database
"""
# Database connection parameters loaded from config.py.
# Expected keys: dbname, user, password, host, port.
# Example:
# DB_CONFIG = {
#     "dbname": "your_db",
#     "user": "your_user",
#     "password": "your_password",
#     "host": "localhost",
#     "port": 5432,
# }
DB_CONFIG = config.DB_CONFIG

# Cutoff year for processing seasons.
# Seasons with starting year before this value are skipped.
# Default set in config.py;
MIN_SEASON_TRACKER = config.MIN_SEASON_TRACKER

# Path to the metadata YAML file from config
METADATA_FILE = config.METADATA_FILE

# Set up the database connection
def get_db_connection() -> psycopg2.extensions.connection:
    """
    Establish and return a new psycopg2 database connection
    using DB_CONFIG settings.
    """
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def process_team(
    cur: psycopg2.extensions.cursor,
    league_name: str,
    api_team_id: int,
    team_name: str,
    team_code: str,
    country_id: int
) -> None:
    """
    Upsert a single team into the dim_teams table.
    Skips if api_team_id or team_name is missing.
    """
    if not api_team_id or not team_name:
        logger.warning(f"Skipping team with missing id or name in league '{league_name}'")
        return
    upsert_team(cur, api_team_id, team_name, team_code, country_id)

def process_season(
    cur: psycopg2.extensions.cursor,
    league_name: str,
    api_league_id: int,
    league_id: int,
    season: Dict[str, Any],
    country_id: int
) -> None:
    """
    Process one season for a league:
    - Filters out seasons before MIN_SEASON_TRACKER or with invalid year.
    - Constructs season_label, upserts into dim_league_seasons.
    - Fetches teams for that season and processes each.
    """
    raw_year = season.get("year")
    try:
        season_year = int(raw_year)
    except (TypeError, ValueError):
        logger.warning(f"Invalid season year '{raw_year}' for league '{league_name}', skipping this season")
        return
    if season_year < MIN_SEASON_TRACKER:
        logger.info(f"Skipping season '{season_year}' for league '{league_name}'")
        return

    season_label = f"{season_year}/{str(season_year+1)[-2:]}"
    start_date = season.get("start")
    end_date = season.get("end")
    upsert_league_season(cur, league_id, season_year, season_label, start_date, end_date)

    teams = fetch_teams("teams", params={"league": api_league_id, "season": season_year})
    if not teams:
        logger.warning(f"No teams found for league '{league_name}' in season {season_year}")
        return
    for team in teams:
        process_team(cur, league_name, team.get("id"), team.get("name"), team.get("code"), country_id)

def process_league(
    cur: psycopg2.extensions.cursor,
    country: str,
    country_id: int,
    league_data: Dict[str, Any],
    missing_league_ids: List[Dict[str, Any]]
) -> None:
    """
    Process one league for a country:
    - Fetches league details from API.
    - Upserts league metadata.
    - Iterates through seasons and processes each season.
    - Records missing leagues.
    """
    league_name = league_data["name"]
    league_info_list = fetch_league_info("leagues", params={"country": country, "name": league_name})
    if not league_info_list:
        logger.warning(f"No league info found for '{league_name}' in country '{country}'")
        missing_league_ids.append({"country": country, "league": league_name})
        return
    league_info = league_info_list[0]
    api_league_id = league_info.get("league", {}).get("id")
    league_id = upsert_league(cur, country_id, league_name, api_league_id)
    league_seasons = league_info.get("seasons", [])
    if not league_seasons:
        logger.warning(f"No seasons found for league '{league_name}' in country '{country}'")
        return
    for season in league_seasons:
        process_season(cur, league_name, api_league_id, league_id, season, country_id)

def process_country(
    cur: psycopg2.extensions.cursor,
    country_data: Dict[str, Any],
    missing_league_ids: List[Dict[str, Any]]
) -> None:
    """
    Process one country entry from metadata:
    - Upserts country record.
    - Iterates through each league and processes it.
    """
    country = country_data["country"]
    country_id = upsert_country(cur, country)
    for league_data in country_data["leagues"]:
        process_league(cur, country, country_id, league_data, missing_league_ids)


# Load API configuration from config.py
API_KEY = config.API['key']
API_HOST = config.API['host']
API_URL = config.API['url']
HEADERS = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': API_HOST
}


def load_yaml(file_path: str) -> Dict[str, Any]:
    """
    Read and parse a YAML file, returning its contents as a dict.
    Raises on file not found or parse errors.
    """
    try:
        with open(file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
            logger.info(f"YAML file '{file_path}' has been loaded successfully.")
            return yaml_data
    except FileNotFoundError as e:
        logger.error(f"YAML file '{file_path}' not found: {e}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file '{file_path}': {e}")
        raise

# endpoint params => League
# endpoint is the leagues api endpoint
# params is a dictionary of {"country": "String", "name": "String"}
# country is the name of the country where the league is located
# name  is the name of the league
# League is an objeck with keys, league, country and seasons

    

#fetch the data of a league from a given country and league name


# Unified rate-limited GET with retry/backoff and 429 handling
def _rate_limited_get(
    endpoint: str,
    params: Dict[str, Any],
    max_retries: int = 5
) -> List[Dict[str, Any]]:
    """
    Perform an HTTP GET with automatic rate-limit handling:
    - Honors daily and per-minute limit headers.
    - Retries on HTTP 429 with Retry-After header.
    - Uses exponential backoff on transient errors.
    Returns list of JSON 'response' elements or empty list on failure.
    """
    backoff = 1
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(f"{API_URL}{endpoint}", headers=HEADERS, params=params, timeout=10)
            # Enforce limits
            daily_rem = int(response.headers.get('x-ratelimit-requests-remaining', 1))
            min_rem   = int(response.headers.get('x-ratelimit-remaining', 1))
            if daily_rem < 1:
                # Sleep until next UTC day
                utc_now = datetime.now(ZoneInfo("UTC"))
                next_day = datetime.combine(utc_now.date() + timedelta(days=1), datetime.min.time(), tzinfo=ZoneInfo("UTC"))
                sleep_sec = (next_day - utc_now).total_seconds()
                logger.info(f"Daily rate limit reached, sleeping {sleep_sec}s")
                time.sleep(sleep_sec)
                continue
            if min_rem < 1:
                logger.info("Minute rate limit reached, sleeping 60s")
                time.sleep(60)
                continue

            # Check for HTTP errors
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', backoff))
                logger.info(f"HTTP 429 received, sleeping {retry_after}s")
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json().get("response", [])
        except (HTTPError, ConnectionError, Timeout) as e:
            logger.warning(f"Transient error on attempt {attempt}/{max_retries}: {e}. Retrying in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except RequestException as e:
            logger.error(f"Non-retryable request error: {e}")
            break
    logger.error(f"Failed to fetch {endpoint} after {max_retries} attempts with params: {params}")
    return []


def fetch_league_info(
    endpoint: str,
    params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Retrieve league information from the API.
    Returns a list of league entries or an empty list.
    """
    return _rate_limited_get(endpoint, params)


# endpoint params => ListOfTeams
# fetches a list of teams in a given league and season 
# endpoint is the teams api endpoint
# params is a dictionary of {"league": INT, "season": INT}
# league is the league_id
# season is the year the season started
# ListOfTeams is a list of teams, each team is an object with keys -  team_name, api_team_id and team_code


def fetch_teams(
    endpoint: str,
    params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Retrieve and transform team list from the API for a given league and season.
    Returns a list of dicts with 'name', 'id', and 'code'.
    """
    raw = _rate_limited_get(endpoint, params)
    # Transform into team dicts
    return [
        {"name": t.get("team", {}).get("name"), "id": t.get("team", {}).get("id"), "code": t.get("team", {}).get("code")}
        for t in raw if t.get("team")
    ]


#  cur, country_name => country_id
# Upsert functions to insert or update country in the database and return the country_id

def upsert_country(
    cur: psycopg2.extensions.cursor,
    country_name: str
) -> int:
    """
    Insert or update a country in dim_countries.
    Returns the surrogate country_id.
    """
    sql = """
    INSERT INTO dim.dim_countries (country_name)
    VALUES (%s)
    ON CONFLICT (country_name) DO NOTHING
    RETURNING country_id;
    """
    try:
        cur.execute(sql, (country_name,))
        result = cur.fetchone()
        if result:
            return result[0] # return the country id
        cur.execute("SELECT country_id FROM dim.dim_countries WHERE country_name = %s", (country_name,))
        logger.info(f"Upserted country: {country_name}")
        return cur.fetchone()[0]  # Return the country_id
    except Exception as e:
        logger.error(f"Error upserting country '{country_name}': {e}")
        raise
    
# cur, country_id, league_name, api_league_id => league_id
# country_id is the id of the country in the database
# league_name is the name of the league
# api_league_id is the id of the league from the API   
## Upsert function to insert or update league in the database and return the league_id

def upsert_league(
    cur: psycopg2.extensions.cursor,
    country_id: int,
    league_name: str,
    api_league_id: int
) -> int:
    """
    Insert or update a league in dim_leagues.
    Returns the surrogate league_id.
    """
    sql = """
    INSERT INTO dim.dim_leagues (country_id, league_name, api_league_id)
    VALUES (%s, %s, %s)
    ON CONFLICT (api_league_id) DO UPDATE 
    SET 
        league_name = EXCLUDED.league_name,
        country_id = EXCLUDED.country_id,
        updated_at = NOW()
    WHERE
        dim.dim_leagues.league_name != EXCLUDED.league_name
        OR dim.dim_leagues.country_id != EXCLUDED.country_id
    RETURNING league_id;
    """ 
    try:
        cur.execute(sql, (country_id, league_name, api_league_id))
        result = cur.fetchone()
        if result:
            return result[0]  # return the league_id
        cur.execute("SELECT league_id FROM dim.dim_leagues WHERE api_league_id = %s", (api_league_id,))
        logger.info(f"Upserted league: {league_name} in country_id: {country_id}")
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Error upserting league '{league_name}' in country_id '{country_id}': {e}")
        raise
        
# cur, league_id, season_label, start_year, end_year => league_season_id
# league_id is the id of the league in the database

def upsert_league_season(
    cur: psycopg2.extensions.cursor,
    league_id: int,
    season: int,
    season_label: str,
    start_date: Optional[str],
    end_date: Optional[str]
) -> int:
    """
    Insert or update a season for a league in dim_league_seasons.
    Returns the surrogate league_season_id.

    Args:
        cur: psycopg2 cursor.
        league_id: Surrogate league ID.
        season: Integer season year (e.g., 2023).
        season_label: String label (e.g., "2023/24").
        start_date: Optional season start date.
        end_date: Optional season end date.
    """
    sql = """
    INSERT INTO dim.dim_league_seasons (league_id, season, season_label, start_date, end_date)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (league_id, season) DO UPDATE 
    SET 
        season_label = EXCLUDED.season_label,
        start_date = EXCLUDED.start_date,
        end_date = EXCLUDED.end_date
    WHERE 
        dim.dim_league_seasons.season_label != EXCLUDED.season_label
        OR dim.dim_league_seasons.start_date != EXCLUDED.start_date
        OR dim.dim_league_seasons.end_date != EXCLUDED.end_date
    RETURNING league_season_id;
    """
    try:
        cur.execute(sql, (league_id, season, season_label, start_date, end_date))
        fetch = cur.fetchone()
        if fetch:
            return fetch[0]
       
        cur.execute(
            "SELECT league_season_id FROM dim.dim_league_seasons WHERE league_id = %s AND season = %s",
            (league_id, season)
        )
        return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Error upserting league season '{season_label}' for league_id '{league_id}': {e}")
        raise
        
# upsert team
def upsert_team(
    cur: psycopg2.extensions.cursor,
    api_team_id: int,
    team_name: str,
    team_code: Optional[str],
    country_id: int
) -> None:
    """
    Insert or update a team in dim_teams.
    Logs and skips if data is invalid.
    """
    sql = """
            INSERT INTO dim.dim_teams (api_team_id, team_name, team_code, country_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (api_team_id) DO UPDATE 
            SET team_name = EXCLUDED.team_name, 
            team_code = EXCLUDED.team_code,
            country_id = EXCLUDED.country_id,
            updated_at = NOW()
            WHERE 
                dim.dim_teams.team_name != EXCLUDED.team_name
                OR dim.dim_teams.team_code != EXCLUDED.team_code
                OR dim.dim_teams.country_id != EXCLUDED.country_id;
            """
    try:
        cur.execute(sql, (api_team_id, team_name, team_code, country_id))
        logger.info(f"Upserted team: {team_name} with id: {api_team_id}")
    except Exception as e:
        logger.error(f"Error upserting team '{team_name}' with id '{api_team_id}': {e}")
        raise


def _download_logos_after_metadata(limit: Optional[int] = None) -> None:
    """
    Populate logo_file_id fields (and venue links) after metadata upserts.
    Uses the existing update_logos module so asset storage behavior stays centralized.
    """
    from etl.src.update_logos import (
        download_country_flags,
        download_league_logos,
        download_team_logos_and_venues,
    )

    countries = download_country_flags(limit=limit)
    leagues = download_league_logos(limit=limit)
    teams = download_team_logos_and_venues(limit=limit)
    logger.info(
        "Logo extraction complete: countries=%s leagues=%s team_assets=%s",
        countries,
        leagues,
        teams,
    )


def extract_metadata(include_logos: bool = True, logo_limit: Optional[int] = None) -> None:
    """
    Main ETL entry point:
    - Loads metadata.yaml.
    - Processes countries, leagues, seasons, and teams.
    - Commits all changes or rolls back on error.
    - Logs any missing league entries.
    """
    #params: metadata is a json file of each country, league and season

    metadata = load_yaml(METADATA_FILE)
    if not metadata:
        logger.error("No metadata found in metadata.yaml")
        return
    else:
        logger.info("Loaded metadata from metadata.yaml")

    #list of country with and leagues with missing league id
    missing_league_ids = []

    conn = get_db_connection()
    cur = None
    try:
        cur = conn.cursor()
        ensure_metadata_bootstrap_ready(cur)
        logger.info("Schema check passed: metadata tables are ready.")

        try:
            for country_data in metadata["countries"]:
                process_country(cur, country_data, missing_league_ids)

            # Commit all changes to the database
            conn.commit()
            logger.info("Metadata extraction and upsert completed successfully.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during metadata extraction: {e}")
            raise
    finally:
        # Close the cursor and connection
        if cur is not None:
            cur.close()
        conn.close()
        logger.info("Database connection closed.")
    #Report any missing items all at once
    if missing_league_ids:
        logger.error("League missing IDs:\n" + pprint.pformat(missing_league_ids))

    if include_logos:
        logger.info("Starting logo extraction as part of metadata extraction.")
        _download_logos_after_metadata(limit=logo_limit)
    else:
        logger.info("Skipping logo extraction for metadata run.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract metadata into dim tables (optionally with logos).")
    parser.add_argument(
        "--skip-logos",
        action="store_true",
        help="Skip country/league/team logo download and venue enrichment after metadata upsert.",
    )
    parser.add_argument(
        "--logo-limit",
        type=int,
        default=None,
        help="Optional limit for logo downloads per entity type (useful for testing).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        extract_metadata(include_logos=not args.skip_logos, logo_limit=args.logo_limit)
    except KeyboardInterrupt:
        logger.warning("Extraction cancelled by user")
