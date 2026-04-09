from dotenv import load_dotenv
import os

load_dotenv(override=True)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": _env_int("DB_PORT", 5432),
}

API = {
    "key": os.getenv("API_KEY"),
    "host": os.getenv("API_HOST"),
    "url": os.getenv("API_URL"),
}

MIN_SEASON_TRACKER = _env_int("MIN_SEASON_TRACKER", 2020)

# File paths
BASE_DATA_DIR = os.getenv("BASE_DATA_DIR", os.path.join(os.getcwd(), "data"))
LOGS_PATH = os.getenv("LOGS_PATH", os.path.join(BASE_DATA_DIR, "logs"))
FIXTURES_PATH = os.getenv("FIXTURES_PATH", os.path.join(BASE_DATA_DIR, "fixtures"))
FIXTURES_UPDATE_DIR = os.getenv("FIXTURES_UPDATE_DIR", os.path.join(FIXTURES_PATH, "updates"))
CLEANED_DATA_DIR = os.getenv("CLEANED_DATA_DIR", os.path.join(BASE_DATA_DIR, "cleaned"))

for d in [LOGS_PATH, FIXTURES_PATH, FIXTURES_UPDATE_DIR, CLEANED_DATA_DIR]:
    if d:
        os.makedirs(d, exist_ok=True)

EXTRACT_METADATA_LOG = os.getenv("EXTRACT_METADATA_LOG", os.path.join(LOGS_PATH, "extract_metadata_logs.txt"))
EXTRACT_FIXTURES_LOG = os.getenv("EXTRACT_FIXTURES_LOG", os.path.join(LOGS_PATH, "extract_fixtures_logs.txt"))
TRANSFORM_FIXTURES_LOG = os.getenv("TRANSFORM_FIXTURES_LOG", os.path.join(LOGS_PATH, "transforming_fixtures_logs.txt"))
LOAD_TO_DB_LOG = os.getenv("LOAD_TO_DB_LOG", os.path.join(LOGS_PATH, "load_to_db_logs.txt"))
UPDATE_FIXTURES_LOG = os.getenv("UPDATE_FIXTURES_LOG", os.path.join(LOGS_PATH, "update_fixtures_log.txt"))
FIXTURE_UPDATES_JSON = os.getenv("FIXTURE_UPDATES_JSON", os.path.join(FIXTURES_PATH, "fixture_updates.json"))
LOAD_UPDATES_LOG = os.getenv("LOAD_UPDATES_LOG", os.path.join(LOGS_PATH, "load_updates_to_db_logs.txt"))
METADATA_FILE = os.getenv("METADATA_FILE", os.path.join(os.getcwd(), "data", "metadata.yaml"))
