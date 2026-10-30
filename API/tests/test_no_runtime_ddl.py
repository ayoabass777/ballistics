"""The account API never changes the schema at request time.

On staging it connects as account_api, which has no CREATE or ALTER rights
(deploy/grants.sql), so any DDL in a request path fails with permission
denied and the endpoint returns 503. Locally it runs as the database owner,
where the same code works, so only this check catches it. Schema changes
belong in API/sql/ (account bootstrap) or db/migrations/.
"""
import re
from pathlib import Path

API_DIR = Path(__file__).resolve().parents[1]
DDL = re.compile(
    r"\b(CREATE|ALTER|DROP|TRUNCATE)\s+(OR\s+REPLACE\s+)?(UNIQUE\s+)?"
    r"(TABLE|SCHEMA|INDEX|EXTENSION|TYPE|FUNCTION|VIEW|TRIGGER|SEQUENCE|ROLE)\b",
    re.IGNORECASE,
)


def test_api_code_runs_no_ddl():
    offenders = []
    for path in sorted(API_DIR.rglob("*.py")):
        if "tests" in path.relative_to(API_DIR).parts:
            continue
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if DDL.search(line):
                offenders.append(f"{path.relative_to(API_DIR)}:{number}: {line.strip()}")
    assert not offenders, "DDL in API code (move it to API/sql/ or db/migrations/):\n" + "\n".join(offenders)
