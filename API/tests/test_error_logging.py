"""5xx responses log their cause (for staging debugging) without SQL or parameters."""
import logging
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.exc import ProgrammingError
from API.app import app

@app.get("/__test_503")
def boom():
    try:
        raise ProgrammingError("CREATE SCHEMA app", {"email": "member@example.com"}, Exception("permission denied for schema app\nLINE 1: ..."))
    except ProgrammingError as exc:
        raise HTTPException(503, "Database unavailable") from exc

def test_logs_cause_without_params(caplog):
    with caplog.at_level(logging.ERROR, logger="account_api"):
        r = TestClient(app).get("/__test_503")
    assert r.status_code == 503 and r.json() == {"detail": "Database unavailable"}
    text = caplog.text
    assert "GET /__test_503 -> 503 Database unavailable" in text
    assert "permission denied for schema app" in text
    assert "member@example.com" not in text and "LINE 1" not in text

def test_4xx_not_logged(caplog):
    with caplog.at_level(logging.ERROR, logger="account_api"):
        r = TestClient(app).get("/me")
    assert r.status_code in (401, 403) and "account_api" not in caplog.text
