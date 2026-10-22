from pathlib import Path
import sys

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from API.app import app


client = TestClient(app)


def test_healthz_ok():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_root_returns_list():
    response = client.get("/")
    assert response.status_code in {200, 503}
    if response.status_code == 200:
        assert isinstance(response.json(), list)
    else:
        assert response.json()["detail"] == "Database unavailable"


def test_me_requires_auth():
    response = client.get("/me")
    assert response.status_code == 401
    assert response.json()["detail"] == "Authentication required"
