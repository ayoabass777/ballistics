from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from API.beta_invites import claim_code, code_hash
from API.app import app


class FakeDB:
    def __init__(self, row):
        self.row = row
        self.updated = False
        self.committed = False

    def execute(self, statement, params):
        if str(statement).lstrip().startswith("SELECT"):
            return self
        self.updated = True
        return self

    def mappings(self):
        return self

    def first(self):
        return self.row

    def commit(self):
        self.committed = True


def test_code_is_keyed_and_invalid_claim_does_not_invite(monkeypatch):
    monkeypatch.setenv("BETA_INVITE_PEPPER", "x" * 32)
    assert code_hash("ABCDEFGHJKLMNPQRSTUV") != "ABCDEFGHJKLMNPQRSTUV"
    db = FakeDB(None)
    with patch("API.beta_invites.httpx.post") as send:
        with pytest.raises(HTTPException) as exc:
            claim_code(db, "ABCDEFGHJKLMNPQRSTUV", "member@example.com")
    assert exc.value.status_code == 400
    send.assert_not_called()


def test_claim_endpoint_requires_server_key(monkeypatch):
    monkeypatch.setenv("BETA_INVITE_API_KEY", "server-only-key")
    response = TestClient(app).post("/beta/invite/claim", json={
        "code": "ABCDEFGHJKLMNPQRSTUV", "email": "member@example.com"})
    assert response.status_code == 401


def test_email_bound_single_use_claim_sends_invitation(monkeypatch):
    monkeypatch.setenv("BETA_INVITE_PEPPER", "x" * 32)
    monkeypatch.setenv("CLERK_SECRET_KEY", "test-secret")
    row = {"email": "member@example.com", "expires_at": datetime.now(timezone.utc) + timedelta(days=1),
           "claimed_at": None}
    db = FakeDB(row)
    with patch("API.beta_invites.httpx.post") as send:
        send.return_value.json.return_value = {"id": "inv_123"}
        with pytest.raises(HTTPException):
            claim_code(db, "ABCDEFGHJKLMNPQRSTUV", "other@example.com")
        send.assert_not_called()
        claim_code(db, "ABCDEFGHJKLMNPQRSTUV", "member@example.com")
        assert send.call_count == 1
        payload = send.call_args.kwargs["json"]
        assert payload["email_address"] == "member@example.com"
        assert payload["notify"] is True
    assert db.updated and db.committed
