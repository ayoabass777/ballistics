"""Beta usage events (D164): allow-listed, account-scoped, idempotent by event_id."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator
from sqlalchemy import text

TUTORIALS = {"discover", "filters", "match", "saved", "settings", "record"}
ROUTE = re.compile(r"^/[a-z0-9\-/\[\]]{0,99}$")


def _enum(*values):
    return lambda v: isinstance(v, str) and v in values


def _count(v):
    return isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= 200


def _fixture(v):
    return isinstance(v, int) and not isinstance(v, bool) and v > 0


def _flag(v):
    return isinstance(v, bool)


# Every allowed event and the only properties it may carry. Anything else is rejected.
EVENTS: dict[str, dict[str, Any]] = {
    "session_started": {"entry_route": lambda v: isinstance(v, str) and bool(ROUTE.match(v)), "device": _enum("mobile", "desktop")},
    "onboarding_finished": {"outcome": _enum("completed", "skipped"), "league_count": _count},
    "discover_viewed": {"page": _enum("discover", "matches", "all"), "followed_league_count": _count},
    "match_opened": {"fixture_id": _fixture, "after_kickoff": _flag},
    "fixture_unsaved": {"fixture_id": _fixture},
    "tutorial_dismissed": {"tutorial_id": lambda v: v in TUTORIALS, "action": _enum("got_it", "done", "skip")},
    "tutorial_replayed": {"what": _enum("tips", "introduction")},
    "methodology_opened": {"from_section": _enum("match_guide", "match_header", "discover", "record")},
}


class UsageEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    session_id: UUID
    event_name: str = Field(max_length=40)
    props: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime

    @field_validator("occurred_at")
    @classmethod
    def plausible_time(cls, value):
        if value.tzinfo is None:
            raise ValueError("occurred_at needs a timezone")
        now = datetime.now(timezone.utc)
        if not now - timedelta(days=7) <= value <= now + timedelta(days=1):
            raise ValueError("occurred_at is out of range")
        return value

    @model_validator(mode="after")
    def allowed(self):
        spec = EVENTS.get(self.event_name)
        if spec is None:
            raise ValueError("Unknown event")
        if set(self.props) - set(spec):
            raise ValueError("Unknown event property")
        for key, value in self.props.items():
            if not spec[key](value):
                raise ValueError(f"Invalid {key}")
        return self


class UsageEventBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    events: list[UsageEvent] = Field(min_length=1, max_length=20)


class UsageEventsSetting(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: StrictBool


def usage_events_enabled(db, user_id: str) -> bool:
    value = db.execute(text("SELECT usage_events_enabled FROM app.user_preferences WHERE user_id = :user_id"), {"user_id": user_id}).scalar()
    return True if value is None else bool(value)


def save_usage_events_setting(db, user_id: str, enabled: bool) -> bool:
    return bool(db.execute(text("""
        INSERT INTO app.user_preferences (user_id, usage_events_enabled)
        VALUES (:user_id, :enabled)
        ON CONFLICT (user_id) DO UPDATE SET
            usage_events_enabled = EXCLUDED.usage_events_enabled,
            updated_at = NOW()
        RETURNING usage_events_enabled
    """), {"user_id": user_id, "enabled": enabled}).scalar_one())


def insert_events(db, user_id: str, events: list[UsageEvent]) -> None:
    db.execute(text("""
        INSERT INTO app.app_events (event_id, user_id, session_id, event_name, props, occurred_at)
        VALUES (:event_id, :user_id, :session_id, :event_name, CAST(:props AS jsonb), :occurred_at)
        ON CONFLICT (event_id) DO NOTHING
    """), [{
        "event_id": str(e.event_id), "user_id": user_id, "session_id": str(e.session_id),
        "event_name": e.event_name, "props": json.dumps(e.props), "occurred_at": e.occurred_at,
    } for e in events])
