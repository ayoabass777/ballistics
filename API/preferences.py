"""Account-owned preferences; null timezone means use the browser's zone."""
from zoneinfo import available_timezones
from functools import lru_cache

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import text


@lru_cache(maxsize=1)
def timezone_names():
    return available_timezones() - {"localtime", "posixrules"}


class PreferencesUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    timezone_override: str | None

    @field_validator("timezone_override")
    @classmethod
    def valid_timezone(cls, value):
        if value is not None and value not in timezone_names():
            raise ValueError("Use an IANA timezone such as Europe/London, or null for browser timezone")
        return value


def read_preferences(db, user_id):
    row = db.execute(text("SELECT timezone_override, created_at, updated_at FROM app.user_preferences WHERE user_id = :user_id"), {"user_id": user_id}).mappings().first()
    return dict(row) if row else {"timezone_override": None, "created_at": None, "updated_at": None}


def save_preferences(db, user_id, timezone_override):
    row = db.execute(text("""
        INSERT INTO app.user_preferences (user_id, timezone_override)
        VALUES (:user_id, :timezone_override)
        ON CONFLICT (user_id) DO UPDATE SET
            timezone_override = EXCLUDED.timezone_override,
            updated_at = NOW()
        RETURNING timezone_override, created_at, updated_at
    """), {"user_id": user_id, "timezone_override": timezone_override}).mappings().one()
    return dict(row)
