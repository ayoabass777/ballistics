"""Account-owned saved fixture snapshots and followed league IDs."""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator
from sqlalchemy import text


class SavedCard(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fixture_id: int = Field(gt=0)
    home: str = Field(min_length=1, max_length=200)
    away: str = Field(min_length=1, max_length=200)
    league_id: int | None = Field(default=None, gt=0)
    league: str = Field(min_length=1, max_length=200)
    country: str | None = Field(default=None, max_length=200)
    kickoff: datetime
    home_crest_url: str | None = Field(default=None, max_length=2000)
    away_crest_url: str | None = Field(default=None, max_length=2000)
    league_logo_url: str | None = Field(default=None, max_length=2000)
    pick: dict[str, Any] | None = None

    @field_validator("pick")
    @classmethod
    def bounded_pick(cls, value):
        if value is not None and len(json.dumps(value, allow_nan=False)) > 4096:
            raise ValueError("Pick snapshot is too large")
        return value


class SavedCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    card: SavedCard
    run_ds: str | None = Field(default=None, max_length=100)


class SavedImportItem(SavedCreate):
    saved_at: datetime


class SavedImport(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[SavedImportItem] = Field(max_length=200)


class FollowedLeaguesUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    league_ids: list[StrictInt] = Field(max_length=200)

    @field_validator("league_ids")
    @classmethod
    def unique_positive(cls, value):
        if any(not isinstance(item, int) or isinstance(item, bool) or item <= 0 or item > 9007199254740991 for item in value):
            raise ValueError("League IDs must be positive integers")
        if len(set(value)) != len(value):
            raise ValueError("League IDs must be unique")
        return value


def ensure_account_user(db, user_id: str) -> None:
    db.execute(
        text("INSERT INTO app.app_users (user_id) VALUES (:user_id) ON CONFLICT (user_id) DO NOTHING"),
        {"user_id": user_id},
    )


def list_saved(db, user_id: str) -> list[dict]:
    from API.saved_snapshots import read_saved
    return read_saved(db, user_id)


def insert_saved(db, user_id: str, item: SavedCreate, saved_at: datetime | None = None) -> None:
    from API.saved_snapshots import save_reference
    card = item.card.model_dump(mode="json")
    if len(json.dumps(card, allow_nan=False)) > 8192:
        raise ValueError("Card snapshot is too large")
    save_reference(db, user_id, card, item.run_ds, saved_at,
                   'legacy_card_v1' if saved_at is not None else 'client_card_v1')


def delete_saved(db, user_id: str, fixture_id: int) -> None:
    db.execute(text('SELECT user_id FROM app.app_users WHERE user_id=:user_id FOR UPDATE'), {'user_id': user_id})
    db.execute(text("""
        DELETE FROM app.saved_fixture_refs
        WHERE user_id = :user_id AND fixture_id = :fixture_id
    """), {"user_id": user_id, "fixture_id": fixture_id})


def list_followed_leagues(db, user_id: str) -> list[int]:
    rows = db.execute(text("""
        SELECT league_id FROM app.user_followed_leagues
        WHERE user_id = :user_id ORDER BY league_id
    """), {"user_id": user_id}).scalars().all()
    return list(rows)


def league_directory(db) -> list[dict]:
    rows = db.execute(text("""
        SELECT l.league_id, l.league_name AS league, c.country_name AS country, l.logo_file_id
        FROM dim.dim_leagues l LEFT JOIN dim.dim_countries c USING (country_id)
        ORDER BY c.country_name NULLS LAST, l.league_name, l.league_id
    """)).mappings().all()
    base = os.getenv("CREST_BASE_URL") or f"https://{os.getenv('ASSETS_BUCKET', 'ballistics-assets')}.s3.{os.getenv('AWS_DEFAULT_REGION', 'ca-central-1')}.amazonaws.com"
    return [{"league_id": row["league_id"], "league": row["league"], "country": row["country"],
             "league_logo_url": f"{base.rstrip('/')}/crests/leagues/{row['logo_file_id']}" if row["logo_file_id"] else None} for row in rows]


def replace_followed_leagues(db, user_id: str, league_ids: list[int]) -> None:
    # Serialize replacement operations for this account, including an empty set.
    db.execute(text("SELECT user_id FROM app.app_users WHERE user_id = :user_id FOR UPDATE"), {"user_id": user_id})
    if league_ids:
        known = set(db.execute(text("SELECT league_id FROM dim.dim_leagues WHERE league_id = ANY(CAST(:ids AS bigint[]))"), {"ids": league_ids}).scalars().all())
        if known != set(league_ids):
            raise ValueError("One or more leagues are unavailable")
    # Retained follows keep their original creation timestamp.
    db.execute(text("DELETE FROM app.user_followed_leagues WHERE user_id = :user_id AND NOT (league_id = ANY(CAST(:ids AS bigint[])))"), {"user_id": user_id, "ids": league_ids})
    for league_id in league_ids:
        db.execute(text("""
            INSERT INTO app.user_followed_leagues (user_id, league_id)
            VALUES (:user_id, :league_id) ON CONFLICT (user_id, league_id) DO NOTHING
        """), {"user_id": user_id, "league_id": league_id})
