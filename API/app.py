import json
import hmac
import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exception_handlers import http_exception_handler
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from API.preferences import PreferencesUpdate, read_preferences, save_preferences
from API.usage_events import (
    UsageEventBatch, UsageEventsSetting, insert_events, save_usage_events_setting, usage_events_enabled,
)
from API.account_data import (
    FollowedLeaguesUpdate, SavedCreate, SavedImport, delete_saved,
    ensure_account_user, insert_saved, list_followed_leagues, list_saved,
    replace_followed_leagues, league_directory,
)
from API.core.database import get_db
from API.beta_invites import InviteClaim, claim_code
from API.models.api_models import (
    FixtureAnalysisResponse,
    FixturePageContextData,
    FixturePageContextResponse,
    HomeStatsResponse,
    NavigationLeagueItem,
    NavigationLeaguesResponse,
    Streak,
    TeamSearchResponse,
    TeamSearchSuggestion,
    TeamPageContextData,
    TeamPageContextResponse,
    TeamPageContextStreak,
    TeamPagePeer,
    TeamStreakRecord,
    TeamSummary,
)
from API.models.db_models import (
    DBEloRating,
    DBFixturePageContext,
    DBHomepageStreak,
    DBLeagueInfo,
    DBNavLeague,
    DBStreak,
    DBTeamForm,
    DBTeamInfo,
    DBTeamPageContext,
    DBTeamStat,
    DBTeamStreakRecord,
)
from API.utils.headliner import get_event_headline


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


HOME_STREAKS_LIMIT = 20
EXPLORE_STREAKS_CACHE_KEY = "explore:streaks:v1:all"
EXPLORE_STREAKS_CACHE_TTL_SECONDS = _env_int("EXPLORE_STREAKS_CACHE_TTL_SECONDS", 120)
EXPLORE_STREAKS_RESTORE_LIMIT_MAX = _env_int("EXPLORE_STREAKS_RESTORE_LIMIT_MAX", 1000)
LOGO_FILE_ID_PATTERN = re.compile(r"^[a-fA-F0-9-]{8,64}$")
LOGO_BASE_PATH = Path(os.getenv("LOGO_BASE_PATH", Path(os.getenv("BASE_DATA_DIR", "./data")) / "logos"))
FORM_FIXTURE_RELATIONS = ("raw_int.fact_team_fixtures", "raw_int.int_fact_fixtures")

app = FastAPI(title="Football Stats API")
logger = logging.getLogger("account_api")


@app.exception_handler(HTTPException)
async def log_server_errors(request: Request, exc: HTTPException):
    """Log why a 5xx happened; the response itself is unchanged.

    Handlers raise HTTPException(503, ...) from the real error, so without
    this the cause is lost. Only the cause's first line is logged: SQLAlchemy
    appends the SQL and its parameters, which can hold member data.
    """
    if exc.status_code >= 500:
        cause = exc.__cause__
        detail = getattr(cause, "orig", None) or cause
        first_line = str(detail).strip().splitlines()[0][:300] if detail else ""
        logger.error("%s %s -> %s %s | cause: %s: %s", request.method, request.url.path,
                     exc.status_code, exc.detail, type(detail).__name__ if detail else "none", first_line)
    return await http_exception_handler(request, exc)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CLERK_JWKS_URL = os.getenv("CLERK_JWKS_URL")
CLERK_ISSUER = os.getenv("CLERK_ISSUER")
CLERK_AUDIENCE = os.getenv("CLERK_AUDIENCE")
CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY")
CLERK_BACKEND_API_BASE = os.getenv("CLERK_BACKEND_API_BASE", "https://api.clerk.com/v1").rstrip("/")


class AuthTokenInvalidError(Exception):
    """Raised when an auth token is malformed, invalid, or expired."""


def _normalize_streak_type(raw_type: str) -> Optional[Literal["goal", "result"]]:
    mapping = {
        "goal_streaks": "goal",
        "result_streaks": "result",
        "goal": "goal",
        "result": "result",
    }
    return mapping.get(str(raw_type), None)


def _normalize_metric_key(event_name: Optional[str]) -> str:
    raw_value = str(event_name or "").strip()
    if not raw_value:
        return "event"

    normalized = raw_value.lower().replace("-", "_").replace(" ", "_")
    normalized = re.sub(r"[^a-z0-9_]", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")

    aliases = {
        "score_1_goal": "score_1goal",
        "score_1_goals": "score_1goal",
        "score1goal": "score_1goal",
        "score1goals": "score_1goal",
        "score_2_goal": "score_2goals",
        "score_2_goals": "score_2goals",
        "score2goal": "score_2goals",
        "score2goals": "score_2goals",
        "concede_1_goal": "concede_1",
        "concede_1_goals": "concede_1",
        "conceded_1": "concede_1",
        "conceded_1_goal": "concede_1",
        "conceded_1_goals": "concede_1",
        "concede_2_goal": "concede_2",
        "concede_2_goals": "concede_2",
        "conceded_2": "concede_2",
        "conceded_2_goal": "concede_2",
        "conceded_2_goals": "concede_2",
        "btts": "BTTS",
        "over_1_5_goals": "Over_1_5",
        "over_2_5_goals": "Over_2_5",
        "over_3_5_goals": "Over_3_5",
        "over_4_5_goals": "Over_4_5",
        "under_1_5_goals": "Under_1_5",
        "under_2_5_goals": "Under_2_5",
        "under_3_5_goals": "Under_3_5",
        "under_4_5_goals": "Under_4_5",
    }
    if normalized in aliases:
        return aliases[normalized]

    special_case = {
        "over_1_5": "Over_1_5",
        "over_2_5": "Over_2_5",
        "over_3_5": "Over_3_5",
        "over_4_5": "Over_4_5",
        "under_1_5": "Under_1_5",
        "under_2_5": "Under_2_5",
        "under_3_5": "Under_3_5",
        "under_4_5": "Under_4_5",
    }
    return special_case.get(normalized, normalized or "event")


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _coerce_utc_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_past_or_current_kickoff(value: Any) -> bool:
    kickoff = _coerce_utc_datetime(value)
    if kickoff is None:
        return False
    return kickoff <= datetime.now(timezone.utc)


@lru_cache(maxsize=1)
def _get_clerk_jwks_client() -> Any:
    if not CLERK_JWKS_URL:
        raise RuntimeError("CLERK_JWKS_URL is not configured")
    try:
        from jwt import PyJWKClient
    except ImportError as exc:
        raise RuntimeError("PyJWT is not installed in the API environment") from exc
    return PyJWKClient(CLERK_JWKS_URL)


def _verify_clerk_session_token(token: str) -> Dict[str, Any]:
    # Local imports keep API startup resilient if PyJWT is not installed.
    try:
        import jwt
        from jwt import InvalidTokenError
    except ImportError as exc:
        raise RuntimeError("PyJWT is not installed in the API environment") from exc

    try:
        signing_key = _get_clerk_jwks_client().get_signing_key_from_jwt(token)
        decode_kwargs: Dict[str, Any] = {
            "algorithms": ["RS256"],
            "options": {
                "verify_signature": True,
                "verify_exp": True,
                "verify_iat": True,
                "verify_aud": bool(CLERK_AUDIENCE),
                "verify_iss": bool(CLERK_ISSUER),
            },
        }
        if CLERK_AUDIENCE:
            decode_kwargs["audience"] = CLERK_AUDIENCE
        if CLERK_ISSUER:
            decode_kwargs["issuer"] = CLERK_ISSUER
        claims = jwt.decode(token, signing_key.key, **decode_kwargs)
        if not isinstance(claims, dict):
            raise InvalidTokenError("Invalid token payload")
        return claims
    except InvalidTokenError as exc:
        raise AuthTokenInvalidError("Invalid or expired token") from exc


def get_current_user_claims(
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    if not authorization:
        raise HTTPException(status_code=401, detail="Authentication required")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    try:
        return _verify_clerk_session_token(token.strip())
    except AuthTokenInvalidError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Authentication verification failed") from exc


def _claim_text(claims: Dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = claims.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _extract_email_from_clerk_user(payload: Dict[str, Any]) -> Optional[str]:
    email_addresses = payload.get("email_addresses")
    if not isinstance(email_addresses, list):
        return None

    primary_id = str(payload.get("primary_email_address_id") or "").strip()
    for item in email_addresses:
        if not isinstance(item, dict):
            continue
        if primary_id and str(item.get("id") or "").strip() != primary_id:
            continue
        email = str(item.get("email_address") or "").strip()
        if email:
            return email

    for item in email_addresses:
        if not isinstance(item, dict):
            continue
        email = str(item.get("email_address") or "").strip()
        if email:
            return email
    return None


def _fetch_clerk_user_profile(user_id: str) -> Dict[str, Optional[str]]:
    if not CLERK_SECRET_KEY:
        return {}
    if not user_id:
        return {}

    try:
        import httpx
    except Exception:
        return {}

    try:
        response = httpx.get(
            f"{CLERK_BACKEND_API_BASE}/users/{user_id}",
            headers={
                "Authorization": f"Bearer {CLERK_SECRET_KEY}",
                "Accept": "application/json",
            },
            timeout=8.0,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return {}
        return {
            "email": _extract_email_from_clerk_user(payload),
            "first_name": _claim_text(payload, "first_name"),
            "last_name": _claim_text(payload, "last_name"),
            "image_url": _claim_text(payload, "image_url"),
        }
    except Exception:
        # Keep /me resilient even if Clerk profile lookup is temporarily unavailable.
        return {}


def _upsert_app_user_from_claims(db: Session, claims: Dict[str, Any]) -> Dict[str, Any]:
    user_id = _claim_text(claims, "sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    email = _claim_text(claims, "email", "email_address")
    first_name = _claim_text(claims, "first_name", "given_name")
    last_name = _claim_text(claims, "last_name", "family_name")
    image_url = _claim_text(claims, "image_url", "picture")

    if not email or not first_name or not last_name or not image_url:
        clerk_profile = _fetch_clerk_user_profile(user_id)
        email = email or _claim_text(clerk_profile, "email")
        first_name = first_name or _claim_text(clerk_profile, "first_name")
        last_name = last_name or _claim_text(clerk_profile, "last_name")
        image_url = image_url or _claim_text(clerk_profile, "image_url")

    upserted = (
        db.execute(
            text(
                """
                INSERT INTO app.app_users (
                    user_id,
                    email,
                    first_name,
                    last_name,
                    image_url,
                    auth_provider
                )
                VALUES (
                    :user_id,
                    :email,
                    :first_name,
                    :last_name,
                    :image_url,
                    'clerk'
                )
                ON CONFLICT (user_id) DO UPDATE SET
                    email = EXCLUDED.email,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    image_url = EXCLUDED.image_url,
                    updated_at = NOW()
                RETURNING
                    user_id,
                    email,
                    first_name,
                    last_name,
                    image_url,
                    plan,
                    onboarding_completed
                """
            ),
            {
                "user_id": user_id,
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "image_url": image_url,
            },
        )
        .mappings()
        .first()
    )

    if not upserted:
        raise HTTPException(status_code=503, detail="Failed to sync authenticated user")

    db.commit()
    return dict(upserted)


def _find_logo_file_path(file_id: str) -> Optional[Path]:
    if not file_id or not LOGO_FILE_ID_PATTERN.match(file_id):
        return None
    if not LOGO_BASE_PATH.exists():
        return None

    matches = list(LOGO_BASE_PATH.rglob(f"{file_id}.*"))
    if not matches:
        return None
    return matches[0]


def _parse_recent_form(value: Optional[str]) -> List[str]:
    if not value:
        return []
    stripped = value.strip("{}")
    if not stripped:
        return []
    parsed = []
    for token in stripped.split(","):
        cleaned = token.strip().strip('"')
        if cleaned:
            parsed.append(cleaned)
    return parsed


def _ensure_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _ensure_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _normalize_form(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if isinstance(value, str):
        return _parse_recent_form(value)
    return []


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _slugify_text(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return slug.strip("-") or "item"


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _logo_asset_url(request: Request, file_id: Optional[str]) -> Optional[str]:
    if not file_id:
        return None
    normalized = str(file_id).strip()
    if not normalized:
        return None
    base = str(request.base_url).rstrip("/")
    return f"{base}/assets/logos/{normalized}"


def _normalize_context_scope(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"home", "away", "overall"}:
        return raw
    if raw == "all":
        return "overall"
    return "overall"


def _normalize_team_form_context(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"all", "overall"}:
        return "all"
    if raw in {"home", "away"}:
        return raw
    return "all"


def _score_team_info_row(
    row: Any,
    *,
    preferred_fixture_id: Optional[int] = None,
    preferred_opponent_id: Optional[int] = None,
) -> tuple[int, int, int, int, int, int]:
    row_fixture_id = _safe_int(getattr(row, "next_fixture_id", None), default=0)
    row_opponent_id = _safe_int(getattr(row, "next_opponent_id", None), default=0)
    row_league_season_id = _safe_int(getattr(row, "league_season_id", None), default=0)
    logo_file_id = _clean_text(getattr(row, "logo_file_id", None))
    next_is_home = getattr(row, "next_is_home", None)
    next_kickoff_utc = getattr(row, "next_kickoff_utc", None)

    return (
        1 if preferred_fixture_id and row_fixture_id == preferred_fixture_id else 0,
        1 if preferred_opponent_id and row_opponent_id == preferred_opponent_id else 0,
        1 if logo_file_id else 0,
        1 if next_is_home is not None else 0,
        1 if row_league_season_id > 0 else 0,
        1 if next_kickoff_utc is not None else 0,
    )


def _select_best_team_info_row(
    rows: List[Any],
    *,
    preferred_fixture_id: Optional[int] = None,
    preferred_opponent_id: Optional[int] = None,
) -> Any:
    best_row: Any = None
    best_score: Optional[tuple[int, int, int, int, int, int]] = None

    for row in rows:
        score = _score_team_info_row(
            row,
            preferred_fixture_id=preferred_fixture_id,
            preferred_opponent_id=preferred_opponent_id,
        )
        if best_score is None or score > best_score:
            best_row = row
            best_score = score

    return best_row


def _scope_matches_next_fixture_venue(
    context_scope: str,
    next_is_home: Optional[bool],
) -> bool:
    if next_is_home is None:
        return True
    if context_scope == "home":
        return next_is_home is True
    if context_scope == "away":
        return next_is_home is False
    return True


def _load_team_form_score_map(
    db: Session,
    team_ids: List[int],
    league_season_ids: List[int],
) -> Dict[tuple[int, int, str, str], float]:
    if not team_ids or not league_season_ids:
        return {}

    rows = (
        db.query(DBTeamForm)
        .filter(
            DBTeamForm.team_id.in_(team_ids),
            DBTeamForm.league_season_id.in_(league_season_ids),
        )
        .all()
    )

    score_map: Dict[tuple[int, int, str, str], float] = {}
    for row in rows:
        league_season_id = _safe_int(getattr(row, "league_season_id", None), default=0)
        team_id = _safe_int(getattr(row, "team_id", None), default=0)
        metric_key = _normalize_metric_key(getattr(row, "event_name", None))
        context = _normalize_team_form_context(getattr(row, "context", None))
        try:
            score = float(getattr(row, "score", None))
        except (TypeError, ValueError):
            continue

        if league_season_id <= 0 or team_id <= 0:
            continue

        key = (league_season_id, team_id, metric_key, context)
        existing = score_map.get(key)
        if existing is None or score > existing:
            score_map[key] = score

    return score_map


def _lookup_confidence_score(
    score_map: Dict[tuple[int, int, str, str], float],
    league_season_id: int,
    team_id: int,
    metric_key: str,
    context_scope: Any,
) -> Optional[float]:
    if league_season_id <= 0 or team_id <= 0:
        return None

    normalized_metric = _normalize_metric_key(metric_key)
    context = _normalize_team_form_context(context_scope)
    candidate_contexts = [context] + (["all"] if context != "all" else [])
    for candidate in candidate_contexts:
        value = score_map.get((league_season_id, team_id, normalized_metric, candidate))
        if value is not None:
            return value
    return None


def _to_form_result(value: Any) -> Optional[str]:
    raw = str(value or "").strip().lower()
    if raw in {"w", "win"}:
        return "W"
    if raw in {"d", "draw"}:
        return "D"
    if raw in {"l", "loss", "lose"}:
        return "L"
    return None


def _load_recent_form_rows_from_fixtures(db: Session, team_ids: List[int]) -> List[Dict[str, Any]]:
    if not team_ids or not hasattr(db, "execute"):
        return []

    last_error: Optional[SQLAlchemyError] = None
    for relation in FORM_FIXTURE_RELATIONS:
        for result_column in ("fulltime_result", "result"):
            try:
                return (
                    db.execute(
                        text(
                            f"""
                            SELECT
                                team_id,
                                is_home,
                                {result_column} AS result_value,
                                kickoff_utc,
                                fixture_id
                            FROM {relation}
                            WHERE team_id = ANY(:team_ids)
                              AND is_played = TRUE
                            ORDER BY team_id, kickoff_utc DESC, fixture_id DESC
                            """
                        ),
                        {"team_ids": team_ids},
                    )
                    .mappings()
                    .all()
                )
            except SQLAlchemyError as exc:
                last_error = exc
                if hasattr(db, "rollback"):
                    db.rollback()
                continue

    if last_error is not None:
        raise last_error
    return []


def _load_recent_form_by_scope_map(
    db: Session,
    team_ids: List[int],
    limit: int = 5,
) -> Dict[int, Dict[str, List[str]]]:
    rows = _load_recent_form_rows_from_fixtures(db, team_ids=team_ids)
    if not rows:
        return {}

    max_items = max(1, int(limit or 5))
    recent_form_by_scope: Dict[int, Dict[str, List[str]]] = {}
    for row in rows:
        team_id = _safe_int(row.get("team_id"), default=0)
        if team_id <= 0:
            continue

        result = _to_form_result(row.get("result_value"))
        if result is None:
            continue

        bucket = recent_form_by_scope.setdefault(team_id, {"overall": [], "home": [], "away": []})
        if len(bucket["overall"]) < max_items:
            bucket["overall"].append(result)

        context_scope = "home" if bool(row.get("is_home")) else "away"
        if len(bucket[context_scope]) < max_items:
            bucket[context_scope].append(result)

    return recent_form_by_scope


def _load_recent_form_map(
    db: Session,
    team_ids: List[int],
    scope_by_team: Optional[Dict[int, str]] = None,
) -> Dict[int, List[str]]:
    if not team_ids:
        return {}

    recent_form_by_scope = _load_recent_form_by_scope_map(db, team_ids=team_ids, limit=5)
    recent_form_map: Dict[int, List[str]] = {}

    unique_team_ids = sorted({_safe_int(team_id, default=0) for team_id in team_ids if _safe_int(team_id, default=0) > 0})
    for team_id in unique_team_ids:
        raw_scope = scope_by_team.get(team_id) if scope_by_team else None
        scope = _normalize_context_scope(raw_scope)
        context_scope = scope if scope in {"home", "away"} else "overall"
        forms = recent_form_by_scope.get(team_id, {})
        values = forms.get(context_scope) or forms.get("overall") or []
        if values:
            recent_form_map[team_id] = values[:5]

    return recent_form_map


def _load_fixture_streak_rows(db: Session, fixture_id: int) -> List[Dict[str, Any]]:
    try:
        return (
            db.execute(
                text(
                    """
                    select
                        team_id,
                        event_name,
                        streak_type,
                        streak_length,
                        polarity,
                        scope,
                        fixture_streak_json
                    from raw_mart.mart_fixture_streaks
                    where fixture_id = :fixture_id
                    order by streak_length desc, event_name asc
                    """
                ),
                {"fixture_id": fixture_id},
            )
            .mappings()
            .all()
        )
    except SQLAlchemyError:
        if hasattr(db, "rollback"):
            db.rollback()

    return list(
        db.execute(
            text(
                """
                select
                    team_id,
                    event_name,
                    streak_type,
                    streak_length,
                    polarity,
                    scope,
                    json_build_object(
                        'event_name', event_name,
                        'count', streak_length,
                        'total', null,
                        'scope', case
                            when scope = 'all' then 'overall'
                            else scope
                        end,
                        'polarity', polarity
                    ) as fixture_streak_json
                from raw_mart.mart_team_streaks
                where fixture_id = :fixture_id
                order by streak_length desc, event_name asc
                """
            ),
            {"fixture_id": fixture_id},
        )
        .mappings()
        .all()
    )


def _sanitize_recent_form_for_streak(
    metric_key: str,
    streak_length: int,
    recent_form: Optional[List[str]],
) -> Optional[List[str]]:
    if not recent_form:
        return None

    normalized = [value for value in recent_form if value in {"W", "D", "L"}][:5]
    if not normalized:
        return None

    normalized_metric = _normalize_metric_key(metric_key)
    check_length = max(0, min(int(streak_length or 0), len(normalized)))
    if check_length == 0:
        return normalized

    prefix = normalized[:check_length]
    validators = {
        "winless": lambda values: all(value in {"D", "L"} for value in values),
        "unbeaten": lambda values: all(value in {"W", "D"} for value in values),
        "win": lambda values: all(value == "W" for value in values),
        "loss": lambda values: all(value == "L" for value in values),
        "draw": lambda values: all(value == "D" for value in values),
    }
    validator = validators.get(normalized_metric)
    if validator is None:
        return normalized

    return normalized if validator(prefix) else None


def _load_opponent_tier_map(db: Session, league_season_ids: List[int]) -> Dict[tuple[int, int], int]:
    if not league_season_ids or not hasattr(db, "execute"):
        return {}

    rows = (
        db.execute(
            text(
                """
                WITH latest_per_team AS (
                    SELECT
                        team_id,
                        league_season_id,
                        post_elo_rating,
                        ROW_NUMBER() OVER (
                            PARTITION BY league_season_id, team_id
                            ORDER BY fixture_id DESC
                        ) AS rn
                    FROM elo.elo_rating
                    WHERE snapshot_type = 'match'
                      AND league_season_id = ANY(:league_season_ids)
                ),
                tiered AS (
                    SELECT
                        team_id,
                        league_season_id,
                        NTILE(3) OVER (
                            PARTITION BY league_season_id
                            ORDER BY post_elo_rating ASC
                        ) AS opponent_tier
                    FROM latest_per_team
                    WHERE rn = 1
                )
                SELECT team_id, league_season_id, opponent_tier
                FROM tiered
                """
            ),
            {"league_season_ids": league_season_ids},
        )
        .mappings()
        .all()
    )

    opponent_tier_map: Dict[tuple[int, int], int] = {}
    for row in rows:
        team_id = _safe_int(row.get("team_id"), default=0)
        league_season_id = _safe_int(row.get("league_season_id"), default=0)
        tier = _safe_int(row.get("opponent_tier"), default=0)
        if team_id <= 0 or league_season_id <= 0 or tier <= 0:
            continue
        opponent_tier_map[(team_id, league_season_id)] = tier

    return opponent_tier_map


def _parse_cursor_index(cursor: Optional[str], fallback_offset: int = 0) -> int:
    if cursor is None or str(cursor).strip() == "":
        return max(0, int(fallback_offset))
    try:
        idx = int(str(cursor).strip())
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Invalid cursor") from exc
    if idx < 0:
        raise HTTPException(status_code=422, detail="Invalid cursor")
    return idx


def _streak_field_value(item: Any, field: str) -> Any:
    if isinstance(item, dict):
        return item.get(field)
    return getattr(item, field, None)


@lru_cache(maxsize=1)
def _get_explore_redis_client() -> Any:
    enabled = str(os.getenv("EXPLORE_STREAKS_CACHE_ENABLED", "true")).strip().lower()
    if enabled in {"0", "false", "no", "off"}:
        return None

    redis_url = os.getenv("EXPLORE_STREAKS_REDIS_URL") or os.getenv("REDIS_URL")
    if not redis_url:
        return None

    try:
        import redis
    except ImportError:
        return None

    return redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=0.15,
        socket_timeout=0.15,
    )


def _streak_to_cache_item(item: Any) -> Dict[str, Any]:
    if isinstance(item, dict):
        data = dict(item)
    elif hasattr(item, "model_dump"):
        data = item.model_dump(mode="json")
    else:
        data = dict(getattr(item, "__dict__", {}))

    return data


def _hydrate_cached_streak_item(request: Request, item: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(item)
    data["team_logo_url"] = _logo_asset_url(request, data.get("team_logo_file_id"))
    data["league_logo_url"] = _logo_asset_url(request, data.get("league_logo_file_id"))
    data["country_logo_url"] = _logo_asset_url(request, data.get("country_logo_file_id"))
    return data


def _load_cached_explore_streaks(request: Request) -> tuple[Optional[List[Dict[str, Any]]], str]:
    redis_client = _get_explore_redis_client()
    if redis_client is None or EXPLORE_STREAKS_CACHE_TTL_SECONDS <= 0:
        return None, "bypass"

    try:
        raw_payload = redis_client.get(EXPLORE_STREAKS_CACHE_KEY)
        if not raw_payload:
            return None, "miss"
        payload = json.loads(raw_payload)
    except Exception:
        return None, "error"

    raw_streaks = payload.get("streaks") if isinstance(payload, dict) else None
    if not isinstance(raw_streaks, list):
        return None, "error"

    hydrated = [
        _hydrate_cached_streak_item(request, item)
        for item in raw_streaks
        if isinstance(item, dict)
    ]
    return hydrated, "hit"


def _store_cached_explore_streaks(items: List[Any]) -> str:
    redis_client = _get_explore_redis_client()
    if redis_client is None or EXPLORE_STREAKS_CACHE_TTL_SECONDS <= 0:
        return "bypass"

    payload = {
        "cachedAtIso": datetime.now(timezone.utc).isoformat(),
        "streaks": [_streak_to_cache_item(item) for item in items],
    }
    try:
        redis_client.setex(
            EXPLORE_STREAKS_CACHE_KEY,
            EXPLORE_STREAKS_CACHE_TTL_SECONDS,
            json.dumps(payload, default=str),
        )
    except Exception:
        return "error"
    return "stored"


def _build_navigation_leagues_payload(
    request: Request,
    nav_rows: List[DBNavLeague],
    league_info_rows: List[DBLeagueInfo],
) -> NavigationLeaguesResponse:
    info_by_season_id: Dict[int, DBLeagueInfo] = {}
    for info in league_info_rows:
        season_id = _safe_int(getattr(info, "league_season_id", None), default=0)
        if season_id <= 0:
            continue
        info_by_season_id[season_id] = info

    deduped_by_slug: Dict[str, NavigationLeagueItem] = {}
    fallback_slug_count = 0
    saw_duplicate = False

    for row in nav_rows:
        league_season_id = _safe_int(getattr(row, "league_season_id", None), default=0)
        if league_season_id <= 0:
            continue

        league_name = _clean_text(getattr(row, "league_name", None)) or "Unknown League"
        country_name = _clean_text(getattr(row, "country_name", None)) or "Unknown Country"

        info_row = info_by_season_id.get(league_season_id)
        league_slug = _clean_text(getattr(info_row, "league_slug", None))
        if not league_slug:
            league_slug = _slugify_text(league_name)
            fallback_slug_count += 1

        info_country_name = _clean_text(getattr(info_row, "country_name", None))
        final_country_name = info_country_name or country_name

        item = NavigationLeagueItem(
            league_season_id=league_season_id,
            league_name=league_name,
            league_slug=league_slug,
            country_name=final_country_name,
            league_logo_url=_logo_asset_url(request, _clean_text(getattr(row, "league_logo_file_id", None))),
            country_logo_url=_logo_asset_url(request, _clean_text(getattr(row, "country_logo_file_id", None))),
            href=f"/league/{league_slug}",
        )

        existing = deduped_by_slug.get(league_slug)
        if existing is None:
            deduped_by_slug[league_slug] = item
            continue

        saw_duplicate = True
        if item.league_season_id > existing.league_season_id:
            deduped_by_slug[league_slug] = item

    leagues = sorted(
        deduped_by_slug.values(),
        key=lambda item: (item.country_name.lower(), item.league_name.lower(), item.league_slug.lower()),
    )

    return NavigationLeaguesResponse(
        data={"leagues": leagues},
        meta={
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "source": "raw_mart.mart_nav_leagues+mart_league_info",
            "deduped": saw_duplicate,
            "fallbackSlugCount": fallback_slug_count,
        },
    )


def _build_team_search_payload(
    request: Request,
    rows: List[Dict[str, Any]],
    query: str,
    limit: int,
) -> TeamSearchResponse:
    suggestions: List[TeamSearchSuggestion] = []
    seen_ids: set[int] = set()

    for row in rows:
        team_id = _safe_int(row.get("team_id"), default=0)
        if team_id <= 0 or team_id in seen_ids:
            continue

        team_name = _clean_text(row.get("team_name")) or _clean_text(row.get("name")) or "Unknown Team"
        slug = _canonical_team_slug(team_name, team_id, row.get("team_slug"))
        logo_file_id = _clean_text(row.get("logo_file_id"))
        league_name = _clean_text(row.get("league_name"))
        country_logo_file_id = _clean_text(row.get("country_logo_file_id"))

        suggestions.append(
            TeamSearchSuggestion(
                team_id=team_id,
                name=team_name,
                slug=slug,
                href=f"/team/{slug}",
                logo_url=_logo_asset_url(request, logo_file_id),
                league_name=league_name,
                country_logo_url=_logo_asset_url(request, country_logo_file_id),
            )
        )
        seen_ids.add(team_id)

    return TeamSearchResponse(
        data={"suggestions": suggestions},
        meta={
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "query": query,
            "limit": limit,
            "source": "raw_mart.mart_dim_search",
        },
    )


def _parse_compact_form(value: Any) -> List[str]:
    raw = str(value or "").strip().upper()
    if not raw:
        return []
    compact = [char for char in raw if char in {"W", "D", "L"}]
    if compact:
        return compact[:5]
    return [token for token in _normalize_form(value) if token in {"W", "D", "L"}][:5]


def _event_label_for_fixture(event_name: Optional[str], surface: str = "fixture") -> str:
    metric_key = _normalize_metric_key(event_name)
    return get_event_headline(metric_key, mode="form", surface=surface)


def _form_matches_from_json(raw_value: Any) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for item in _ensure_json_list(raw_value):
        if not isinstance(item, dict):
            continue
        result = str(item.get("result") or "").strip().upper()
        if result not in {"W", "D", "L"}:
            continue
        matches.append(
            {
                "date_iso": str(item.get("date") or item.get("date_iso") or ""),
                "opponent": str(item.get("opponent") or ""),
                "score": str(item.get("score") or ""),
                "result": result,
                "is_home": bool(item.get("isHome") if "isHome" in item else item.get("is_home")),
            }
        )
    return matches


def _form_results_only(matches: List[Dict[str, Any]]) -> List[str]:
    return [str(match.get("result")) for match in matches if str(match.get("result")) in {"W", "D", "L"}]


def _avg_goals_from_form_matches(matches: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    scored_total = 0
    conceded_total = 0
    counted = 0

    for match in matches:
        score_text = _clean_text(match.get("score")) if isinstance(match, dict) else None
        if not score_text:
            continue

        parsed = re.match(r"^\s*(\d+)\s*[-:]\s*(\d+)\s*$", score_text)
        if not parsed:
            continue

        scored_total += _safe_int(parsed.group(1))
        conceded_total += _safe_int(parsed.group(2))
        counted += 1

    if counted == 0:
        return None

    return {
        "scored": scored_total / counted,
        "conceded": conceded_total / counted,
    }


def _dedupe_h2h_streaks(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[tuple[str, str], Dict[str, Any]] = {}
    for item in items:
        key = (str(item.get("event", "")), str(item.get("teams", "")))
        existing = best.get(key)
        item_count = _safe_int(item.get("count"))
        existing_count = _safe_int(existing.get("count")) if existing is not None else -1
        should_replace = existing is None or item_count > existing_count
        if not should_replace and existing is not None and item_count == existing_count:
            item_total = _safe_int(item.get("total")) if item.get("total") is not None else -1
            existing_total = _safe_int(existing.get("total")) if existing.get("total") is not None else -1
            should_replace = item_total > existing_total
        if should_replace:
            best[key] = item
    deduped = list(best.values())
    deduped.sort(key=lambda row: (-_safe_int(row.get("count")), str(row.get("event", "")), str(row.get("teams", ""))))
    return deduped


def _is_eligible_h2h_streak(item: Dict[str, Any]) -> bool:
    count_value = item.get("count")
    total_value = item.get("total")
    if count_value is None or total_value is None:
        return False
    count_int = _safe_int(count_value)
    total_int = _safe_int(total_value)
    return count_int > 0 and total_int > 0 and count_int == total_int


def _normalize_streak_type_for_api(raw_type: Any) -> Optional[str]:
    normalized = _normalize_streak_type(str(raw_type or ""))
    if normalized is None:
        return None
    return f"{normalized}_streaks"


def _normalize_polarity(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"positive", "negative", "neutral"}:
        return raw
    return "neutral"


def _canonical_team_slug(team_name: str, team_id: Any, raw_slug: Any = None) -> str:
    provided = str(raw_slug or "").strip()
    if provided:
        suffix_match = re.search(r"-(\d+)$", provided)
        if suffix_match:
            return _slugify_text(provided)
    return f"{_slugify_text(team_name)}-{_safe_int(team_id)}"


def _extract_team_id_from_slug(team_slug: str) -> Optional[int]:
    match = re.search(r"-(\d+)$", str(team_slug or "").strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _serialize_team_summary(record: DBTeamStat, recent_form: Optional[List[str]] = None) -> TeamSummary:
    resolved_recent_form = (
        [value for value in (recent_form or []) if value in {"W", "D", "L"}][:5]
        if recent_form is not None
        else _parse_recent_form(record.recent_form)
    )
    return TeamSummary(
        team_id=record.team_id,
        team_name=record.team_name,
        league_name=record.league_name,
        league_season_id=record.league_season_id,
        season=str(record.season) if record.season is not None else None,
        win_rate=record.win_rate,
        draw_rate=record.draw_rate,
        loss_rate=record.loss_rate,
        avg_goals_for=record.avg_goals_for,
        avg_goals_against=record.avg_goals_against,
        next_opponent_id=record.next_opponent_id,
        next_opponent_name=record.next_opponent_name,
        next_fixture_id=record.next_fixture_id,
        next_kickoff_utc=_to_iso(record.next_kickoff_utc),
        recent_form=resolved_recent_form,
    )


def _serialize_team_streaks(rows: List[DBTeamStreakRecord]) -> List[TeamStreakRecord]:
    serialized: List[TeamStreakRecord] = []
    for row in rows:
        streak_type = _normalize_streak_type(str(row.streak_type))
        if streak_type is None:
            continue
        serialized.append(
            TeamStreakRecord(
                team_id=row.team_id,
                team_name=row.team_name,
                event_name=row.event_name,
                streak_type=streak_type,
                current_streak_length=row.streak_length,
            )
        )
    return serialized


def _serialize_context_streaks(
    raw_streaks: Any,
    default_team_id: Optional[int] = None,
    default_team_name: Optional[str] = None,
) -> List[TeamStreakRecord]:
    streaks: List[TeamStreakRecord] = []
    for item in _ensure_json_list(raw_streaks):
        if not isinstance(item, dict):
            continue

        streak_type = _normalize_streak_type(str(item.get("streakType") or item.get("streak_type") or ""))
        if streak_type is None:
            continue

        try:
            team_id = int(item.get("teamId") or item.get("team_id") or default_team_id or 0)
            streak_len = int(item.get("streakLength") or item.get("streak_length") or 0)
        except (TypeError, ValueError):
            continue

        event_name = str(item.get("eventName") or item.get("event_name") or "").strip()
        if not event_name:
            continue

        team_name = str(item.get("teamName") or item.get("team_name") or default_team_name or "")
        streaks.append(
            TeamStreakRecord(
                team_id=team_id,
                team_name=team_name,
                event_name=event_name,
                streak_type=streak_type,
                current_streak_length=streak_len,
            )
        )
    return streaks


def _serialize_team_page_context(record: DBTeamPageContext) -> TeamPageContextData:
    peers: List[TeamPagePeer] = []
    for item in _ensure_json_list(record.league_teams_json):
        if not isinstance(item, dict):
            continue
        try:
            peer_id = int(item.get("teamId") or item.get("team_id"))
        except (TypeError, ValueError):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        peers.append(
            TeamPagePeer(
                team_id=peer_id,
                name=name,
                team_href=item.get("teamHref") or item.get("team_href"),
                logo_file_id=item.get("logoFileId") or item.get("logo_file_id"),
            )
        )

    streaks: List[TeamPageContextStreak] = []
    for item in _ensure_json_list(record.streaks_json):
        if not isinstance(item, dict):
            continue
        streak_type = _normalize_streak_type(str(item.get("streakType") or item.get("streak_type") or ""))
        if streak_type is None:
            continue
        try:
            team_id = int(item.get("teamId") or item.get("team_id") or record.team_id)
            streak_len = int(item.get("streakLength") or item.get("streak_length") or 0)
        except (TypeError, ValueError):
            continue
        event_name = str(item.get("eventName") or item.get("event_name") or "").strip()
        if not event_name:
            continue
        raw_streak_id = item.get("streakId") or item.get("streak_id")
        streaks.append(
            TeamPageContextStreak(
                team_id=team_id,
                streak_id=str(raw_streak_id) if raw_streak_id is not None else None,
                event_name=event_name,
                streak_type=streak_type,
                streak_length=streak_len,
                polarity=item.get("polarity"),
                scope=item.get("scope"),
            )
        )

    return TeamPageContextData(
        team_id=record.team_id,
        team_name=record.team_name,
        team_slug=record.team_slug,
        rank=record.rank,
        form=_normalize_form(record.form),
        league_name=record.league_name,
        season_label=record.season_label,
        country_name=record.country_name,
        league_teams=peers,
        streaks=streaks,
        next_fixture_id=record.next_fixture_id,
        next_opponent_id=record.next_opponent_id,
        next_opponent_name=record.next_opponent_name,
        next_is_home=record.next_is_home,
        next_kickoff_utc=_to_iso(record.next_kickoff_utc),
    )


def _serialize_fixture_page_context(record: DBFixturePageContext) -> FixturePageContextData:
    return FixturePageContextData(
        fixture_id=record.fixture_id,
        fixture_slug=record.fixture_slug,
        team_pair_key=record.team_pair_key,
        home_team_id=record.home_team_id,
        away_team_id=record.away_team_id,
        fixture_info=_ensure_json_object(record.fixture_info_json),
        fixture_streaks=_ensure_json_object(record.fixture_streaks_json),
        h2h_info=_ensure_json_object(record.h2h_info_json),
        h2h_streaks=_ensure_json_list(record.h2h_streaks_json),
        h2h_matches=_ensure_json_list(record.h2h_matches_json),
        h2h_context_scopes=_ensure_json_list(record.h2h_context_scopes_json),
        team_fixtures=_ensure_json_object(record.team_fixtures_json),
    )


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/beta/invite/claim")
def claim_beta_invite(
    body: InviteClaim,
    x_beta_invite_api_key: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
):
    expected = os.getenv("BETA_INVITE_API_KEY", "")
    if not expected or not x_beta_invite_api_key or not hmac.compare_digest(expected, x_beta_invite_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        claim_code(db, body.code, str(body.email))
    except Exception:
        db.rollback()
        raise
    return JSONResponse({"sent": True}, headers={"Cache-Control": "no-store"})


@app.get("/me")
def get_me(
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    # No DDL here: the account role has none. API/sql/bootstrap_accounts.sql
    # creates app.app_users, and every deploy runs it as the owner.
    try:
        user_row = _upsert_app_user_from_claims(db, claims)
    except HTTPException:
        db.rollback()
        raise
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    return {
        "data": {
            "userId": str(user_row.get("user_id") or ""),
            "email": user_row.get("email"),
            "firstName": user_row.get("first_name"),
            "lastName": user_row.get("last_name"),
            "imageUrl": user_row.get("image_url"),
            "plan": str(user_row.get("plan") or "free"),
            "onboardingCompleted": bool(user_row.get("onboarding_completed")),
        },
        "meta": {
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "authProvider": "clerk",
            "source": "app.app_users",
        },
    }


@app.post("/account/onboarding/complete")
def complete_onboarding(
    body: FollowedLeaguesUpdate,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    user_id = _claim_text(claims, "sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    try:
        ensure_account_user(db, user_id)
        current = db.execute(text("SELECT onboarding_completed FROM app.app_users WHERE user_id=:user_id FOR UPDATE"), {"user_id": user_id}).scalar_one()
        if not current:
            replace_followed_leagues(db, user_id, body.league_ids)
            db.execute(text("UPDATE app.app_users SET onboarding_completed=TRUE, updated_at=NOW() WHERE user_id=:user_id"), {"user_id": user_id})
        db.commit()
        return JSONResponse({"data": {"onboarding_completed": True}}, headers={"Cache-Control": "no-store"})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc


def _preferences_request(db, claims, update=None):
    user_id = _claim_text(claims, "sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    try:
        if update is not None:
            # Bootstrap is an explicit deployment step; no request-time DDL.
            # Only identity is needed here; avoid overwriting Clerk profile fields.
            db.execute(text("INSERT INTO app.app_users (user_id) VALUES (:user_id) ON CONFLICT (user_id) DO NOTHING"), {"user_id": user_id})
        data = (read_preferences(db, user_id) if update is None else
                save_preferences(db, user_id, update.timezone_override))
        db.commit()
        return JSONResponse(content=jsonable_encoder({"data": data}), headers={"Cache-Control": "no-store"})
    except HTTPException:
        db.rollback()
        raise
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc


@app.get("/preferences")
def get_preferences(
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    return _preferences_request(db, claims)


@app.post("/preferences")
def update_preferences(
    body: PreferencesUpdate,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    return _preferences_request(db, claims, body)


@app.post("/preferences/usage-events")
def update_usage_events_setting(
    body: UsageEventsSetting,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    def save(uid):
        return {"usage_events_enabled": save_usage_events_setting(db, uid, body.enabled)}
    return _account_data_request(db, claims, save, write=True)


@app.post("/account/events")
def record_usage_events(
    body: UsageEventBatch,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    def record(uid):
        # A member who opted out has nothing stored, even if a stale tab still sends.
        if not usage_events_enabled(db, uid):
            return {"accepted": 0, "enabled": False}
        insert_events(db, uid, body.events)
        return {"accepted": len(body.events), "enabled": True}
    return _account_data_request(db, claims, record, write=True)


def _account_data_request(db, claims, operation, write=False):
    user_id = _claim_text(claims, "sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    try:
        if write:
            ensure_account_user(db, user_id)
        data = operation(user_id)
        if write:
            db.commit()
        return JSONResponse(content=jsonable_encoder({"data": data}), headers={"Cache-Control": "no-store"})
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc


@app.get("/account/saved")
def get_account_saved(
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    return _account_data_request(db, claims, lambda uid: list_saved(db, uid))


@app.get("/account/saved/page")
def get_saved_page(
    scope: Literal['all','upcoming','live','finished','other'] = 'all',
    cursor: Optional[str] = Query(default=None, max_length=1024),
    limit: int = Query(default=12, ge=1, le=50),
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    from API.saved_snapshots import read_saved_page
    return _account_data_request(db, claims, lambda uid: read_saved_page(db, uid, scope, cursor, limit))


@app.get("/account/saved/ids")
def get_saved_ids(claims: Dict[str, Any] = Depends(get_current_user_claims), db: Session = Depends(get_db)):
    from API.saved_snapshots import saved_ids
    return _account_data_request(db, claims, lambda uid: saved_ids(db, uid))


@app.post("/account/saved")
def add_account_saved(
    body: SavedCreate,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    def save(uid):
        insert_saved(db, uid, body)
        return list_saved(db, uid)
    return _account_data_request(db, claims, save, write=True)


@app.post("/account/saved/import")
def import_account_saved(
    body: SavedImport,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    def import_items(uid):
        for item in body.items:
            insert_saved(db, uid, item, item.saved_at)
        return list_saved(db, uid)
    return _account_data_request(db, claims, import_items, write=True)


@app.delete("/account/saved/{fixture_id}")
def remove_account_saved(
    fixture_id: int,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    if fixture_id <= 0:
        raise HTTPException(status_code=422, detail="Invalid fixture ID")
    def remove(uid):
        delete_saved(db, uid, fixture_id)
        return list_saved(db, uid)
    return _account_data_request(db, claims, remove, write=True)


@app.get("/account/leagues/directory")
def get_account_league_directory(
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    return _account_data_request(db, claims, lambda uid: {"leagues": league_directory(db)})


@app.get("/account/leagues")
def get_account_leagues(
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    return _account_data_request(
        db, claims, lambda uid: {"league_ids": list_followed_leagues(db, uid)})


@app.put("/account/leagues")
def update_account_leagues(
    body: FollowedLeaguesUpdate,
    claims: Dict[str, Any] = Depends(get_current_user_claims),
    db: Session = Depends(get_db),
):
    def replace(uid):
        replace_followed_leagues(db, uid, body.league_ids)
        return {"league_ids": list_followed_leagues(db, uid)}
    return _account_data_request(db, claims, replace, write=True)


@app.get("/assets/logos/{file_id}")
def get_logo_asset(file_id: str):
    file_path = _find_logo_file_path(file_id)
    if file_path is None or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Logo asset not found")
    return FileResponse(file_path)


@app.get("/fixtures/page-data")
def get_fixture_page_data(
    request: Request,
    fixture_id: int = Query(...),
    db: Session = Depends(get_db),
):
    try:
        fixture_row = (
            db.execute(
                text(
                    """
                    select
                        fixture_id,
                        league_season_id,
                        league_name,
                        kickoff_utc,
                        home_team_id,
                        away_team_id,
                        home_team_name,
                        away_team_name
                    from raw_int.int_fixtures
                    where fixture_id = :fixture_id
                    limit 1
                    """
                ),
                {"fixture_id": fixture_id},
            )
            .mappings()
            .first()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if fixture_row is None:
        raise HTTPException(status_code=404, detail="Fixture page data not found")

    home_team_id = _safe_int(fixture_row.get("home_team_id"))
    away_team_id = _safe_int(fixture_row.get("away_team_id"))
    league_season_id = _safe_int(fixture_row.get("league_season_id"))
    team_pair_key = f"{min(home_team_id, away_team_id)}_{max(home_team_id, away_team_id)}"

    try:
        league_row = (
            db.execute(
                text(
                    """
                    select
                        league_name,
                        country_name,
                        league_slug,
                        league_logo_file_id::text as league_logo_file_id,
                        country_logo_file_id::text as country_logo_file_id
                    from raw_int.int_league_context
                    where league_season_id = :league_season_id
                    limit 1
                    """
                ),
                {"league_season_id": league_season_id},
            )
            .mappings()
            .first()
        )

        team_meta_rows = (
            db.execute(
                text(
                    """
                    select
                        d.team_id,
                        coalesce(m.team_name, d.team_name) as team_name,
                        m.team_slug,
                        coalesce(m.logo_file_id::text, d.logo_file_id::text) as logo_file_id,
                        d.venue_id
                    from dim.dim_teams d
                    left join raw_mart.mart_team_info m
                      on m.team_id = d.team_id
                    where d.team_id in (:home_team_id, :away_team_id)
                    """
                ),
                {"home_team_id": home_team_id, "away_team_id": away_team_id},
            )
            .mappings()
            .all()
        )

        team_stats_rows = (
            db.execute(
                text(
                    """
                    select *
                    from (
                        select
                            team_id,
                            team_name,
                            avg_goals_for,
                            avg_goals_against,
                            recent_form,
                            next_fixture_id,
                            kickoff_utc,
                            games_played,
                            row_number() over (
                                partition by team_id
                                order by
                                    case
                                        when next_fixture_id = :fixture_id then 0
                                        when next_fixture_id is not null then 1
                                        else 2
                                    end,
                                    kickoff_utc desc
                            ) as rn
                        from raw_mart.mart_team_stats
                        where team_id in (:home_team_id, :away_team_id)
                          and league_season_id = :league_season_id
                    ) ranked
                    where rn = 1
                    """
                ),
                {
                    "fixture_id": fixture_id,
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "league_season_id": league_season_id,
                },
            )
            .mappings()
            .all()
        )

        team_context_avg_rows = (
            db.execute(
                text(
                    """
                    select
                        team_id,
                        avg(goals_scored)::float as overall_scored,
                        avg(goals_conceded)::float as overall_conceded,
                        avg(case when is_home then goals_scored end)::float as home_scored,
                        avg(case when is_home then goals_conceded end)::float as home_conceded,
                        avg(case when not is_home then goals_scored end)::float as away_scored,
                        avg(case when not is_home then goals_conceded end)::float as away_conceded
                    from raw_mart.mart_team_stats
                    where team_id in (:home_team_id, :away_team_id)
                      and league_season_id = :league_season_id
                    group by team_id
                    """
                ),
                {
                    "home_team_id": home_team_id,
                    "away_team_id": away_team_id,
                    "league_season_id": league_season_id,
                },
            )
            .mappings()
            .all()
        )

        team_fixture_rows = (
            db.execute(
                text(
                    """
                    select team_id, context_scope, fixtures_json
                    from raw_mart.mart_team_fixture
                    where team_id in (:home_team_id, :away_team_id)
                      and context_scope in ('overall', 'home', 'away')
                    """
                ),
                {"home_team_id": home_team_id, "away_team_id": away_team_id},
            )
            .mappings()
            .all()
        )

    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    try:
        fixture_streak_rows = _load_fixture_streak_rows(db, fixture_id)
    except SQLAlchemyError:
        db.rollback()
        fixture_streak_rows = []

    try:
        h2h_info_row = (
            db.execute(
                text(
                    """
                    select
                        h2h_info_json,
                        h2h_streaks_json,
                        h2h_matches_json
                    from raw_mart.mart_head_to_head_info
                    where current_fixture_id = :fixture_id
                    limit 1
                    """
                ),
                {"fixture_id": fixture_id},
            )
            .mappings()
            .first()
        )
    except SQLAlchemyError:
        db.rollback()
        h2h_info_row = None

    try:
        h2h_streaks_row = (
            db.execute(
                text(
                    """
                    select h2h_streaks_json
                    from raw_mart.mart_head_to_head_streaks
                    where team_pair_key = :team_pair_key
                    limit 1
                    """
                ),
                {"team_pair_key": team_pair_key},
            )
            .mappings()
            .first()
        )
    except SQLAlchemyError:
        db.rollback()
        h2h_streaks_row = None

    team_meta_by_id = { _safe_int(row.get("team_id")): row for row in team_meta_rows }
    team_stats_by_id = { _safe_int(row.get("team_id")): row for row in team_stats_rows }
    team_context_avg_by_id = { _safe_int(row.get("team_id")): row for row in team_context_avg_rows }

    team_forms_by_id: Dict[int, Dict[str, List[Dict[str, Any]]]] = {
        home_team_id: {"overall": [], "home": [], "away": []},
        away_team_id: {"overall": [], "home": [], "away": []},
    }
    for row in team_fixture_rows:
        team_id = _safe_int(row.get("team_id"))
        scope = str(row.get("context_scope") or "").strip().lower()
        if team_id not in team_forms_by_id or scope not in {"overall", "home", "away"}:
            continue
        team_forms_by_id[team_id][scope] = _form_matches_from_json(row.get("fixtures_json"))

    try:
        recent_form_by_scope = _load_recent_form_by_scope_map(
            db,
            team_ids=[home_team_id, away_team_id],
            limit=5,
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    def build_team_payload(team_id: int, fallback_name: str) -> Dict[str, Any]:
        meta_row = team_meta_by_id.get(team_id, {})
        stats_row = team_stats_by_id.get(team_id, {})
        context_avg_row = team_context_avg_by_id.get(team_id, {})
        team_name = str(meta_row.get("team_name") or stats_row.get("team_name") or fallback_name or f"Team {team_id}")
        raw_team_slug = str(meta_row.get("team_slug") or "").strip()
        team_slug = _slugify_text(raw_team_slug) if raw_team_slug else f"{_slugify_text(team_name)}-{team_id}"
        logo_url = _logo_asset_url(request, meta_row.get("logo_file_id"))

        forms_by_context = team_forms_by_id.get(team_id, {"overall": [], "home": [], "away": []})
        forms_from_fixtures = recent_form_by_scope.get(team_id, {"overall": [], "home": [], "away": []})
        recent_form = forms_from_fixtures.get("overall", [])

        avg_from_overall_form = _avg_goals_from_form_matches(forms_by_context.get("overall", []))
        avg_from_home_form = _avg_goals_from_form_matches(forms_by_context.get("home", []))
        avg_from_away_form = _avg_goals_from_form_matches(forms_by_context.get("away", []))

        # Cumulative rolling averages are the canonical source for fixture average-goals stats.
        # Fall back to parsed recent fixture rows when the stats mart is empty/stale.
        stats_overall = {
            "scored": _safe_float(
                context_avg_row.get("overall_scored"),
                default=_safe_float(
                    stats_row.get("avg_goals_for"),
                    default=_safe_float((avg_from_overall_form or {}).get("scored")),
                ),
            ),
            "conceded": _safe_float(
                context_avg_row.get("overall_conceded"),
                default=_safe_float(
                    stats_row.get("avg_goals_against"),
                    default=_safe_float((avg_from_overall_form or {}).get("conceded")),
                ),
            ),
        }
        context_overall = stats_overall
        context_home = {
            "scored": _safe_float(
                context_avg_row.get("home_scored"),
                default=_safe_float((avg_from_home_form or {}).get("scored"), default=context_overall["scored"]),
            ),
            "conceded": _safe_float(
                context_avg_row.get("home_conceded"),
                default=_safe_float((avg_from_home_form or {}).get("conceded"), default=context_overall["conceded"]),
            ),
        }
        context_away = {
            "scored": _safe_float(
                context_avg_row.get("away_scored"),
                default=_safe_float((avg_from_away_form or {}).get("scored"), default=context_overall["scored"]),
            ),
            "conceded": _safe_float(
                context_avg_row.get("away_conceded"),
                default=_safe_float((avg_from_away_form or {}).get("conceded"), default=context_overall["conceded"]),
            ),
        }

        return {
            "team_id": str(team_id),
            "name": team_name,
            "slug": team_slug,
            "logo_url": logo_url,
            "avg_goals_scored": context_overall["scored"],
            "avg_goals_conceded": context_overall["conceded"],
            "avg_goals": {
                "overall": context_overall,
                "home": context_home,
                "away": context_away,
            },
            "recent_form": recent_form,
            "form_by_context": {
                "overall": forms_from_fixtures.get("overall", []),
                "home": forms_from_fixtures.get("home", []),
                "away": forms_from_fixtures.get("away", []),
            },
        }

    home_team_payload = build_team_payload(home_team_id, str(fixture_row.get("home_team_name") or "Home Team"))
    away_team_payload = build_team_payload(away_team_id, str(fixture_row.get("away_team_name") or "Away Team"))

    fixture_streaks_by_team: Dict[int, List[Dict[str, Any]]] = {home_team_id: [], away_team_id: []}
    for row in fixture_streak_rows:
        team_id = _safe_int(row.get("team_id"))
        if team_id not in fixture_streaks_by_team:
            continue
        if _normalize_streak_type(str(row.get("streak_type") or "")) is None:
            continue

        streak_json = _ensure_json_object(row.get("fixture_streak_json"))
        raw_event_name = str(streak_json.get("event_name") or row.get("event_name") or "")
        total_value = streak_json.get("total")
        total_int = _safe_int(total_value) if total_value is not None else None
        polarity = str(streak_json.get("polarity") or row.get("polarity") or "").strip().lower() or None
        if polarity not in {"positive", "negative", "neutral"}:
            polarity = None

        fixture_streaks_by_team[team_id].append(
            {
                "event": _event_label_for_fixture(raw_event_name, surface="fixture-page"),
                "count": _safe_int(streak_json.get("count") or row.get("streak_length")),
                "total": total_int,
                "scope": _normalize_context_scope(streak_json.get("scope") or row.get("scope")),
                "polarity": polarity,
            }
        )

    h2h_info = _ensure_json_object(h2h_info_row.get("h2h_info_json") if h2h_info_row else None)
    h2h_matches_source = _ensure_json_list(h2h_info_row.get("h2h_matches_json") if h2h_info_row else None)

    h2h_matches: List[Dict[str, Any]] = []
    for item in h2h_matches_source:
        if not isinstance(item, dict):
            continue
        h2h_matches.append(
            {
                "date_iso": str(item.get("date") or item.get("date_iso") or ""),
                "home_score": _safe_int(item.get("home") if "home" in item else item.get("home_score")),
                "away_score": _safe_int(item.get("away") if "away" in item else item.get("away_score")),
                "result": str(item.get("result") or "D"),
                "is_home_team_home": bool(
                    item.get("isHomeTeamHome") if "isHomeTeamHome" in item else item.get("is_home_team_home")
                ),
            }
        )

    raw_h2h_streak_items = _ensure_json_list(h2h_info_row.get("h2h_streaks_json") if h2h_info_row else None)
    if not raw_h2h_streak_items and h2h_streaks_row:
        raw_h2h_streak_items = _ensure_json_list(h2h_streaks_row.get("h2h_streaks_json"))

    mapped_h2h_streaks: List[Dict[str, Any]] = []
    for item in raw_h2h_streak_items:
        if not isinstance(item, dict):
            continue

        raw_event_name = str(item.get("event_name") or item.get("event") or "")
        normalized_event_name = raw_event_name.strip().lower()
        item_scope = str(item.get("scope") or "").strip().lower()
        teams_value = str(item.get("teams") or "").strip().lower()
        item_team_id = _safe_int(item.get("team_id"), default=0)
        is_shared_h2h_streak = normalized_event_name in {
            "btts",
            "over_1_5",
            "over_2_5",
            "over_3_5",
            "over_4_5",
            "under_1_5",
            "under_2_5",
            "under_3_5",
            "under_4_5",
        }
        if item_scope not in {"all", ""}:
            continue
        if is_shared_h2h_streak:
            teams_value = "both"
        elif item_team_id == home_team_id:
            teams_value = "home"
        elif item_team_id == away_team_id:
            teams_value = "away"
        elif teams_value not in {"home", "away", "both"}:
            teams_value = "both"
        total_value = item.get("total")
        total_int = _safe_int(total_value) if total_value is not None else None
        streak_count = _safe_int(item.get("streak_length") if "streak_length" in item else item.get("count"))
        mapped_h2h_streaks.append(
            {
                "event": _event_label_for_fixture(raw_event_name, surface="fixture-h2h"),
                "count": streak_count,
                "total": total_int,
                "teams": teams_value,
            }
        )
    mapped_h2h_streaks = [item for item in mapped_h2h_streaks if _is_eligible_h2h_streak(item)]
    mapped_h2h_streaks = _dedupe_h2h_streaks(mapped_h2h_streaks)[:8]

    home_team_meta = team_meta_by_id.get(home_team_id, {})
    venue_row = None
    home_venue_id = home_team_meta.get("venue_id")
    if home_venue_id:
        try:
            venue_row = (
                db.execute(
                    text(
                        """
                        select venue_name, venue_city, venue_capacity, logo_file_id::text as logo_file_id
                        from dim.dim_venue
                        where venue_id = :venue_id
                        limit 1
                        """
                    ),
                    {"venue_id": _safe_int(home_venue_id)},
                )
                .mappings()
                .first()
            )
        except SQLAlchemyError:
            db.rollback()
            venue_row = None

    home_stats_row = team_stats_by_id.get(home_team_id, {})
    away_stats_row = team_stats_by_id.get(away_team_id, {})
    home_games_played = home_stats_row.get("games_played")
    away_games_played = away_stats_row.get("games_played")
    matchday = None
    if home_games_played is not None and away_games_played is not None:
        matchday = max(_safe_int(home_games_played), _safe_int(away_games_played))

    fixture_slug = f"{home_team_payload['slug']}-{away_team_payload['slug']}-{fixture_id}"

    return {
        "data": {
            "fixture": {
                "fixture_id": str(fixture_id),
                "slug": fixture_slug,
            },
            "league": {
                "league_season_id": str(league_season_id),
                "name": str(league_row.get("league_name") if league_row else fixture_row.get("league_name") or ""),
                "logo_url": _logo_asset_url(request, league_row.get("league_logo_file_id") if league_row else None),
                "country": str(league_row.get("country_name") if league_row else ""),
                "country_logo_url": _logo_asset_url(request, league_row.get("country_logo_file_id") if league_row else None),
                "matchday": matchday,
            },
            "kickoff": {"iso": _to_iso(fixture_row.get("kickoff_utc"))},
            "venue": {
                "name": str(venue_row.get("venue_name") if venue_row else "Unknown venue"),
                "city": str(venue_row.get("venue_city") if venue_row else "Unknown city"),
                "capacity": _safe_int(venue_row.get("venue_capacity")) if venue_row and venue_row.get("venue_capacity") is not None else None,
                "image_url": None,
            },
            "home_team": home_team_payload,
            "away_team": away_team_payload,
            "home_streaks": fixture_streaks_by_team.get(home_team_id, []),
            "away_streaks": fixture_streaks_by_team.get(away_team_id, []),
            "h2h_stats": {
                "total_matches": _safe_int(h2h_info.get("totalMatches")),
                "home_wins": _safe_int(h2h_info.get("homeWins")),
                "draws": _safe_int(h2h_info.get("draws")),
                "away_wins": _safe_int(h2h_info.get("awayWins")),
                "recent_matches": h2h_matches,
                "streaks": mapped_h2h_streaks,
            },
            "home_team_form": team_forms_by_id.get(home_team_id, {}).get("overall", []),
            "away_team_form": team_forms_by_id.get(away_team_id, {}).get("overall", []),
        },
        "meta": {
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
        },
    }


@app.get("/", response_model=List[Streak])
def get_streaks(request: Request, db: Session = Depends(get_db)) -> List[Streak]:
    try:
        rows = (
            db.query(DBStreak)
            .order_by(DBStreak.current_streak_length.desc(), DBStreak.team_name.asc())
            .all()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    return _serialize_db_streak_rows(request, db, rows)


def _serialize_db_streak_rows(request: Request, db: Session, rows: List[DBStreak]) -> List[Streak]:
    team_info_rows_by_id: Dict[int, List[Any]] = {}
    nav_by_league_season_id: Dict[int, Any] = {}
    form_score_map: Dict[tuple[int, int, str, str], float] = {}
    recent_form_by_scope_map: Dict[int, Dict[str, List[str]]] = {}
    opponent_tier_map: Dict[tuple[int, int], int] = {}
    try:
        team_ids = sorted({_safe_int(getattr(row, "team_id", 0)) for row in rows if _safe_int(getattr(row, "team_id", 0)) > 0})
        opponent_ids = sorted(
            {
                _safe_int(getattr(row, "next_opponent_id", 0))
                for row in rows
                if _safe_int(getattr(row, "next_opponent_id", 0)) > 0
            }
        )
        row_league_season_ids = sorted(
            {
                _safe_int(getattr(row, "league_season_id", 0), default=0)
                for row in rows
                if _safe_int(getattr(row, "league_season_id", 0), default=0) > 0
            }
        )
        team_lookup_ids = sorted(set(team_ids + opponent_ids))

        if team_lookup_ids:
            team_info_rows = db.query(DBTeamInfo).filter(DBTeamInfo.team_id.in_(team_lookup_ids)).all()
            for team_info_row in team_info_rows:
                team_id = _safe_int(getattr(team_info_row, "team_id", 0))
                if team_id <= 0:
                    continue
                team_info_rows_by_id.setdefault(team_id, []).append(team_info_row)

        team_info_league_season_ids = sorted(
            {
                _safe_int(getattr(team_info_row, "league_season_id", 0), default=0)
                for team_info_rows in team_info_rows_by_id.values()
                for team_info_row in team_info_rows
                if _safe_int(getattr(team_info_row, "league_season_id", 0), default=0) > 0
            }
        )
        league_season_ids = sorted(set(row_league_season_ids + team_info_league_season_ids))
        if league_season_ids:
            nav_rows = db.query(DBNavLeague).filter(DBNavLeague.league_season_id.in_(league_season_ids)).all()
            nav_by_league_season_id = {
                _safe_int(getattr(nav_row, "league_season_id", 0)): nav_row
                for nav_row in nav_rows
                if _safe_int(getattr(nav_row, "league_season_id", 0)) > 0
            }
            form_score_map = _load_team_form_score_map(db, team_ids=team_ids, league_season_ids=league_season_ids)
            opponent_tier_map = _load_opponent_tier_map(db, league_season_ids=league_season_ids)

        recent_form_by_scope_map = _load_recent_form_by_scope_map(db, team_ids=team_ids, limit=5)
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    serialized: List[Streak] = []
    for row in rows:
        team_id = _safe_int(getattr(row, "team_id", 0))
        streak_type = _normalize_streak_type(str(row.streak_type))
        if streak_type is None:
            continue
        metric_key = _normalize_metric_key(row.event_name)
        headline = get_event_headline(
            metric_key,
            mode="streak",
            streak_length=row.current_streak_length,
            surface="explore",
        )
        team_info = _select_best_team_info_row(
            team_info_rows_by_id.get(team_id, []),
            preferred_fixture_id=_safe_int(getattr(row, "next_fixture_id", None), default=0) or None,
            preferred_opponent_id=_safe_int(getattr(row, "next_opponent_id", None), default=0) or None,
        )
        resolved_next_kickoff = _coerce_utc_datetime(getattr(row, "next_kickoff_utc", None))
        if resolved_next_kickoff is None and team_info is not None:
            resolved_next_kickoff = _coerce_utc_datetime(getattr(team_info, "next_kickoff_utc", None))
        if _is_past_or_current_kickoff(resolved_next_kickoff):
            continue
        row_league_season_id = _safe_int(getattr(row, "league_season_id", 0), default=0)
        league_season_id = _safe_int(getattr(team_info, "league_season_id", 0), default=0) if team_info else 0
        if league_season_id <= 0:
            league_season_id = row_league_season_id
        nav_row = nav_by_league_season_id.get(league_season_id) if league_season_id > 0 else None
        league_name = _clean_text(getattr(row, "league_name", None)) or ""
        next_is_home_raw = getattr(team_info, "next_is_home", None) if team_info else None
        next_is_home = bool(next_is_home_raw) if next_is_home_raw is not None else None
        team_logo_file_id = _clean_text(getattr(team_info, "logo_file_id", None))
        league_logo_file_id = _clean_text(getattr(nav_row, "league_logo_file_id", None))
        country_logo_file_id = _clean_text(getattr(nav_row, "country_logo_file_id", None))
        normalized_context_scope = _normalize_context_scope(getattr(row, "context_scope", None))
        if not _scope_matches_next_fixture_venue(normalized_context_scope, next_is_home):
            continue
        confidence_score = _lookup_confidence_score(
            form_score_map,
            league_season_id=league_season_id,
            team_id=team_id,
            metric_key=metric_key,
            context_scope=normalized_context_scope,
        )
        forms_by_scope = recent_form_by_scope_map.get(team_id, {})
        form_scope = normalized_context_scope if normalized_context_scope in {"home", "away"} else "overall"
        scoped_recent_form = forms_by_scope.get(form_scope) or forms_by_scope.get("overall")
        recent_form = _sanitize_recent_form_for_streak(
            metric_key=metric_key,
            streak_length=row.current_streak_length,
            recent_form=scoped_recent_form,
        )
        opponent_id = _safe_int(getattr(row, "next_opponent_id", None), default=0)
        opponent_info = (
            _select_best_team_info_row(team_info_rows_by_id.get(opponent_id, []))
            if opponent_id > 0
            else None
        )
        opponent_league_season_id = _safe_int(getattr(opponent_info, "league_season_id", 0), default=0)
        if opponent_league_season_id <= 0:
            opponent_league_season_id = league_season_id
        opponent_tier = (
            opponent_tier_map.get((opponent_id, opponent_league_season_id))
            if opponent_id > 0 and opponent_league_season_id > 0
            else None
        )
        team_tier = (
            opponent_tier_map.get((team_id, league_season_id))
            if team_id > 0 and league_season_id > 0
            else None
        )
        serialized.append(
            Streak(
                streak_id=str(row.streak_id),
                team_id=team_id,
                event_name=row.event_name,
                metric_key=metric_key,
                headline=headline,
                streak_type=streak_type,
                current_streak_length=row.current_streak_length,
                team_name=row.team_name,
                league_name=league_name,
                next_kickoff_utc=_to_iso(resolved_next_kickoff),
                next_opponent_name=row.next_opponent_name,
                next_opponent_id=row.next_opponent_id,
                next_fixture_id=row.next_fixture_id,
                next_is_home=next_is_home,
                context_scope=normalized_context_scope,
                team_logo_url=_logo_asset_url(request, team_logo_file_id),
                league_logo_url=_logo_asset_url(request, league_logo_file_id),
                country_logo_url=_logo_asset_url(request, country_logo_file_id),
                team_logo_file_id=team_logo_file_id,
                league_logo_file_id=league_logo_file_id,
                country_name=_clean_text(getattr(nav_row, "country_name", None)),
                country_logo_file_id=country_logo_file_id,
                recent_form=recent_form,
                confidence_score=confidence_score,
                team_tier=team_tier,
                opponent_tier=opponent_tier,
            )
        )
    return serialized


@app.get("/teams/stats", response_model=FixtureAnalysisResponse)
def get_team_stats(team_id: int = Query(...), db: Session = Depends(get_db)):
    recent_form_map: Dict[int, List[str]] = {}
    try:
        latest_team = (
            db.query(DBTeamStat)
            .filter(DBTeamStat.team_id == team_id)
            .order_by(DBTeamStat.kickoff_utc.desc())
            .first()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if latest_team is None:
        raise HTTPException(status_code=404, detail="No stats found for provided team identifier")

    form_team_ids = [team_id]
    if latest_team.next_opponent_id:
        form_team_ids.append(_safe_int(latest_team.next_opponent_id, default=0))

    try:
        scope_by_team = {tid: "overall" for tid in form_team_ids if tid > 0}
        recent_form_map = _load_recent_form_map(db, team_ids=form_team_ids, scope_by_team=scope_by_team)
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    team_context = None
    try:
        team_context = db.query(DBTeamPageContext).filter(DBTeamPageContext.team_id == team_id).first()
    except SQLAlchemyError:
        db.rollback()
        team_context = None

    team_streaks = _serialize_context_streaks(
        team_context.streaks_json if team_context else None,
        default_team_id=team_id,
        default_team_name=latest_team.team_name,
    )
    if not team_streaks:
        team_streak_rows = (
            db.query(DBTeamStreakRecord)
            .filter(DBTeamStreakRecord.team_id == team_id)
            .order_by(DBTeamStreakRecord.streak_length.desc(), DBTeamStreakRecord.event_name.asc())
            .all()
        )
        team_streaks = _serialize_team_streaks(team_streak_rows)

    opponent_summary = None
    opponent_streaks: List[TeamStreakRecord] = []
    if latest_team.next_opponent_id:
        opponent = (
            db.query(DBTeamStat)
            .filter(DBTeamStat.team_id == latest_team.next_opponent_id)
            .order_by(DBTeamStat.kickoff_utc.desc())
            .first()
        )
        if opponent:
            opponent_summary = _serialize_team_summary(
                opponent,
                recent_form=recent_form_map.get(_safe_int(opponent.team_id, default=0), []),
            )
            opponent_context = None
            try:
                opponent_context = (
                    db.query(DBTeamPageContext)
                    .filter(DBTeamPageContext.team_id == latest_team.next_opponent_id)
                    .first()
                )
            except SQLAlchemyError:
                db.rollback()
                opponent_context = None

            opponent_streaks = _serialize_context_streaks(
                opponent_context.streaks_json if opponent_context else None,
                default_team_id=latest_team.next_opponent_id,
                default_team_name=opponent.team_name,
            )
            if not opponent_streaks:
                opponent_streak_rows = (
                    db.query(DBTeamStreakRecord)
                    .filter(DBTeamStreakRecord.team_id == latest_team.next_opponent_id)
                    .order_by(DBTeamStreakRecord.streak_length.desc(), DBTeamStreakRecord.event_name.asc())
                    .all()
                )
                opponent_streaks = _serialize_team_streaks(opponent_streak_rows)

    return FixtureAnalysisResponse(
        team=_serialize_team_summary(
            latest_team,
            recent_form=recent_form_map.get(_safe_int(latest_team.team_id, default=0), []),
        ),
        opponent=opponent_summary,
        team_streaks=team_streaks,
        opponent_streaks=opponent_streaks,
    )


@app.get("/pages/team-context", response_model=TeamPageContextResponse)
def get_team_page_context(team_id: int = Query(...), db: Session = Depends(get_db)):
    try:
        record = db.query(DBTeamPageContext).filter(DBTeamPageContext.team_id == team_id).first()
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if record is None:
        raise HTTPException(status_code=404, detail="Team page context not found")

    return TeamPageContextResponse(
        data=_serialize_team_page_context(record),
        meta={
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "source": "raw_mart.mart_team_page_context",
        },
    )


@app.get("/teams/page-data")
def get_team_page_data(
    request: Request,
    team_slug: str = Query(...),
    db: Session = Depends(get_db),
):
    raw_slug = str(team_slug or "").strip()
    if not raw_slug:
        raise HTTPException(status_code=400, detail="team_slug is required")

    candidate_team_id = _extract_team_id_from_slug(raw_slug)

    def _team_base_query_by_id(team_id_value: int):
        return (
            db.execute(
                text(
                    """
                    select
                        t.team_id,
                        t.team_name,
                        t.team_slug,
                        t.rank,
                        t.form,
                        t.league_season_id,
                        t.next_fixture_id,
                        t.next_opponent_id,
                        t.next_opponent_name,
                        t.next_is_home,
                        t.next_kickoff_utc,
                        t.logo_file_id::text as team_logo_file_id,
                        d.logo_file_id::text as dim_logo_file_id
                    from raw_mart.mart_team_info t
                    left join dim.dim_teams d
                      on d.team_id = t.team_id
                    left join raw_int.int_league_context lc
                      on lc.league_season_id = t.league_season_id
                    where t.team_id = :team_id
                    order by
                        case when t.next_kickoff_utc > now() then 0 else 1 end,
                        case when coalesce(lc.is_current, false) then 0 else 1 end,
                        lc.season desc nulls last,
                        t.league_season_id desc
                    limit 1
                    """
                ),
                {"team_id": team_id_value},
            )
            .mappings()
            .first()
        )

    def _team_base_query_by_slug(team_slug_value: str):
        return (
            db.execute(
                text(
                    """
                    select
                        t.team_id,
                        t.team_name,
                        t.team_slug,
                        t.rank,
                        t.form,
                        t.league_season_id,
                        t.next_fixture_id,
                        t.next_opponent_id,
                        t.next_opponent_name,
                        t.next_is_home,
                        t.next_kickoff_utc,
                        t.logo_file_id::text as team_logo_file_id,
                        d.logo_file_id::text as dim_logo_file_id
                    from raw_mart.mart_team_info t
                    left join dim.dim_teams d
                      on d.team_id = t.team_id
                    left join raw_int.int_league_context lc
                      on lc.league_season_id = t.league_season_id
                    where lower(t.team_slug) = lower(:team_slug)
                    order by
                        case when t.next_kickoff_utc > now() then 0 else 1 end,
                        case when coalesce(lc.is_current, false) then 0 else 1 end,
                        lc.season desc nulls last,
                        t.league_season_id desc
                    limit 1
                    """
                ),
                {"team_slug": team_slug_value},
            )
            .mappings()
            .first()
        )

    try:
        team_row = _team_base_query_by_id(candidate_team_id) if candidate_team_id else None
        if team_row is None:
            team_row = _team_base_query_by_slug(raw_slug)
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if team_row is None:
        raise HTTPException(status_code=404, detail="Team page data not found")

    team_id = _safe_int(team_row.get("team_id"))
    league_season_id = _safe_int(team_row.get("league_season_id"))
    canonical_slug = _canonical_team_slug(team_row.get("team_name"), team_id, team_row.get("team_slug"))

    try:
        league_row = (
            db.execute(
                text(
                    """
                    select
                        m.league_season_id,
                        m.league_name,
                        m.league_slug,
                        m.season_label,
                        m.country_name,
                        i.league_logo_file_id::text as league_logo_file_id,
                        i.country_logo_file_id::text as country_logo_file_id
                    from raw_mart.mart_league_info m
                    left join raw_int.int_league_context i
                      on i.league_season_id = m.league_season_id
                    where m.league_season_id = :league_season_id
                    limit 1
                    """
                ),
                {"league_season_id": league_season_id},
            )
            .mappings()
            .first()
        )

        peer_rows = (
            db.execute(
                text(
                    """
                    select
                        team_id,
                        team_name,
                        team_slug,
                        logo_file_id::text as logo_file_id,
                        team_rank
                    from raw_mart.mart_league_teams
                    where league_season_id = :league_season_id
                    order by team_rank nulls last, team_name asc
                    """
                ),
                {"league_season_id": league_season_id},
            )
            .mappings()
            .all()
        )

        streak_rows = (
            db.execute(
                text(
                    """
                    select
                        streak_id,
                        team_id,
                        event_name,
                        streak_type,
                        streak_length,
                        polarity,
                        scope,
                        coalesce(is_streak_eligible, false) as is_streak_eligible
                    from raw_mart.mart_team_streaks
                    where team_id = :team_id
                      and league_season_id = :league_season_id
                    order by
                        coalesce(is_streak_eligible, false) desc,
                        streak_length desc,
                        event_name asc
                    """
                ),
                {"team_id": team_id, "league_season_id": league_season_id},
            )
            .mappings()
            .all()
        )

        opponent_row = None
        next_opponent_id = team_row.get("next_opponent_id")
        if next_opponent_id is not None:
            opponent_row = (
                db.execute(
                    text(
                        """
                        select
                            d.team_id,
                            coalesce(m.team_name, d.team_name) as team_name,
                            m.team_slug,
                            m.rank,
                            coalesce(m.logo_file_id::text, d.logo_file_id::text) as logo_file_id
                        from dim.dim_teams d
                        left join raw_mart.mart_team_info m
                          on m.team_id = d.team_id
                        where d.team_id = :team_id
                        limit 1
                        """
                    ),
                    {"team_id": _safe_int(next_opponent_id)},
                )
                .mappings()
                .first()
            )

        venue_name = None
        next_fixture_id = team_row.get("next_fixture_id")
        if next_fixture_id is not None:
            fixture_row = (
                db.execute(
                    text(
                        """
                        select home_team_id, away_team_id
                        from raw_int.int_fixtures
                        where fixture_id = :fixture_id
                        limit 1
                        """
                    ),
                    {"fixture_id": _safe_int(next_fixture_id)},
                )
                .mappings()
                .first()
            )
            if fixture_row:
                venue_team_id = _safe_int(fixture_row.get("home_team_id"))
                venue_row = (
                    db.execute(
                        text(
                            """
                            select v.venue_name
                            from dim.dim_teams t
                            join dim.dim_venue v on v.venue_id = t.venue_id
                            where t.team_id = :team_id
                            limit 1
                            """
                        ),
                        {"team_id": venue_team_id},
                    )
                    .mappings()
                    .first()
                )
                if venue_row:
                    venue_name = str(venue_row.get("venue_name") or "").strip() or None
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    league_teams: List[Dict[str, Any]] = []
    for row in peer_rows:
        peer_id = _safe_int(row.get("team_id"))
        peer_name = str(row.get("team_name") or "").strip()
        if not peer_name:
            continue
        league_teams.append(
            {
                "team_id": str(peer_id),
                "name": peer_name,
                "slug": _canonical_team_slug(peer_name, peer_id, row.get("team_slug")),
            }
        )

    mapped_streaks: List[Dict[str, Any]] = []
    for row in streak_rows:
        streak_type = _normalize_streak_type_for_api(row.get("streak_type"))
        if streak_type is None:
            continue
        streak_length = _safe_int(row.get("streak_length"))
        metric_key = _normalize_metric_key(row.get("event_name"))
        mapped_streaks.append(
            {
                "streak_id": str(row.get("streak_id") or f"{team_id}-{metric_key}-{streak_length}"),
                "team_id": str(team_id),
                "streak_type": streak_type,
                "polarity": _normalize_polarity(row.get("polarity")),
                "scope": _normalize_context_scope(row.get("scope")),
                "metric_key": metric_key,
                "length": streak_length,
                "headline": get_event_headline(
                    metric_key,
                    mode="streak",
                    streak_length=streak_length,
                    surface="team-page",
                ),
            }
        )

    primary_streaks = mapped_streaks[:4]
    secondary_streaks = mapped_streaks[4:6]

    form_values = [value for value in _normalize_form(team_row.get("form")) if value in {"W", "D", "L"}][:5]

    upcoming_match = None
    if next_fixture_id is not None and opponent_row:
        opponent_id = _safe_int(opponent_row.get("team_id"))
        opponent_name = str(
            opponent_row.get("team_name") or team_row.get("next_opponent_name") or "Opponent"
        ).strip()
        upcoming_match = {
            "match_id": str(_safe_int(next_fixture_id)),
            "kickoff_iso": _to_iso(team_row.get("next_kickoff_utc")),
            "is_home": bool(team_row.get("next_is_home")),
            "opponent": {
                "team_id": str(opponent_id),
                "name": opponent_name,
                "slug": _canonical_team_slug(opponent_name, opponent_id, opponent_row.get("team_slug")),
                "crest_url": _logo_asset_url(request, opponent_row.get("logo_file_id")),
                "rank": _safe_int(opponent_row.get("rank")) if opponent_row.get("rank") is not None else None,
            },
            "venue": venue_name,
        }

    return {
        "data": {
            "team": {
                "team_id": str(team_id),
                "name": str(team_row.get("team_name") or ""),
                "slug": canonical_slug,
                "crest_url": _logo_asset_url(
                    request,
                    team_row.get("team_logo_file_id") or team_row.get("dim_logo_file_id"),
                ),
                "rank": _safe_int(team_row.get("rank")) if team_row.get("rank") is not None else None,
            },
            "context": {
                "league_season_id": str(league_season_id),
                "league_name": str(league_row.get("league_name") if league_row else ""),
                "season_label": str(league_row.get("season_label") if league_row else ""),
                "country_name": str(league_row.get("country_name") if league_row else "") or None,
                "league_teams": league_teams,
            },
            "form": {
                "last_n": form_values,
            },
            "current_streaks": {
                "primary": primary_streaks,
                **({"secondary": secondary_streaks} if secondary_streaks else {}),
            },
            **({"upcoming_match": upcoming_match} if upcoming_match else {}),
            "ui_hints": {
                "show_upcoming_match": bool(upcoming_match),
                "show_secondary_streak_row": len(secondary_streaks) >= 2,
            },
        },
        "meta": {
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
        },
    }


@app.get("/fixtures/analysis", response_model=FixturePageContextResponse)
@app.get("/pages/fixture-context", response_model=FixturePageContextResponse)
def get_fixture_page_context(fixture_id: int = Query(...), db: Session = Depends(get_db)):
    try:
        record = db.query(DBFixturePageContext).filter(DBFixturePageContext.fixture_id == fixture_id).first()
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if record is None:
        raise HTTPException(status_code=404, detail="Fixture page context not found")

    return FixturePageContextResponse(
        data=_serialize_fixture_page_context(record),
        meta={
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "source": "raw_mart.mart_fixture_page_context",
        },
    )


@app.get("/home/streaks")
def get_home_streaks(request: Request, db: Session = Depends(get_db)):
    team_info_rows_by_id: Dict[int, List[Any]] = {}
    form_score_map: Dict[tuple[int, int, str, str], float] = {}
    recent_form_by_scope_map: Dict[int, Dict[str, List[str]]] = {}
    opponent_tier_map: Dict[tuple[int, int], int] = {}
    try:
        rows = (
            db.query(DBHomepageStreak)
            .order_by(
                DBHomepageStreak.relevance_score.desc(),
                DBHomepageStreak.streak_length.desc(),
                DBHomepageStreak.team_name.asc(),
            )
            .limit(HOME_STREAKS_LIMIT)
            .all()
        )

        team_ids = sorted(
            {
                _safe_int(getattr(row, "team_id", 0))
                for row in rows
                if _safe_int(getattr(row, "team_id", 0)) > 0
            }
        )
        opponent_ids = sorted(
            {
                _safe_int(getattr(row, "next_opponent_id", 0))
                for row in rows
                if _safe_int(getattr(row, "next_opponent_id", 0)) > 0
            }
        )
        team_lookup_ids = sorted(set(team_ids + opponent_ids))
        if team_lookup_ids:
            team_info_rows = db.query(DBTeamInfo).filter(DBTeamInfo.team_id.in_(team_lookup_ids)).all()
            for team_info_row in team_info_rows:
                team_id = _safe_int(getattr(team_info_row, "team_id", 0))
                if team_id <= 0:
                    continue
                team_info_rows_by_id.setdefault(team_id, []).append(team_info_row)

        league_season_ids = sorted(
            {
                _safe_int(getattr(row, "league_season_id", 0), default=0)
                for row in rows
                if _safe_int(getattr(row, "league_season_id", 0), default=0) > 0
            }
        )
        if not league_season_ids:
            league_season_ids = sorted(
                {
                    _safe_int(
                        getattr(_select_best_team_info_row(team_info_rows_by_id.get(team_id, [])), "league_season_id", 0),
                        default=0,
                    )
                    for team_id in team_ids
                    if _safe_int(
                        getattr(_select_best_team_info_row(team_info_rows_by_id.get(team_id, [])), "league_season_id", 0),
                        default=0,
                    ) > 0
                }
            )

        if league_season_ids:
            form_score_map = _load_team_form_score_map(db, team_ids=team_ids, league_season_ids=league_season_ids)
            opponent_tier_map = _load_opponent_tier_map(db, league_season_ids=league_season_ids)
        recent_form_by_scope_map = _load_recent_form_by_scope_map(db, team_ids=team_ids, limit=5)
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    streaks: List[Streak] = []
    for row in rows:
        team_id = _safe_int(getattr(row, "team_id", 0), default=0)
        streak_type = _normalize_streak_type(str(row.streak_type))
        if streak_type is None:
            continue
        metric_key = _normalize_metric_key(row.event_name)
        headline = get_event_headline(
            metric_key,
            mode="streak",
            streak_length=row.streak_length,
            surface="home",
        )
        team_info = _select_best_team_info_row(
            team_info_rows_by_id.get(team_id, []),
            preferred_fixture_id=_safe_int(getattr(row, "next_fixture_id", None), default=0) or None,
            preferred_opponent_id=_safe_int(getattr(row, "next_opponent_id", None), default=0) or None,
        )
        league_season_id = _safe_int(getattr(row, "league_season_id", 0), default=0)
        if league_season_id <= 0:
            league_season_id = _safe_int(getattr(team_info, "league_season_id", 0), default=0)
        next_is_home_raw = getattr(team_info, "next_is_home", None) if team_info else None
        next_is_home = bool(next_is_home_raw) if next_is_home_raw is not None else None
        team_logo_file_id = str(row.team_logo_file_id) if row.team_logo_file_id else None
        league_logo_file_id = str(row.league_logo_file_id) if row.league_logo_file_id else None
        country_logo_file_id = str(row.country_logo_file_id) if row.country_logo_file_id else None
        normalized_context_scope = _normalize_context_scope(getattr(row, "context_scope", None))
        if not _scope_matches_next_fixture_venue(normalized_context_scope, next_is_home):
            continue
        confidence_score = _lookup_confidence_score(
            form_score_map,
            league_season_id=league_season_id,
            team_id=team_id,
            metric_key=metric_key,
            context_scope=normalized_context_scope,
        )
        forms_by_scope = recent_form_by_scope_map.get(team_id, {})
        form_scope = normalized_context_scope if normalized_context_scope in {"home", "away"} else "overall"
        scoped_recent_form = forms_by_scope.get(form_scope) or forms_by_scope.get("overall")
        recent_form = _sanitize_recent_form_for_streak(
            metric_key=metric_key,
            streak_length=row.streak_length,
            recent_form=scoped_recent_form,
        )
        opponent_id = _safe_int(getattr(row, "next_opponent_id", None), default=0)
        opponent_info = (
            _select_best_team_info_row(team_info_rows_by_id.get(opponent_id, []))
            if opponent_id > 0
            else None
        )
        opponent_league_season_id = _safe_int(getattr(opponent_info, "league_season_id", 0), default=0)
        if opponent_league_season_id <= 0:
            opponent_league_season_id = league_season_id
        opponent_tier = (
            opponent_tier_map.get((opponent_id, opponent_league_season_id))
            if opponent_id > 0 and opponent_league_season_id > 0
            else None
        )
        team_tier = (
            opponent_tier_map.get((team_id, league_season_id))
            if team_id > 0 and league_season_id > 0
            else None
        )
        streaks.append(
            Streak(
                streak_id=str(row.streak_id),
                team_id=team_id,
                event_name=row.event_name,
                metric_key=metric_key,
                headline=headline,
                streak_type=streak_type,
                current_streak_length=row.streak_length,
                team_name=row.team_name,
                league_name=row.league_name,
                next_kickoff_utc=_to_iso(row.next_kickoff_utc),
                next_opponent_name=row.next_opponent_name,
                next_opponent_id=row.next_opponent_id,
                next_fixture_id=row.next_fixture_id,
                next_is_home=next_is_home,
                context_scope=normalized_context_scope,
                team_logo_url=_logo_asset_url(request, team_logo_file_id),
                league_logo_url=_logo_asset_url(request, league_logo_file_id),
                country_logo_url=_logo_asset_url(request, country_logo_file_id),
                team_logo_file_id=team_logo_file_id,
                league_logo_file_id=league_logo_file_id,
                country_name=row.country_name,
                country_logo_file_id=country_logo_file_id,
                recent_form=recent_form,
                confidence_score=confidence_score,
                team_tier=team_tier,
                opponent_tier=opponent_tier,
            )
        )

    return {
        "data": {"streaks": streaks},
        "meta": {
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "limit": HOME_STREAKS_LIMIT,
        },
    }


@app.get("/home/stats", response_model=HomeStatsResponse)
def get_home_stats(request: Request, db: Session = Depends(get_db)):
    active_streaks = len(get_streaks(request=request, db=db))
    try:
        row = (
            db.execute(
                text(
                    """
                    SELECT
                      COALESCE((SELECT COUNT(DISTINCT team_id) FROM raw_mart.mart_team_info), 0)::int AS teams_tracked,
                      COALESCE(
                        (
                          SELECT COUNT(*) FROM (
                            SELECT
                              COALESCE(NULLIF(LOWER(li.country_name), ''), 'unknown_country') AS country_key,
                              COALESCE(NULLIF(LOWER(li.league_name), ''), 'unknown_league') AS league_key
                            FROM raw_mart.mart_top_team_streaks s
                            JOIN raw_mart.mart_team_info ti ON ti.team_id = s.team_id
                            LEFT JOIN raw_mart.mart_league_info li ON li.league_season_id = ti.league_season_id
                            GROUP BY 1, 2
                          ) deduped_leagues
                        ),
                        0
                      )::int AS leagues
                    """
                )
            )
            .mappings()
            .first()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    teams_tracked = _safe_int(row.get("teams_tracked") if row else 0, default=0)
    leagues = _safe_int(row.get("leagues") if row else 0, default=0)

    return {
        "data": {
            "active_streaks": active_streaks,
            "teams_tracked": teams_tracked,
            "leagues": leagues,
        },
        "meta": {
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "source": "raw_mart.mart_top_team_streaks+raw_mart.mart_team_info+raw_mart.mart_league_info",
        },
    }


@app.get("/explore/streaks")
def get_explore_streaks_stub(
    request: Request,
    db: Session = Depends(get_db),
    cursor: Optional[str] = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=50),
    restore_limit: Optional[int] = Query(default=None, alias="restoreLimit", ge=1),
    sort: Literal["default", "match_date_asc"] = Query(default="default"),
):
    start_idx = _parse_cursor_index(cursor, fallback_offset=offset)
    effective_limit = limit
    if restore_limit is not None:
        effective_limit = min(restore_limit, EXPLORE_STREAKS_RESTORE_LIMIT_MAX)
    all_streaks, cache_status = _load_cached_explore_streaks(request)
    cache_store_status = "skipped"
    if all_streaks is None:
        all_streaks = get_streaks(request=request, db=db)
        cache_store_status = _store_cached_explore_streaks(all_streaks)
        if cache_status == "miss" and cache_store_status == "stored":
            cache_status = "miss_stored"
    if sort == "match_date_asc":
        all_streaks = sorted(
            all_streaks,
            key=lambda item: (
                _coerce_utc_datetime(_streak_field_value(item, "next_kickoff_utc")) is None,
                _coerce_utc_datetime(_streak_field_value(item, "next_kickoff_utc")) or datetime.max.replace(tzinfo=timezone.utc),
                -_safe_int(_streak_field_value(item, "current_streak_length"), default=0),
                str(_streak_field_value(item, "team_name") or "").lower(),
            ),
        )
    page = all_streaks[start_idx : start_idx + effective_limit]
    total = len(all_streaks)
    next_idx = start_idx + len(page)
    has_more = next_idx < total
    next_cursor = str(next_idx) if has_more else None

    def _normalized_type(raw_value: Any) -> Optional[str]:
        raw = str(raw_value or "").strip().lower()
        if raw in {"goal", "goal_streaks"}:
            return "goal_streaks"
        if raw in {"result", "result_streaks"}:
            return "result_streaks"
        return None

    league_filters = sorted(
        {
            str(_streak_field_value(item, "league_name") or "").strip()
            for item in all_streaks
            if str(_streak_field_value(item, "league_name") or "").strip()
        }
    )
    league_option_counts: Dict[tuple[str, str], int] = {}
    league_option_type_counts: Dict[tuple[str, str], Dict[str, int]] = {}
    type_counts = {"all": 0, "goal_streaks": 0, "result_streaks": 0}
    for item in all_streaks:
        league_name = str(_streak_field_value(item, "league_name") or "").strip()
        if not league_name:
            continue
        country_name = str(_streak_field_value(item, "country_name") or "").strip()
        key = (league_name, country_name)
        league_option_counts[key] = league_option_counts.get(key, 0) + 1
        per_league = league_option_type_counts.get(key)
        if per_league is None:
            per_league = {"all": 0, "goal_streaks": 0, "result_streaks": 0}
            league_option_type_counts[key] = per_league
        per_league["all"] += 1
        normalized_type = _normalized_type(_streak_field_value(item, "streak_type"))
        type_counts["all"] += 1
        if normalized_type in {"goal_streaks", "result_streaks"}:
            type_counts[normalized_type] += 1
            per_league[normalized_type] += 1

    league_options: List[Dict[str, Any]] = []
    used_ids: set[str] = set()
    for (league_name, country_name), count in sorted(
        league_option_counts.items(),
        key=lambda entry: ((entry[0][1] or "").lower(), entry[0][0].lower()),
    ):
        base_id = f"{_slugify_text(country_name or 'unknown')}-{_slugify_text(league_name)}"
        option_id = base_id
        suffix = 2
        while option_id in used_ids:
            option_id = f"{base_id}-{suffix}"
            suffix += 1
        used_ids.add(option_id)
        league_type_counts = league_option_type_counts.get((league_name, country_name), {})
        league_options.append(
            {
                "id": option_id,
                "league_name": league_name,
                "country_name": country_name or None,
                "label": f"{league_name} ({country_name})" if country_name else league_name,
                "count": count,
                "goal_streaks_count": _safe_int(league_type_counts.get("goal_streaks"), default=0),
                "result_streaks_count": _safe_int(league_type_counts.get("result_streaks"), default=0),
            }
        )

    type_filters = sorted(
        {
            f"{str(_streak_field_value(item, 'streak_type') or '').strip()}_streaks"
            if str(_streak_field_value(item, "streak_type") or "").strip() in {"goal", "result"}
            else str(_streak_field_value(item, "streak_type") or "").strip()
            for item in all_streaks
            if str(_streak_field_value(item, "streak_type") or "").strip()
        }
    )

    return {
        "data": {
            "streaks": page,
            "filters": {
                "leagues": league_filters,
                "types": type_filters,
                "league_options": league_options,
                "type_counts": type_counts,
            },
        },
        "pagination": {
            "offset": start_idx,
            "limit": effective_limit,
            "cursor": cursor if cursor is not None else str(start_idx),
            "nextCursor": next_cursor,
            "total": total,
            "hasMore": has_more,
            "sort": sort,
        },
        "meta": {
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "stub": False,
            "sort": sort,
            "message": "Cursor pagination enabled",
            "cache": {
                "key": EXPLORE_STREAKS_CACHE_KEY,
                "status": cache_status,
                "store_status": cache_store_status,
                "ttl_seconds": EXPLORE_STREAKS_CACHE_TTL_SECONDS,
            },
        },
    }


@app.get("/navigation/leagues")
def get_navigation_leagues(request: Request, db: Session = Depends(get_db)):
    try:
        nav_rows = db.query(DBNavLeague).all()
        league_info_rows = db.query(DBLeagueInfo).all()
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    payload = _build_navigation_leagues_payload(request, nav_rows, league_info_rows)
    return payload.model_dump()


@app.get("/search/teams")
def search_teams(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(default=8, ge=1, le=20),
    db: Session = Depends(get_db),
):
    query = str(q or "").strip()
    if not query:
        return TeamSearchResponse(
            data={"suggestions": []},
            meta={
                "generatedAtIso": datetime.now(timezone.utc).isoformat(),
                "query": "",
                "limit": limit,
                "source": "raw_mart.mart_dim_search",
            },
        ).model_dump()

    query_lc = query.lower()
    prefix = f"{query_lc}%"
    contains = f"%{query_lc}%"

    try:
        rows = (
            db.execute(
                text(
                    """
                    select
                        s.entity_id::bigint as team_id,
                        s.name,
                        t.team_name,
                        t.team_slug,
                        s.logo_file_id::text as logo_file_id,
                        l.league_name,
                        c.country_logo_file_id::text as country_logo_file_id
                    from raw_mart.mart_dim_search s
                    left join raw_mart.mart_team_info t
                      on t.team_id = s.entity_id
                    left join raw_mart.mart_league_info l
                      on l.league_season_id = t.league_season_id
                    left join raw_int.int_league_context c
                      on c.league_season_id = t.league_season_id
                    where s.entity_type = 'team'
                      and s.name_lc like :contains
                    order by
                      case when s.name_lc like :prefix then 0 else 1 end,
                      position(:query_lc in s.name_lc),
                      char_length(s.name_lc),
                      s.name_lc asc
                    limit :limit
                    """
                ),
                {
                    "contains": contains,
                    "prefix": prefix,
                    "query_lc": query_lc,
                    "limit": limit,
                },
            )
            .mappings()
            .all()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    payload = _build_team_search_payload(request, rows, query, limit)
    return payload.model_dump()


@app.get("/leagues/page-data")
def get_league_page_data(
    request: Request,
    league_slug: str = Query(...),
    db: Session = Depends(get_db),
):
    raw_slug = str(league_slug or "").strip()
    if not raw_slug:
        raise HTTPException(status_code=400, detail="league_slug is required")

    try:
        league_row = (
            db.execute(
                text(
                    """
                    select
                        m.league_season_id,
                        m.league_name,
                        m.league_slug,
                        m.season_label,
                        m.country_name,
                        i.league_logo_file_id::text as league_logo_file_id,
                        i.country_logo_file_id::text as country_logo_file_id
                    from raw_mart.mart_league_info m
                    left join raw_int.int_league_context i
                      on i.league_season_id = m.league_season_id
                    where lower(m.league_slug) = lower(:league_slug)
                    limit 1
                    """
                ),
                {"league_slug": raw_slug},
            )
            .mappings()
            .first()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if league_row is None:
        raise HTTPException(status_code=404, detail="League page data not found")

    league_season_id = _safe_int(league_row.get("league_season_id"))

    try:
        team_rows = (
            db.execute(
                text(
                    """
                    select
                        t.team_id,
                        t.team_name,
                        t.team_slug,
                        t.logo_file_id::text as logo_file_id,
                        coalesce(s.rank, t.rank) as rank,
                        coalesce(s.points, t.points) as points,
                        s.games_played,
                        s.wins,
                        s.draws,
                        s.losses,
                        s.goals_for,
                        s.goals_against,
                        s.goal_diff,
                        coalesce(s.form, t.form) as form
                    from raw_mart.mart_team_info t
                    left join dim.dim_teams d
                      on d.team_id = t.team_id
                    left join raw.raw_league_standings s
                      on s.league_season_id = t.league_season_id
                     and s.api_team_id = d.api_team_id
                    where t.league_season_id = :league_season_id
                    order by coalesce(s.rank, t.rank) nulls last, coalesce(s.points, t.points) desc nulls last, t.team_name asc
                    """
                ),
                {"league_season_id": league_season_id},
            )
            .mappings()
            .all()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if not team_rows:
        try:
            team_rows = (
                db.execute(
                    text(
                        """
                        select
                            f.team_id,
                            f.team_name,
                            concat_ws(
                                '-',
                                lower(regexp_replace(f.team_name, '\\s+', '-', 'g')),
                                f.team_id
                            ) as team_slug,
                            d.logo_file_id::text as logo_file_id,
                            null::int as rank,
                            null::int as points,
                            null::int as games_played,
                            null::int as wins,
                            null::int as draws,
                            null::int as losses,
                            null::int as goals_for,
                            null::int as goals_against,
                            null::int as goal_diff,
                            null::text as form
                        from raw_int.fact_team_fixtures f
                        left join dim.dim_teams d
                          on d.team_id = f.team_id
                        where f.league_season_id = :league_season_id
                        group by
                            f.team_id,
                            f.team_name,
                            d.logo_file_id
                        order by f.team_name asc
                        """
                    ),
                    {"league_season_id": league_season_id},
                )
                .mappings()
                .all()
            )
        except SQLAlchemyError as exc:
            db.rollback()
            raise HTTPException(status_code=503, detail="Database unavailable") from exc

    try:
        league_streak_rows = (
            db.query(DBStreak)
            .filter(DBStreak.league_season_id == league_season_id)
            .order_by(DBStreak.current_streak_length.desc(), DBStreak.team_name.asc())
            .limit(6)
            .all()
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    standings: List[Dict[str, Any]] = []
    for row in team_rows:
        team_id = _safe_int(row.get("team_id"))
        team_name = str(row.get("team_name") or "").strip()
        if team_id <= 0 or not team_name:
            continue

        team_slug_value = _canonical_team_slug(team_name, team_id, row.get("team_slug"))
        standings.append(
            {
                "position": _safe_int(row.get("rank")) if row.get("rank") is not None else None,
                "team_id": str(team_id),
                "team_name": team_name,
                "team_slug": team_slug_value,
                "team_logo_url": _logo_asset_url(request, row.get("logo_file_id")),
                "points": _safe_int(row.get("points")) if row.get("points") is not None else None,
                "played": _safe_int(row.get("games_played")) if row.get("games_played") is not None else None,
                "wins": _safe_int(row.get("wins")) if row.get("wins") is not None else None,
                "draws": _safe_int(row.get("draws")) if row.get("draws") is not None else None,
                "losses": _safe_int(row.get("losses")) if row.get("losses") is not None else None,
                "goals_for": _safe_int(row.get("goals_for")) if row.get("goals_for") is not None else None,
                "goals_against": _safe_int(row.get("goals_against")) if row.get("goals_against") is not None else None,
                "goal_difference": _safe_int(row.get("goal_diff")) if row.get("goal_diff") is not None else None,
                "form": _parse_compact_form(row.get("form")),
            }
        )

    return {
        "data": {
            "league": {
                "league_season_id": str(league_season_id),
                "league_slug": str(league_row.get("league_slug") or raw_slug),
                "league_name": str(league_row.get("league_name") or ""),
                "season_label": str(league_row.get("season_label") or ""),
                "country_name": str(league_row.get("country_name") or "") or None,
                "league_logo_url": _logo_asset_url(request, league_row.get("league_logo_file_id")),
                "country_logo_url": _logo_asset_url(request, league_row.get("country_logo_file_id")),
            },
            "top_streaks": [item.model_dump() for item in _serialize_db_streak_rows(request, db, league_streak_rows)],
            "standings": {
                "overall": standings,
            },
            "available_contexts": ["overall"],
        },
        "meta": {
            "generatedAtIso": datetime.now(timezone.utc).isoformat(),
            "source": "raw_mart.mart_league_info+raw_mart.mart_team_info+raw.raw_league_standings+raw_mart.mart_top_team_streaks",
        },
    }
