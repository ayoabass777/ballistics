"""Utilities for event headline rendering across streak cards and form contexts."""

from typing import Any, Dict, Literal, Optional

from .logger import get_logger

HeadlineMode = Literal["streak", "form"]

HEADLINE_SCHEMA_VERSION = "headline_schema_v5"
LOGGER = get_logger("API.utils.headliner")

# event_name -> display metadata used by streak cards and form views.
EVENT_HEADLINES: Dict[str, Dict[str, Any]] = {
    "score_1goal": {
        "event_key": "score_1goal",
        "category": "goal",
        "event_title": "Scored (1+)",
        "display": {
            "streak": {
                "template": "Scored in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Scored in matches",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "score_2goals": {
        "event_key": "score_2goals",
        "category": "goal",
        "event_title": "Scored 2+ goals",
        "display": {
            "streak": {
                "template": "Scored 2+ goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Scored 2+ goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "concede_1": {
        "event_key": "concede_1",
        "category": "goal",
        "event_title": "Conceded (1+)",
        "display": {
            "streak": {
                "template": "Conceded in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Conceded in matches",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "concede_2": {
        "event_key": "concede_2",
        "category": "goal",
        "event_title": "Conceded 2+ goals",
        "display": {
            "streak": {
                "template": "Conceded 2+ goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Conceded 2+ goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "goalless": {
        "event_key": "goalless",
        "category": "goal",
        "event_title": "No goals scored",
        "display": {
            "streak": {
                "template": "Failed to score in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Failed to score",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Over_1_5": {
        "event_key": "Over_1_5",
        "category": "goal",
        "event_title": "Over 1.5 goals",
        "display": {
            "streak": {
                "template": "Over 1.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Over 1.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Over_2_5": {
        "event_key": "Over_2_5",
        "category": "goal",
        "event_title": "Over 2.5 goals",
        "display": {
            "streak": {
                "template": "Over 2.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Over 2.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Over_3_5": {
        "event_key": "Over_3_5",
        "category": "goal",
        "event_title": "Over 3.5 goals",
        "display": {
            "streak": {
                "template": "Over 3.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Over 3.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Over_4_5": {
        "event_key": "Over_4_5",
        "category": "goal",
        "event_title": "Over 4.5 goals",
        "display": {
            "streak": {
                "template": "Over 4.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Over 4.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Under_1_5": {
        "event_key": "Under_1_5",
        "category": "goal",
        "event_title": "Under 1.5 goals",
        "display": {
            "streak": {
                "template": "Under 1.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Under 1.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Under_2_5": {
        "event_key": "Under_2_5",
        "category": "goal",
        "event_title": "Under 2.5 goals",
        "display": {
            "streak": {
                "template": "Under 2.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Under 2.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Under_3_5": {
        "event_key": "Under_3_5",
        "category": "goal",
        "event_title": "Under 3.5 goals",
        "display": {
            "streak": {
                "template": "Under 3.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Under 3.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "Under_4_5": {
        "event_key": "Under_4_5",
        "category": "goal",
        "event_title": "Under 4.5 goals",
        "display": {
            "streak": {
                "template": "Under 4.5 goals in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Under 4.5 goals",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "clean_sheet": {
        "event_key": "clean_sheet",
        "category": "goal",
        "event_title": "Clean sheet",
        "display": {
            "streak": {
                "template": "Kept a clean sheet in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Clean sheets",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "win": {
        "event_key": "win",
        "category": "result",
        "event_title": "Win",
        "display": {
            "streak": {
                "template": "Won {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Wins",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "draw": {
        "event_key": "draw",
        "category": "result",
        "event_title": "Draw",
        "display": {
            "streak": {
                "template": "Drew {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Draws",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "loss": {
        "event_key": "loss",
        "category": "result",
        "event_title": "Loss",
        "display": {
            "streak": {
                "template": "Lost {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Losses",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "unbeaten": {
        "event_key": "unbeaten",
        "category": "result",
        "event_title": "Unbeaten",
        "display": {
            "streak": {
                "template": "Unbeaten in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Unbeaten matches",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "winless": {
        "event_key": "winless",
        "category": "result",
        "event_title": "Winless",
        "display": {
            "streak": {
                "template": "Winless in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Winless matches",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "halftime_win": {
        "event_key": "halftime_win",
        "category": "result",
        "event_title": "Half-time win",
        "display": {
            "streak": {
                "template": "Led at half-time in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Led at half-time",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
    "BTTS": {
        "event_key": "BTTS",
        "category": "goal",
        "event_title": "Both teams scored",
        "display": {
            "streak": {
                "template": "Both teams scored in {N} straight matches",
                "uses_N_in_headline": True,
            },
            "form": {
                "label": "Both teams scored",
                "uses_N_in_headline": False,
                "count_unit": "matches",
            },
        },
    },
}


def _humanize_event_name(event_name: str) -> str:
    return event_name.replace("_", " ").strip() or "Event"


def get_event_headline(
    event_name: str,
    mode: HeadlineMode,
    streak_length: Optional[int] = None,
    surface: Optional[str] = None,
) -> str:
    """
    Return display text for an event in either streak or form mode.

    - streak mode: may include {N} if a streak_length is provided.
    - form mode: returns label only (count is rendered separately by UI).
    """
    if mode not in ("streak", "form"):
        LOGGER.warning(
            "Unknown headline mode provided; defaulting to streak mode mode=%s event_name=%s surface=%s",
            mode,
            event_name,
            surface or "unspecified",
        )

    event_meta = EVENT_HEADLINES.get(event_name)

    if event_meta:
        LOGGER.debug(
            "Headline mapping hit event_name=%s mode=%s streak_length=%s surface=%s",
            event_name,
            mode,
            streak_length,
            surface or "unspecified",
        )
        display = event_meta["display"]
        if mode == "form":
            return display["form"]["label"]

        template = display["streak"]["template"]
        if streak_length is None:
            LOGGER.info(
                "Streak headline requested without streak_length event_name=%s mode=%s surface=%s uses_N_in_headline=%s",
                event_name,
                mode,
                surface or "unspecified",
                display["streak"].get("uses_N_in_headline", False),
            )
            return template
        return template.replace("{N}", str(streak_length))

    # Fallback keeps event clarity even for unmapped values.
    LOGGER.warning(
        "Headline mapping miss event_name=%s mode=%s streak_length=%s surface=%s schema_version=%s known_events=%s fallback=humanized",
        event_name,
        mode,
        streak_length,
        surface or "unspecified",
        HEADLINE_SCHEMA_VERSION,
        len(EVENT_HEADLINES),
    )
    fallback_title = _humanize_event_name(event_name)
    if mode == "form":
        return fallback_title

    if streak_length is None:
        return f"{fallback_title} streak"
    return f"{fallback_title} in {streak_length} straight matches"


def get_form_count_unit(event_name: str, surface: Optional[str] = None) -> str:
    """Return count unit used by form mode (defaults to matches)."""
    event_meta = EVENT_HEADLINES.get(event_name)
    if not event_meta:
        LOGGER.warning(
            "Form count unit mapping miss event_name=%s surface=%s schema_version=%s known_events=%s default_unit=matches",
            event_name,
            surface or "unspecified",
            HEADLINE_SCHEMA_VERSION,
            len(EVENT_HEADLINES),
        )
        return "matches"
    LOGGER.debug(
        "Form count unit mapping hit event_name=%s surface=%s count_unit=%s",
        event_name,
        surface or "unspecified",
        event_meta["display"]["form"].get("count_unit", "matches"),
    )
    return event_meta["display"]["form"].get("count_unit", "matches")


def is_supported_event(event_name: str) -> bool:
    return event_name in EVENT_HEADLINES
