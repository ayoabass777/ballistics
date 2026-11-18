"""Full-time match statistics from the live poller's by-id fetch.

API-Football's fixtures?ids= response carries `statistics` (corners, shots,
possession, cards, fouls, offsides, saves, passes and, where covered,
expected_goals). The poller makes that call once when a fixture leaves the
live list, so the statistics of every final fixture it tracked are stored at
no extra cost (raw.fixture_statistics, migration 0004).

Pure parsing here; etl/src/live_poller.py does the IO.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

# Provider names that don't snake_case into something readable.
RENAMES = {"passes_": "passes_pct", "passes_percent": "passes_pct"}


def stat_key(name: str) -> str:
    """'Corner Kicks' → corner_kicks, 'Passes %' → passes_pct."""
    key = re.sub(r"[^a-z0-9]+", "_", (name or "").strip().lower().replace("%", " percent"))
    key = key.strip("_")
    return RENAMES.get(key, key)


def stat_value(value: Any) -> Optional[float]:
    """57 for '57%', 1.34 for '1.34', None when the provider sends nothing usable."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip().rstrip("%").strip()
    try:
        return float(text)
    except ValueError:
        return None


def parse_statistics(item: Dict[str, Any], fixture_id: int, now: datetime) -> List[Dict[str, Any]]:
    """Provider fixture item → raw.fixture_statistics rows (empty when it has none)."""
    fixture = item.get("fixture") or {}
    teams = item.get("teams") or {}
    sides = {(teams.get("home") or {}).get("id"): "home", (teams.get("away") or {}).get("id"): "away"}
    status = (fixture.get("status") or {}).get("short")
    rows: Dict[tuple, Dict[str, Any]] = {}
    for block in item.get("statistics") or []:
        side = sides.get((block.get("team") or {}).get("id"))
        if side is None:
            continue
        for entry in block.get("statistics") or []:
            key = stat_key(entry.get("type") or "")
            if not key:
                continue
            raw = entry.get("value")
            rows[(side, key)] = {
                "fixture_id": fixture_id, "api_fixture_id": fixture.get("id"), "side": side, "stat": key,
                "value": stat_value(raw), "raw_value": None if raw is None else str(raw),
                "fixture_status": status, "captured_at": now,
            }
    return list(rows.values())
