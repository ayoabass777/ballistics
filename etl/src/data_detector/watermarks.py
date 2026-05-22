"""
Watermark helpers for detector movement records.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Tuple

from etl.src.data_detector.common import coerce_datetime


def fixture_kickoff_watermark(fixtures: Iterable[Dict[str, Any]]) -> Tuple[str, Optional[datetime], Optional[datetime]]:
    """
    Return the data coverage watermark for a fixture payload.
    """
    values = []
    for fixture in fixtures:
        value = coerce_datetime(fixture.get("kickoff_utc"))
        if value is not None:
            values.append(value)

    if not values:
        return "kickoff_utc", None, None

    return "kickoff_utc", min(values), max(values)
