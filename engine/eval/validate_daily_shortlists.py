"""Validate that all curated consensus sections were refreshed for a run."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


SHORTLIST_SECTIONS = (
    "bounds_1_4_shortlist",
    "over_1_5_shortlist",
    "elo_gap_shortlist",
)
MAX_REFRESH_AGE = timedelta(hours=6)


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def validate_daily_shortlists(
    payload: dict,
    run_ds: str,
    *,
    now: datetime | None = None,
    max_age: timedelta = MAX_REFRESH_AGE,
) -> None:
    """Raise ``ValueError`` when a daily shortlist payload is stale or partial.

    Empty lists are valid: zero qualifying events is different from a section
    that was not rebuilt. The refresh metadata records that distinction.
    """
    errors = []
    refresh = payload.get("shortlist_refresh")
    if not isinstance(refresh, dict):
        raise ValueError("missing shortlist_refresh metadata")

    if payload.get("run_ds") != run_ds:
        errors.append(
            f"payload run_ds={payload.get('run_ds')!r}, expected {run_ds!r}"
        )
    if refresh.get("run_ds") != run_ds:
        errors.append(
            f"refresh run_ds={refresh.get('run_ds')!r}, expected {run_ds!r}"
        )

    generated_raw = refresh.get("generated_at")
    try:
        generated_at = _parse_utc(generated_raw)
    except (AttributeError, TypeError, ValueError) as exc:
        errors.append(f"invalid generated_at={generated_raw!r}: {exc}")
        generated_at = None

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if generated_at is not None:
        age = current - generated_at
        if age > max_age:
            errors.append(
                f"shortlists are stale by {age}; maximum age is {max_age}"
            )
        if age < -timedelta(minutes=5):
            errors.append("shortlist generated_at is unexpectedly in the future")

    section_meta = refresh.get("sections")
    if not isinstance(section_meta, dict):
        errors.append("missing shortlist_refresh.sections metadata")
        section_meta = {}

    for key in SHORTLIST_SECTIONS:
        rows = payload.get(key)
        if not isinstance(rows, list):
            errors.append(f"{key} is missing or is not a list")
            continue
        metadata = section_meta.get(key)
        if not isinstance(metadata, dict):
            errors.append(f"missing refresh metadata for {key}")
            continue
        if metadata.get("count") != len(rows):
            errors.append(
                f"{key} count={len(rows)}, metadata count={metadata.get('count')!r}"
            )
        if metadata.get("updated_at") != generated_raw:
            errors.append(f"{key} updated_at does not match generated_at")

    if errors:
        raise ValueError("daily shortlist validation failed: " + "; ".join(errors))


def validate_file(path: Path, run_ds: str) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"daily consensus output not found: {path}")
    with path.open() as handle:
        payload = json.load(handle)
    validate_daily_shortlists(payload, run_ds)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify that today's 1-4, Over 1.5 and Elo lists are fresh"
    )
    parser.add_argument("--run-ds", required=True, help="Airflow logical run date")
    parser.add_argument("--path", type=Path, default=None)
    args = parser.parse_args()

    path = args.path or (
        Path(__file__).parent / f"consensus_slips_{args.run_ds}.json"
    )
    payload = validate_file(path, args.run_ds)
    counts = {
        key: len(payload[key])
        for key in SHORTLIST_SECTIONS
    }
    print(
        "Daily shortlists verified "
        f"for run_ds={args.run_ds}: {json.dumps(counts, sort_keys=True)}"
    )


if __name__ == "__main__":
    main()
