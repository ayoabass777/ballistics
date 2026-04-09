"""
CLI entrypoint to correct fixture kickoff times.
Uses fixture_correction helpers to compare API vs DB times and update mismatches.
"""
import argparse
import os

from etl.src.config import UPDATE_FIXTURES_LOG
from etl.src.fixture_correction import correct_changed_fixtures, correct_missed_fixtures
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Correct fixture kickoff times.")
    parser.add_argument(
        "--mode",
        choices=["missed", "changed", "both"],
        default="both",
        help="Which fixtures to correct: recently played (missed), upcoming (changed), or both.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=11,
        help="Number of fixtures to check per league (default: 11).",
    )
    return parser.parse_args()


def run(mode: str, count: int) -> int:
    updated_total = 0
    if mode in {"missed", "both"}:
        updated = correct_missed_fixtures(no_of_fixtures=count)
        updated_total += len(updated)
    if mode in {"changed", "both"}:
        updated = correct_changed_fixtures(no_of_fixtures=count)
        updated_total += len(updated)
    return updated_total


if __name__ == "__main__":
    if not os.getenv("UPDATE_FIXTURES_LOG"):
        logger.warning("Environment variable UPDATE_FIXTURES_LOG not set; using default log path: %s", UPDATE_FIXTURES_LOG)
    args = parse_args()
    try:
        updated_count = run(args.mode, args.count)
        logger.info("Fixture correction complete: %d fixtures updated.", updated_count)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received; shutting down fixture correction.")
    except Exception as exc:
        logger.error("Error running fixture correction: %s", exc, exc_info=True)
