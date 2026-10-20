"""Tests for the daily curated-section freshness contract."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from engine.eval.consensus_slips import save_slips
from engine.eval.validate_daily_shortlists import validate_daily_shortlists


class DailyShortlistValidationTests(unittest.TestCase):
    NOW = datetime(2026, 9, 3, 18, 0, tzinfo=timezone.utc)

    def payload(self):
        generated = "2026-09-03T17:30:00Z"
        sections = {
            "bounds_1_4_shortlist": [{"fixture_id": 1}],
            "elo_gap_shortlist": [],
        }
        return {
            "run_ds": "2026-09-03",
            **sections,
            "shortlist_refresh": {
                "run_ds": "2026-09-03",
                "generated_at": generated,
                "sections": {
                    key: {"count": len(rows), "updated_at": generated}
                    for key, rows in sections.items()
                },
            },
        }

    def test_accepts_fresh_sections_including_empty_results(self):
        validate_daily_shortlists(
            self.payload(), "2026-09-03", now=self.NOW
        )

    def test_rejects_a_stale_refresh(self):
        payload = self.payload()
        payload["shortlist_refresh"]["generated_at"] = (
            self.NOW - timedelta(hours=7)
        ).isoformat()

        with self.assertRaisesRegex(ValueError, "shortlists are stale"):
            validate_daily_shortlists(payload, "2026-09-03", now=self.NOW)

    def test_rejects_a_missing_section(self):
        payload = self.payload()
        del payload["elo_gap_shortlist"]

        with self.assertRaisesRegex(ValueError, "elo_gap_shortlist"):
            validate_daily_shortlists(payload, "2026-09-03", now=self.NOW)

    def test_rejects_a_previous_run_payload(self):
        payload = self.payload()
        payload["run_ds"] = "2026-09-02"

        with self.assertRaisesRegex(ValueError, "payload run_ds"):
            validate_daily_shortlists(payload, "2026-09-03", now=self.NOW)

    def test_save_slips_marks_every_section_as_refreshed(self):
        result = {
            "pool": [],
            "near_threshold": [],
            "bounds_1_4_shortlist": [],
            "elo_gap_shortlist": [],
            "fixture_analysis": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            output_path = save_slips(
                result,
                run_ds="2026-09-03",
                output_dir=Path(directory),
            )
            with open(output_path) as handle:
                payload = json.load(handle)

        self.assertEqual(payload["run_ds"], "2026-09-03")
        for key in (
            "bounds_1_4_shortlist",
            "elo_gap_shortlist",
        ):
            self.assertEqual(
                payload["shortlist_refresh"]["sections"][key]["count"],
                0,
            )
        # Betting outputs removed 2026-10-20.
        self.assertNotIn("slips", payload)
        self.assertNotIn("over_1_5_shortlist", payload)


if __name__ == "__main__":
    unittest.main()
