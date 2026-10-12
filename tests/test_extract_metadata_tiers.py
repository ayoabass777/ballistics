"""Regression coverage for league tiers supplied by metadata.yaml."""

import unittest
from pathlib import Path
from unittest.mock import ANY, patch

import yaml

from etl.src.extract_metadata import process_league, upsert_league


class _Cursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def fetchone(self):
        return (123,)


class ExtractMetadataTierTests(unittest.TestCase):
    def test_english_league_one_is_configured_as_tier_three(self):
        metadata_path = Path(__file__).parents[1] / "data" / "metadata.yaml"
        metadata = yaml.safe_load(metadata_path.read_text())
        england = next(
            country for country in metadata["countries"]
            if country["country"] == "England"
        )
        leagues = {league["name"]: league["tier"] for league in england["leagues"]}

        self.assertEqual(leagues["League One"], 3)

    def test_upsert_league_persists_tier(self):
        cursor = _Cursor()

        league_id = upsert_league(cursor, 10, "League One", 41, tier=3)

        self.assertEqual(league_id, 123)
        sql, params = cursor.calls[0]
        self.assertIn("tier = EXCLUDED.tier", sql)
        self.assertEqual(params, (10, "League One", 41, 3))

    @patch("etl.src.extract_metadata.upsert_league", return_value=123)
    @patch("etl.src.extract_metadata.fetch_league_info")
    def test_process_league_forwards_configured_tier(self, fetch_league_info,
                                                     upsert_league_mock):
        fetch_league_info.return_value = [{
            "league": {"id": 41},
            "seasons": [],
        }]

        process_league(
            _Cursor(), "England", 10,
            {"name": "League One", "tier": 3},
            [],
        )

        upsert_league_mock.assert_called_once_with(
            ANY, 10, "League One", 41, tier=3)


if __name__ == "__main__":
    unittest.main()
