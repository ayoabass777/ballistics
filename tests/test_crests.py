"""Crest URLs (engine.api.crests)."""
import os
import unittest
from unittest.mock import patch

from engine.api import crests


class CrestUrlTests(unittest.TestCase):
    def test_none_without_id(self):
        self.assertIsNone(crests.crest_url("teams", None))
        self.assertIsNone(crests.crest_url("teams", ""))

    def test_default_bucket_url(self):
        env = {"ASSETS_BUCKET": "b", "AWS_DEFAULT_REGION": "ca-central-1"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("CREST_BASE_URL", None)
            self.assertEqual(crests.crest_url("teams", "abc"),
                             "https://b.s3.ca-central-1.amazonaws.com/crests/teams/abc")

    def test_cdn_override(self):
        with patch.dict(os.environ, {"CREST_BASE_URL": "https://cdn.example/"}):
            self.assertEqual(crests.crest_url("leagues", "x"), "https://cdn.example/crests/leagues/x")

    def test_unknown_kind(self):
        with self.assertRaises(ValueError):
            crests.crest_url("venues", "x")


if __name__ == "__main__":
    unittest.main()
