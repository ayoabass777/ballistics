"""card_versions.publish: one version per meaningful card change."""
import unittest

from engine.eval import card_versions as cv


class FakeCur:
    def __init__(self):
        self.inserts = []

    def execute(self, sql, params=None):
        if sql.lstrip().startswith("INSERT"):
            self.inserts.append(params)

    def fetchone(self):
        return None                      # no earlier version


def _row(fid, kickoff):
    pick = {"list": "goals", "team": "Toluca", "market": "over_1.5", "probability": 0.8}
    return fid, {"discovery": {"fixture": {"fixture_id": fid, "home": "Toluca", "away": "America",
                                           "kickoff": kickoff},
                               "standout": pick, "sections": {"goals": pick}}}


class PublishTests(unittest.TestCase):
    def test_fixture_without_kickoff_is_skipped_not_fatal(self):
        cur = FakeCur()
        cv.publish(cur, [_row(1, None), _row(2, "2026-10-20T19:00:00+00:00")], "2026-10-20")
        self.assertEqual({p[0] for p in cur.inserts}, {2})

    def test_same_pick_in_two_places_is_one_version(self):
        cur = FakeCur()
        cv.publish(cur, [_row(2, "2026-10-20T19:00:00Z")], "2026-10-20")
        # fixture-level card (no pick) + the pick, shared by standout and section
        self.assertEqual(len(cur.inserts), 2)


if __name__ == "__main__":
    unittest.main()
