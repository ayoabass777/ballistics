"""Model watch (D136 Big dog DC cross-check, D120 Over 1.5) and its email block."""
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from engine.eval.big_dog_watch import watch_rows
from engine.eval.consensus_slips import BIG_DOG_DC_CONFIRM, dc_favourite_win
from etl.src.data_detector import email_templates as et
from etl.src.data_detector import model_watch as mw

DS = date(2026, 10, 18)


class FakeCursor:
    def __init__(self, rows, exists=True):
        self.rows, self.exists, self.last = rows, exists, None

    def execute(self, sql, params=None):
        self.last = sql

    def fetchone(self):
        return (self.exists,)

    def fetchall(self):
        return self.rows


def ko(day):
    return datetime(2026, 10, day, 15, tzinfo=timezone.utc)


class DcWinTests(unittest.TestCase):
    fixture = {"home": "A", "away": "B",
               "dixon_coles": [{"market": "Match Over 1.5", "lambda_home": 2.4, "lambda_away": 0.6}]}

    def test_favourite_side_and_range(self):
        home = dc_favourite_win(self.fixture, "A")
        away = dc_favourite_win(self.fixture, "B")
        self.assertGreater(home, 0.7)
        self.assertLess(away, 0.15)
        self.assertLess(home + away, 1)            # draws take the rest

    def test_missing_rates_or_team(self):
        self.assertIsNone(dc_favourite_win({"home": "A", "away": "B", "dixon_coles": []}, "A"))
        self.assertIsNone(dc_favourite_win(self.fixture, "C"))

    def test_watch_rows(self):
        pick = {"fixture_id": 7, "kickoff": "2026-10-19 16:00:00+00:00", "league": "L", "country": "X",
                "favourite": "B", "underdog": "A", "home": "A", "elo_gap": 300,
                "dc_win_prob": 0.7, "dc_confirms": 0.7 >= BIG_DOG_DC_CONFIRM}
        (row,) = watch_rows("2026-10-18", [pick, {"fixture_id": 8}])
        self.assertEqual(row[0], 7)
        self.assertFalse(row[8])                   # favourite is away
        self.assertTrue(row[11])


class WatchTests(unittest.TestCase):
    def test_over_1_5_latest_and_previous(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "w.csv"
            p.write_text("logged_at,report_stamp,market,source,n,avg_predicted,hit_rate,overconfidence,brier,flag\n"
                         "x,r1,Match Over 1.5,all,100,0.77,0.78,-0.01,0.16,\n"
                         "x,r2,Match Over 1.5,all,120,0.77,0.79,-0.02,0.16,\n"
                         "x,r2,Match Over 1.5,dixon_coles_only,90,0.74,0.79,-0.05,0.17,overconfidence -4.7 pts\n")
            o = mw.over_1_5(p)
        self.assertEqual(o["refit"], "r2")
        self.assertEqual(o["rows"][0]["previous_overconfidence"], -0.01)
        self.assertIsNone(o["rows"][1]["previous_overconfidence"])
        self.assertTrue(o["rows"][1]["flag"])
        self.assertIsNone(mw.over_1_5(Path("/nonexistent.csv")))

    def test_big_dogs_groups_and_day(self):
        rows = [(ko(17), "L", "A", "B", 300, 0.8, True, 2, 0),
                (ko(17), "L", "C", "D", 260, 0.6, False, 1, 1),
                (ko(18), "L", "E", "F", 280, 0.7, True, 0, 1),
                (ko(18), "L", "G", "H", 280, None, None, 3, 0)]   # no DC: day list only
        b = mw.big_dogs(FakeCursor(rows), DS)
        self.assertEqual((b["confirmed"]["won"], b["confirmed"]["n"]), (1, 2))
        self.assertEqual((b["not_confirmed"]["won"], b["not_confirmed"]["n"]), (0, 1))
        self.assertEqual([d["result"] for d in b["day"]], ["lost", "won"])
        self.assertTrue(b["verdict"].startswith("collecting"))
        self.assertIsNone(mw.big_dogs(FakeCursor([], exists=False), DS))

    def test_verdict_flags_when_not_helping(self):
        rows = [(ko(1), "L", "A", "B", 300, 0.8, True, 1 if i < 60 else 0, 0) for i in range(100)]
        rows += [(ko(1), "L", "A", "B", 260, 0.6, False, 1 if i < 70 else 0, 0) for i in range(100)]
        self.assertTrue(mw.big_dogs(FakeCursor(rows), DS)["verdict"].startswith("FLAG"))


class EmailTests(unittest.TestCase):
    watch = {"over_1_5": {"refit": "r2", "rows": [
                 {"source": "Dixon-Coles alone", "n": 776, "predicted": 0.745, "hit": 0.79,
                  "overconfidence": -0.047, "previous_overconfidence": -0.022, "flag": "overconfidence -4.7 pts"}]},
             "big_dogs": {"confirmed": {"n": 3, "won": 2, "rate": 2 / 3, "low": 0.3, "high": 0.9},
                          "not_confirmed": {"n": 1, "won": 0, "rate": 0.0, "low": 0.0, "high": 0.8},
                          "verdict": "collecting (3 of 100 confirmed Big dogs settled)",
                          "day": [{"match": "Porto v <Gil>", "league": "L", "score": "2-0", "gap": 334,
                                   "dc": 0.58, "result": "won"}]}}

    def test_watch_text_and_html(self):
        lines = "\n".join(et._watch_lines(self.watch))
        self.assertIn("Model watch", lines)
        self.assertIn("-4.7 pts, was -2.2", lines)
        self.assertIn("confirmed: 2/3 won 67%", lines)
        html = et._watch_html(self.watch)
        self.assertIn("Model watch", html)
        self.assertIn("&lt;Gil&gt;", html)
        self.assertEqual(et._watch_lines(None), [])
        self.assertIn("no log yet", et._watch_html({"over_1_5": None, "big_dogs": None}))


if __name__ == "__main__":
    unittest.main()
