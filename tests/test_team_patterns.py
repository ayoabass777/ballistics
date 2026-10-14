"""Team patterns selection (contract §3.4, D122)."""
import unittest

from engine.eval import team_patterns as tp

PRIOR = 15.0


def rec(side, event, k, n, base, *, context=None, eligible=True, comparison=None,
        primary=True, team=None, streak=0):
    """A pattern row shaped like the export, with the engine's Beta posterior."""
    a = PRIOR * base + k
    b = PRIOR * (1 - base) + (n - k)
    gap = round(k / n - base, 4)
    if comparison is None:
        comparison = tp_label(gap)
    return {"side": side, "team": team or side.title(), "event": event, "k": k, "n": n,
            "alpha": a, "beta": b, "base_rate": base, "league_gap": gap,
            "league_comparison": comparison, "display_eligible": eligible,
            "context": context or side, "primary": primary, "streak": streak}


def tp_label(gap):
    if gap >= 0.15:
        return "well_above"
    if gap >= 0.05:
        return "above"
    if gap > -0.05:
        return "typical"
    if gap > -0.15:
        return "below"
    return "well_below"


class ConfidenceTests(unittest.TestCase):
    def test_direction_follows_gap(self):
        up = rec("home", "winless", 6, 6, 0.55)
        down = rec("home", "win", 0 + 1, 8, 0.45)
        self.assertGreater(tp.league_confidence(up), 0.75)
        self.assertGreater(tp.league_confidence(down), 0.75)  # confidently below

    def test_typical_record_is_near_half(self):
        r = rec("home", "Over_1_5", 7, 9, 0.75)
        self.assertLess(tp.league_confidence(r), 0.75)

    def test_missing_inputs(self):
        self.assertIsNone(tp.league_confidence({"alpha": 1, "beta": 1, "base_rate": 0.5}))


class SelectionTests(unittest.TestCase):
    # Fixture 111915 shape: Atromitos (home) v AEK (away).
    ROWS = [
        rec("home", "winless", 6, 6, 0.55, team="Atromitos"),
        rec("home", "loss", 4, 6, 0.30, team="Atromitos"),
        rec("home", "concede_1", 6, 6, 0.65, team="Atromitos"),
        rec("home", "concede_2", 3, 6, 0.30, team="Atromitos"),
        rec("home", "Under_3_5", 6, 6, 0.75, team="Atromitos"),
        rec("home", "Under_4_5", 6, 6, 0.90, team="Atromitos"),
        rec("away", "unbeaten", 6, 6, 0.55, team="AEK"),
        rec("away", "score_1goal", 6, 6, 0.65, team="AEK"),
        rec("away", "Over_1_5", 5, 6, 0.70, team="AEK"),
        rec("away", "win", 0, 6, 0.30, team="AEK"),                      # k = 0: never shown
        rec("away", "clean_sheet", 3, 6, 0.30, team="AEK", eligible=False),  # not eligible
        rec("home", "halftime_win", 4, 6, 0.2, team="Atromitos"),        # excluded event
    ]

    def setUp(self):
        self.view = tp.select_view(self.ROWS)

    def events(self, key):
        return [(r["side"], r["event"]) for r in self.view[key]]

    def all_events(self):
        return [e for key in ("top_results", "top_attack_defence", "also", "see_all")
                for e in self.events(key)]

    def test_top_results_one_per_team(self):
        self.assertEqual(self.events("top_results"), [("home", "winless"), ("away", "unbeaten")])

    def test_attack_defence_pair(self):
        self.assertEqual(self.events("top_attack_defence"),
                         [("home", "concede_1"), ("away", "score_1goal")])

    def test_no_duplicates_and_exclusions(self):
        evs = self.all_events()
        self.assertEqual(len(evs), len(set(evs)))
        self.assertNotIn(("away", "win"), evs)
        self.assertNotIn(("away", "clean_sheet"), evs)
        self.assertNotIn(("home", "halftime_win"), evs)

    def test_one_rung_per_ladder(self):
        evs = self.all_events()
        self.assertEqual(sum(e in evs for e in [("home", "concede_1"), ("home", "concede_2")]), 1)
        self.assertEqual(sum(e in evs for e in [("home", "Under_3_5"), ("home", "Under_4_5")]), 1)

    def test_totals_never_top_and_also_by_hit_rate(self):
        tops = self.events("top_results") + self.events("top_attack_defence")
        self.assertFalse(any(e.startswith(("Over_", "Under_")) for _, e in tops))
        rates = [r["rate"] for r in self.view["also"] + self.view["see_all"]]
        self.assertEqual(rates, sorted(rates, reverse=True))
        self.assertLessEqual(len(self.view["also"]), 3)

    def test_payload_hides_engine_internals(self):
        r = self.view["top_results"][0]
        for key in ("alpha", "beta", "posterior", "league_gap", "league_confidence",
                    "display_eligible"):
            self.assertNotIn(key, r)
        self.assertEqual(r["label"], "Winless")

    def test_single_side_attack_defence(self):
        rows = [rec("home", "goalless", 5, 6, 0.25), rec("away", "concede_1", 4, 6, 0.6)]
        view = tp.select_view(rows)
        self.assertEqual([r["event"] for r in view["top_attack_defence"]], ["goalless"])

    def test_category_omitted_when_nothing_passes(self):
        view = tp.select_view([rec("home", "unbeaten", 5, 7, 0.6)])
        self.assertEqual(view["top_results"], [])
        self.assertEqual([r["event"] for r in view["also"]], ["unbeaten"])

    def test_no_comparison_cannot_be_top(self):
        view = tp.select_view([rec("home", "winless", 6, 6, 0.55, comparison="too_few_matches")])
        self.assertEqual(view["top_results"], [])
        self.assertEqual(len(view["also"]), 1)


class ViewTests(unittest.TestCase):
    def test_views_and_default(self):
        rows = [rec("home", "winless", 6, 6, 0.55),
                rec("home", "winless", 8, 10, 0.55, context="all", primary=False),
                rec("away", "unbeaten", 6, 6, 0.55),
                rec("away", "unbeaten", 9, 10, 0.55, context="all", primary=False)]
        p = tp.select_patterns(rows)
        self.assertEqual(p["default_view"], "venue")
        self.assertEqual(p["venue"]["top_results"][0]["context"], "home")
        self.assertEqual(p["all"]["top_results"][0]["context"], "all")

    def test_default_all_when_a_team_lacks_venue_sample(self):
        rows = [rec("home", "winless", 6, 6, 0.55),
                rec("away", "unbeaten", 6, 6, 0.55, context="all")]
        self.assertEqual(tp.select_patterns(rows)["default_view"], "all")

    def test_attach_adds_confidence_and_patterns(self):
        fx = {"bayesian": [rec("home", "winless", 6, 6, 0.55)]}
        tp.attach_team_patterns([fx])
        self.assertIn("league_confidence", fx["bayesian"][0])
        self.assertIn("patterns", fx)


if __name__ == "__main__":
    unittest.main()
