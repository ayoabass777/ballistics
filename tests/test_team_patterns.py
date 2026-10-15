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
        rec("away", "clean_sheet", 2, 6, 0.30, team="AEK"),             # gap too small to qualify
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

    def test_weak_team_record_does_not_qualify(self):
        # Unbeaten 5/7 vs 60%: gap too uncertain -> nowhere, not even See all.
        view = tp.select_view([rec("home", "unbeaten", 5, 7, 0.6)])
        self.assertEqual(sum(len(v) for v in view.values()), 0)

    def test_no_comparison_team_record_hidden(self):
        view = tp.select_view([rec("home", "winless", 6, 6, 0.55, comparison="too_few_matches")])
        self.assertEqual(sum(len(v) for v in view.values()), 0)

    def test_team_events_ignore_engine_score(self):
        # D123: Zlin Lost 9/10 vs 40% has engine score 0.62 (not eligible) but qualifies.
        rows = [rec("away", "loss", 9, 10, 0.40, eligible=False, team="Zlin"),
                rec("away", "winless", 10, 10, 0.60, team="Zlin")]
        view = tp.select_view(rows)
        self.assertEqual([r["event"] for r in view["top_results"]], ["loss"])
        self.assertEqual([r["event"] for r in view["also"]], ["winless"])

    def test_below_league_team_records_hidden(self):
        # Won 1/10 vs 40% is confidently below the league: not shown (mirror of Winless).
        view = tp.select_view([rec("home", "win", 1, 10, 0.40)])
        self.assertEqual(sum(len(v) for v in view.values()), 0)

    def test_pair_survives_higher_rung(self):
        rows = [rec("home", "concede_1", 6, 6, 0.71), rec("away", "score_1goal", 6, 6, 0.71),
                rec("away", "score_2goals", 4, 6, 0.38)]
        view = tp.select_view(rows)
        self.assertEqual([r["event"] for r in view["top_attack_defence"]], ["concede_1", "score_1goal"])
        self.assertNotIn("score_2goals", [r["event"] for r in view["also"] + view["see_all"]])

    def test_dropped_lines_never_shown(self):
        rows = [rec("home", "Over_4_5", 6, 6, 0.10), rec("home", "Under_1_5", 6, 6, 0.20)]
        view = tp.select_view(rows)
        self.assertEqual(sum(len(v) for v in view.values()), 0)

    def test_match_goals_use_engine_score(self):
        typical = rec("home", "Under_4_5", 6, 6, 0.90)                  # eligible, typical
        unusual = rec("home", "Over_2_5", 6, 6, 0.50, eligible=False)  # not eligible
        view = tp.select_view([typical, unusual])
        self.assertEqual([r["event"] for r in view["also"]], ["Under_4_5"])


class LabelTests(unittest.TestCase):
    """D126: evidence decides the comparison wording."""

    def lab(self, k, n, base, **kw):
        return tp.comparison_label(rec("home", "win", k, n, base, **kw))

    def test_strong_evidence_above_and_below(self):
        self.assertEqual(self.lab(9, 10, 0.40), "well_above")      # Zlin Lost 9/10
        self.assertEqual(self.lab(30, 40, 0.65), "above")          # +10 pts over 40 games
        self.assertEqual(self.lab(3, 10, 0.73), "well_below")

    def test_small_sample_big_gap_is_not_typical(self):
        self.assertEqual(self.lab(4, 6, 0.50), "inconclusive")     # 67% vs 50%, 6 games

    def test_typical_needs_twelve_games(self):
        self.assertEqual(self.lab(7, 9, 0.80), "inconclusive")     # close, but only 9 games
        self.assertEqual(self.lab(10, 12, 0.80), "typical")        # close, 12 games
        self.assertEqual(self.lab(9, 15, 0.62), "typical")

    def test_no_league_rate_passes_through(self):
        self.assertEqual(self.lab(9, 10, 0.40, comparison="too_few_matches"), "too_few_matches")

    def test_public_record_uses_new_label(self):
        view = tp.select_view([rec("home", "Under_4_5", 7, 9, 0.80)])     # close, 9 games
        self.assertEqual(view["also"][0]["league_comparison"], "inconclusive")


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
