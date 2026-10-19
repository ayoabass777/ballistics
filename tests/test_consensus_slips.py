"""Focused regression tests for consensus market eligibility rules."""

import unittest
from unittest.mock import patch

from engine.eval import run_model
from engine.eval.bayesian_streaks import MIN_POSTERIOR_INTERESTING
from engine.eval.consensus_slips import (
    _build_elo_gap_shortlist,
    _within_shortlist_end_date,
    _build_slips,
    _prepare_slip_nodes,
    _analysis_from_signals,
    _passes_goal_model_rules,
    annotate_bayesian_analysis,
    bayesian_signals,
)


class EloGapShortlistTests(unittest.TestCase):
    def test_shortlist_cutoff_uses_vancouver_local_date(self):
        self.assertTrue(_within_shortlist_end_date(
            "2026-09-01T06:59:00Z", "2026-08-31"))
        self.assertFalse(_within_shortlist_end_date(
            "2026-09-01T07:00:00Z", "2026-08-31"))

    @staticmethod
    def fixture(fid, league, gap=320):
        home = f"Home {fid}"
        away = f"Favourite {fid}"
        return {
            "fixture_id": fid,
            "home": home,
            "away": away,
            "league": league,
            "country": "Testland",
            "kickoff": f"2026-09-{fid:02d}T18:00:00Z",
            "mismatch": [{
                "market": f"{away} Win",
                "probability": 0.81,
                "gap": gap,
                "gap_bin": "300+" if gap >= 300 else "200-300",
            }, {
                "market": f"{away} Over 0.5",
                "probability": 0.88,
                "gap": gap,
                "gap_bin": "300+" if gap >= 300 else "200-300",
            }],
        }

    def test_requires_gap_above_250_and_low_league_upset_rate(self):
        # Favourites here play away: D137 takes the home edge off their gap.
        fixtures = [
            self.fixture(1, "Stable", gap=316),     # 316 - 65 = 251
            self.fixture(2, "Volatile", gap=400),
            self.fixture(3, "Stable", gap=315),     # 250: not above
            self.fixture(4, "Stable", gap=265),     # Bayern-at-Freiburg case: 200
        ]
        volatility = {
            ("Stable", "Testland"): {"upset_rate": 0.27, "matches": 500},
            ("Volatile", "Testland"): {"upset_rate": 0.271, "matches": 500},
        }

        shortlist = _build_elo_gap_shortlist(fixtures, volatility)

        self.assertEqual(len(shortlist), 1)
        self.assertEqual(shortlist[0]["favourite"], "Favourite 1")
        self.assertEqual(shortlist[0]["underdog"], "Home 1")
        self.assertEqual(shortlist[0]["elo_gap"], 251)
        self.assertEqual(shortlist[0]["elo_gap_flat"], 316)
        self.assertEqual(shortlist[0]["gap_bin"], ">250")
        self.assertEqual(shortlist[0]["best_signal"], "Favourite 1 Over 0.5")
        self.assertEqual(shortlist[0]["win_confidence"], 0.81)

    def test_home_favourite_gets_the_home_edge(self):
        f = self.fixture(5, "Stable", gap=190)          # 190 + 65 = 255
        f["home"], f["away"] = "Favourite 5", "Away 5"
        g = self.fixture(6, "Stable", gap=185)          # 250: not above
        g["home"], g["away"] = "Favourite 6", "Away 6"
        volatility = {("Stable", "Testland"): {"upset_rate": 0.2, "matches": 500}}
        shortlist = _build_elo_gap_shortlist([f, g], volatility)
        self.assertEqual([p["favourite"] for p in shortlist], ["Favourite 5"])
        self.assertEqual(shortlist[0]["elo_gap"], 255)


class ConsensusGoalModelRuleTests(unittest.TestCase):
    def test_over_2_5_has_no_model_combination_restriction(self):
        for model in ("mismatch", "bayesian", "dixon_coles"):
            with self.subTest(model=model):
                self.assertTrue(
                    _passes_goal_model_rules("Match Over 2.5", {model})
                )

    def test_over_1_5_and_unders_need_dixon_coles(self):
        # Poisson retired 2026-10-11: Dixon-Coles alone qualifies these.
        for market in ("Match Over 1.5", "Match Under 3.5"):
            with self.subTest(market=market):
                self.assertTrue(_passes_goal_model_rules(market, {"dixon_coles"}))
                self.assertFalse(_passes_goal_model_rules(market, {"bayesian"}))
                self.assertFalse(
                    _passes_goal_model_rules(market, {"bayesian", "mismatch"}))


class BayesianFixtureAnalysisTests(unittest.TestCase):
    def test_display_cutoff_is_75_percent(self):
        self.assertEqual(MIN_POSTERIOR_INTERESTING, 0.75)

    @staticmethod
    def belief(event="score_1goal", **overrides):
        belief = {
            "event": event,
            "team": "Alpha FC",
            "posterior": 0.76,
            "posterior_raw": 0.74,
            "posterior_calibrated": 0.75,
            "base_rate": 0.62,
            "n": 10,
            "k": 8,
            "context": "home",
            "primary": True,
            "display_eligible": True,
            "display_threshold": 0.75,
            "streak": 4,
            "trend": "stable",
        }
        belief.update(overrides)
        return belief

    def test_analysis_keeps_every_belief_with_explicit_reason(self):
        beliefs = {
            101: {
                "home": [
                    self.belief(),
                    self.belief("Under_4_5"),
                    self.belief("BTTS"),
                    self.belief("loss"),
                    self.belief("Over_2_5", display_eligible=False),
                    self.belief("unbeaten", primary=False, context="all"),
                ],
                "away": [],
            }
        }

        annotated = annotate_bayesian_analysis(
            beliefs, {101: ("Alpha FC", "Beta FC")}
        )
        rows = annotated[101]["home"]

        self.assertEqual(len(rows), 6)
        self.assertEqual(
            [row["eligibility_reason"] for row in rows],
            [
                "included",
                "not_betting_allowlist",
                "btts_disabled",
                "no_market_mapping",
                "below_display_threshold",
                "secondary_context",
            ],
        )
        self.assertEqual(sum(row["consensus_eligible"] for row in rows), 1)

    def test_signal_pool_uses_only_included_analysis_rows(self):
        beliefs = {
            101: {
                "home": [
                    self.belief(),
                    self.belief("Under_4_5"),
                    self.belief("Over_2_5", display_eligible=False),
                ],
                "away": [],
            }
        }
        home_away = {101: ("Alpha FC", "Beta FC")}
        annotated = annotate_bayesian_analysis(beliefs, home_away)

        signals = bayesian_signals(annotated, home_away)

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["unified_market"], "Alpha FC Over 0.5")

    def test_fan_in_analysis_replaces_lean_signal_with_full_belief_audit(self):
        signals = [{
            "fixture_id": 101,
            "model": "bayesian",
            "unified_market": "Alpha FC Over 0.5",
            "probability": 0.76,
            "detail": {"event": "score_1goal", "team": "Alpha FC"},
        }]
        fixture_meta = {
            101: {
                "fixture_id": 101,
                "home": "Alpha FC",
                "away": "Beta FC",
                "league": "Test League",
                "country": "Testland",
                "kickoff": "2026-08-29T12:00:00+00:00",
            }
        }
        audit = {
            101: {
                "home": [
                    self.belief(consensus_eligible=True,
                                eligibility_reason="included"),
                    self.belief("Under_4_5", consensus_eligible=False,
                                eligibility_reason="not_betting_allowlist"),
                ],
                "away": [],
            }
        }

        analysis = _analysis_from_signals(
            signals, fixture_meta, {}, bayesian_analysis=audit
        )

        self.assertEqual(len(analysis), 1)
        self.assertEqual(len(analysis[0]["bayesian"]), 2)
        self.assertEqual(
            analysis[0]["bayesian"][1]["eligibility_reason"],
            "not_betting_allowlist",
        )

    def test_bayesian_runner_persists_full_analysis_separately_from_signals(self):
        signals = [{
            "fixture_id": 101,
            "unified_market": "Alpha FC Over 0.5",
            "probability": 0.76,
        }]
        audit = {101: {"home": [self.belief()], "away": []}}

        with (
            patch.object(
                run_model, "_run_bayesian", return_value=(signals, audit)
            ) as runner,
            patch.object(run_model.ps, "write_model_analysis") as write_analysis,
            patch.object(run_model.ps, "write_model_picks", return_value=1),
            patch.object(run_model.ps, "write_model_run") as write_run,
        ):
            count = run_model.run_and_store("bayesian", "2026-08-28", 10)

        self.assertEqual(count, 1)
        runner.assert_called_once_with(10, include_analysis=True)
        write_analysis.assert_called_once_with(
            "2026-08-28", "bayesian", audit
        )
        write_run.assert_called_once_with(
            "2026-08-28", "bayesian", "ok", 1
        )


class WeightedSlipGraphTests(unittest.TestCase):
    @staticmethod
    def pick(fid, *, home=None, away=None, models=2, confidence=0.80,
             odds=1.70, selection=None):
        home = home or f"Home {fid}"
        away = away or f"Away {fid}"
        return {
            "fixture_id": fid,
            "home": home,
            "away": away,
            "match": f"{home} vs {away}",
            "kickoff": "2026-08-29T12:00:00+00:00",
            "league": "Test League",
            "sportybet_market": "Match Result",
            "sportybet_selection": selection or home,
            "line": None,
            "n_models": models,
            "models": [f"model_{i}" for i in range(models)],
            "avg_confidence": confidence,
            "max_confidence": confidence,
            "consensus_score": models + confidence + 0.05,
            "has_odds": True,
            "odds": odds,
            "real_odds": odds,
            "est_odds": odds,
            "ev": 1.05,
        }

    def test_elite_club_can_be_core_but_still_needs_quality(self):
        qualified = self.pick(
            1, home="Real Madrid", models=1, confidence=0.84
        )
        weak = self.pick(
            2, home="FC Barcelona", models=1, confidence=0.78
        )
        opponent_pick = self.pick(
            3, away="PSV Eindhoven", models=2, confidence=0.80,
            selection="Home 3 Over 0.5",
        )

        nodes = _prepare_slip_nodes(
            [qualified, weak, opponent_pick], min_ev=1.0
        )

        self.assertEqual(nodes[0]["slip_role"], "core")
        self.assertEqual(nodes[0]["repeat_limit"], 3)
        self.assertEqual(nodes[0]["core_team"], "real madrid")
        self.assertEqual(nodes[1]["slip_role"], "filler")
        self.assertIsNone(nodes[2]["core_team"])
        self.assertEqual(nodes[2]["slip_role"], "support")

    def test_graph_slips_repeat_core_and_keep_fixture_conflicts_apart(self):
        core = [self.pick(i, models=3, confidence=0.82) for i in range(1, 5)]
        support = [
            self.pick(i, models=2, confidence=0.78)
            for i in range(5, 23)
        ]
        # A second market on fixture 5 creates a hard conflict edge.
        conflict = self.pick(
            5, models=2, confidence=0.77, selection="Away 5"
        )

        slips = _build_slips(
            core + support + [conflict], target_odds=4.0,
            max_legs=8, bankroll=0,
        )

        self.assertGreaterEqual(len(slips), 2)
        usage = {}
        for slip in slips:
            fixture_ids = [pick["fixture_id"] for pick in slip["selections"]]
            self.assertEqual(len(fixture_ids), len(set(fixture_ids)))
            self.assertLessEqual(slip["role_counts"]["core"], 2)
            self.assertGreaterEqual(slip["role_counts"]["core"], 1)
            for pick in slip["selections"]:
                key = (pick["fixture_id"], pick["sportybet_selection"])
                usage[key] = usage.get(key, 0) + 1
                self.assertLessEqual(usage[key], pick["repeat_limit"])

        core_usage = [
            count for (fid, _), count in usage.items() if fid in {1, 2, 3, 4}
        ]
        self.assertTrue(any(count > 1 for count in core_usage))
        self.assertTrue(all(count <= 3 for count in core_usage))


if __name__ == "__main__":
    unittest.main()


class SlightAndTierTests(unittest.TestCase):
    def test_slight_level_and_record(self):
        from engine.eval.consensus_slips import slight_favourite_level, aggregate_slight_record
        self.assertEqual(slight_favourite_level(175), "slight_edge")
        self.assertEqual(slight_favourite_level(176), "favourite")
        rec = aggregate_slight_record([("slight_edge", 100, 51, 26), ("favourite", 50, 31, 0)])
        self.assertEqual(rec["slight_edge"]["win_rate"], 0.51)
        self.assertEqual(rec["favourite"]["draw_rate"], 0.0)

    def test_tier_labels_and_attach(self):
        from engine.eval.consensus_slips import attach_team_tiers, tier_labels
        ratings = {i: r for i, r in enumerate([1800, 1790, 1810, 1600, 1610, 1590, 1605, 1400, 1410, 1395])}
        t = tier_labels(ratings)
        self.assertEqual({t[0], t[1], t[2]}, {"Top"})
        self.assertEqual(t[4], "Middle")
        self.assertEqual(t[9], "Bottom")
        self.assertEqual(tier_labels({1: 1500, 2: 1600}), {})       # too few teams
        fx = [{"league": "L", "country": "C", "home_team_id": 0, "away_team_id": 9},
              {"league": "L", "country": "C", "home_team_id": 0, "away_team_id": 99}]
        attach_team_tiers(fx, {("L", "C"): t})
        self.assertEqual(fx[0]["tiers"], {"home": "Top", "away": "Bottom"})
        self.assertIsNone(fx[1]["tiers"])
