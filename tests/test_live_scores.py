"""Live poller (docs/live_scores.md): parsing, diffs, pacing and alerts."""
from datetime import datetime, timedelta, timezone
import unittest

from etl.src import live_scores as ls
from etl.src import live_poller as lp

NOW = datetime(2026, 11, 7, 15, 0, tzinfo=timezone.utc)


def item(api_id, status="2H", minute=60, home=1, away=0, events=()):
    return {"fixture": {"id": api_id, "status": {"short": status, "elapsed": minute, "extra": None}},
            "teams": {"home": {"id": 10}, "away": {"id": 20}},
            "goals": {"home": home, "away": away}, "events": list(events)}


def event(minute, team, etype="Goal", detail="Normal Goal", player="A"):
    return {"time": {"elapsed": minute, "extra": None}, "team": {"id": team},
            "player": {"id": None, "name": player}, "assist": {}, "type": etype, "detail": detail}


class ParseTests(unittest.TestCase):
    def test_state_and_red_cards(self):
        state, events = ls.parse_fixture(item(5, events=[
            event(12, 10), event(40, 20, "Card", "Red Card", "B"),
            event(70, 20, "Card", "Second Yellow card", "C"), event(80, 10, "Card", "Yellow Card")]), 99)
        self.assertEqual(state["fixture_id"], 99)
        self.assertEqual((state["status"], state["elapsed"], state["home_goals"], state["away_goals"]),
                         ("2H", 60, 1, 0))
        self.assertEqual((state["home_red_cards"], state["away_red_cards"]), (0, 2))
        self.assertEqual([e["side"] for e in events], ["home", "away", "away", "home"])

    def test_event_keys_are_stable_and_distinct(self):
        twice = [event(30, 10, "Card", "Yellow Card", "A"), event(30, 10, "Card", "Yellow Card", "A")]
        first = [e["event_key"] for e in ls.parse_events(item(1, events=twice))]
        again = [e["event_key"] for e in ls.parse_events(item(1, events=twice))]
        self.assertEqual(first, again)
        self.assertEqual(len(set(first)), 2)


def stored(events, revoked=False):
    return {e["event_key"]: {**e, "revoked": revoked} for e in events}


class DiffTests(unittest.TestCase):
    def test_var_revokes_and_return_restores(self):
        goal = ls.parse_events(item(1, events=[event(20, 10)]))
        upsert, revoke, fixed = ls.diff_events({}, goal)
        self.assertEqual((len(upsert), revoke, fixed), (1, [], []))
        key = goal[0]["event_key"]
        # VAR rules the goal out: it disappears from the feed.
        self.assertEqual(ls.diff_events(stored(goal), []), ([], [key], []))
        # Seen again: upserted, which clears revoked_at.
        self.assertEqual(ls.diff_events(stored(goal, revoked=True), goal), (goal, [], []))
        # Unchanged: nothing to write.
        self.assertEqual(ls.diff_events(stored(goal), goal), ([], [], []))

    def corrected(self, before, after):
        old = ls.parse_events(item(1, events=before))
        new = ls.parse_events(item(1, events=after))
        upsert, revoke, fixed = ls.diff_events(stored(old), new)
        self.assertEqual((upsert, revoke), ([], []))
        return {k: e for k, e in fixed}

    def test_scorer_filled_in_is_a_correction(self):
        fixed = self.corrected([event(38, 20, player=None)], [event(38, 20, player="I. Tuci")])
        self.assertEqual([e["player"] for e in fixed.values()], ["I. Tuci"])

    def test_minute_moved_is_a_correction(self):
        fixed = self.corrected([event(7, 10, player=None)], [event(8, 10, player=None)])
        self.assertEqual([e["minute"] for e in fixed.values()], [8])
        self.corrected([event(25, 20, "Card", "Yellow Card", None)],
                       [event(24, 20, "Card", "Yellow Card", "Ante Aralica")])

    def test_booking_reassigned_is_a_correction(self):
        self.corrected([event(22, 20, "Card", "Yellow Card", "Maksim Matveev")],
                       [event(22, 20, "Card", "Yellow Card", "Dzambolat Tsallagov")])

    def test_renumbered_substitutions_keep_their_player(self):
        fixed = self.corrected(
            [event(90, 10, "subst", "Substitution 3", "Danilin"), event(90, 10, "subst", "Substitution 4", "Simanenka")],
            [event(90, 10, "subst", "Substitution 3", "Simanenka"), event(90, 10, "subst", "Substitution 4", "Danilin")])
        self.assertEqual({k.split("|")[3]: e["detail"] for k, e in fixed.items()},
                         {"Danilin": "Substitution 4", "Simanenka": "Substitution 3"})

    def test_ruled_out_goal_is_not_explained_by_another(self):
        old = ls.parse_events(item(1, events=[event(20, 10, player="A")]))
        for after in ([event(20, 20, player="A")],             # other side
                      [event(30, 10, player="A")],             # far later
                      [event(21, 10, player="B")],             # another scorer, another minute
                      [event(20, 10, "Var", "Goal cancelled", "A")]):
            new = ls.parse_events(item(1, events=after))
            upsert, revoke, fixed = ls.diff_events(stored(old), new)
            self.assertEqual((revoke, fixed, len(upsert)), ([old[0]["event_key"]], [], 1), after)

    def test_state_changed_ignores_identical_polls(self):
        state, _ = ls.parse_fixture(item(1), 1)
        self.assertTrue(ls.state_changed(None, state))
        self.assertFalse(ls.state_changed(dict(state), state))
        self.assertTrue(ls.state_changed(dict(state), {**state, "elapsed": 61}))


class WindowTests(unittest.TestCase):
    def test_idle_sleep_wakes_before_kickoff(self):
        self.assertEqual(ls.idle_sleep(NOW, NOW + timedelta(minutes=13)), 180)
        self.assertEqual(ls.idle_sleep(NOW, None), 600)
        self.assertEqual(ls.idle_sleep(NOW, NOW + timedelta(minutes=5)), 30)

    def test_should_be_live_needs_several_under_way(self):
        under_way = [{"kickoff_utc": NOW - timedelta(minutes=40), "status": "NS"}] * 3
        self.assertEqual(ls.should_be_live(under_way, NOW), 3)
        self.assertEqual(ls.should_be_live(under_way[:2], NOW), 0)
        finished = [{"kickoff_utc": NOW - timedelta(minutes=40), "status": "FT"}] * 3
        self.assertEqual(ls.should_be_live(finished, NOW), 0)

    def test_provider_errors(self):
        self.assertIsNone(ls.provider_errors({"errors": []}))
        self.assertIn("plan", ls.provider_errors({"errors": {"plan": "Free plans do not have access"}}))


class FakeProvider:
    def __init__(self, live=(), ids=(), fail=None, remaining=5000):
        self._live, self._ids, self.fail, self.remaining = list(live), list(ids), fail, remaining
        self.id_calls = []

    def live(self):
        if self.fail:
            raise self.fail
        return self._live

    def by_ids(self, api_ids):
        self.id_calls.append(api_ids)
        return self._ids


class FakeStore:
    def __init__(self, window=(), open_live=None, ids=None, next_kickoff=None):
        self._window, self._open = list(window), open_live or {}
        self._ids, self._next = ids or {}, next_kickoff
        self.writes = []

    def window(self, now): return self._window
    def open_fixtures(self): return self._open
    def next_kickoff(self, now): return self._next
    def fixture_ids(self, api_ids): return {a: f for a, f in self._ids.items() if a in api_ids}

    def write_poll(self, parsed, now):
        self.writes.append(parsed)
        return [s["fixture_id"] for s, _ in parsed]


def poller(provider, store):
    alerts = []
    p = lp.Poller(provider, store, lambda subject, body: alerts.append(subject), clock=lambda: NOW)
    return p, alerts


class PollerTests(unittest.TestCase):
    def test_sleeps_without_calling_provider_when_nothing_is_live(self):
        provider = FakeProvider(fail=AssertionError("must not call"))
        p, _ = poller(provider, FakeStore(next_kickoff=NOW + timedelta(hours=2)))
        self.assertEqual(p.cycle(), 600)

    def test_finished_fixtures_do_not_keep_it_awake(self):
        store = FakeStore(window=[{"fixture_id": 1, "kickoff_utc": NOW - timedelta(hours=2), "status": "FT"}])
        p, _ = poller(FakeProvider(fail=AssertionError("must not call")), store)
        self.assertEqual(p.cycle(), 600)

    def test_keeps_only_tracked_fixtures(self):
        store = FakeStore(window=[{"fixture_id": 7, "kickoff_utc": NOW - timedelta(minutes=50), "status": "NS"}],
                          ids={500: 7})
        p, _ = poller(FakeProvider(live=[item(500), item(999)]), store)
        self.assertEqual(p.cycle(), lp.POLL_SECONDS)
        self.assertEqual([s["fixture_id"] for s, _ in store.writes[0]], [7])

    def test_fixture_leaving_live_list_is_fetched_once_by_id(self):
        provider = FakeProvider(live=[], ids=[item(500, status="FT", minute=90, home=2, away=1)])
        store = FakeStore(open_live={7: 500}, ids={500: 7})
        p, alerts = poller(provider, store)
        p.cycle()
        self.assertEqual(provider.id_calls, [[500]])
        final = store.writes[0][0][0]
        self.assertEqual((final["status"], final["home_goals"], final["away_goals"]), ("FT", 2, 1))
        self.assertEqual(alerts, [])

    def test_empty_feed_while_matches_under_way_alerts_once(self):
        window = [{"fixture_id": i, "kickoff_utc": NOW - timedelta(minutes=45), "status": "NS"} for i in range(4)]
        p, alerts = poller(FakeProvider(live=[]), FakeStore(window=window))
        p.cycle(); p.cycle()
        self.assertEqual(alerts, ["✗ Ballistics live scores: empty live feed"])

    def test_provider_failures_back_off_then_alert(self):
        store = FakeStore(window=[{"fixture_id": 1, "kickoff_utc": NOW, "status": "NS"}])
        p, alerts = poller(FakeProvider(fail=lp.ProviderError("plan: Free plans do not have access")), store)
        delays = [p.cycle() for _ in range(lp.FAILURES_BEFORE_ALERT)]
        self.assertEqual(delays[0], lp.POLL_SECONDS * 2)
        self.assertTrue(all(a <= b for a, b in zip(delays, delays[1:])))
        self.assertEqual(alerts, ["✗ Ballistics live scores: provider failing"])

    def test_low_quota_slows_then_pauses(self):
        store = FakeStore(window=[{"fixture_id": 1, "kickoff_utc": NOW, "status": "NS"}])
        p, _ = poller(FakeProvider(remaining=lp.DAILY_RESERVE - 1), store)
        self.assertEqual(p.cycle(), lp.SLOW_POLL_SECONDS)
        p, alerts = poller(FakeProvider(remaining=lp.DAILY_FLOOR - 1), store)
        self.assertGreater(p.cycle(), 8 * 3600)   # until after 00:00 UTC
        self.assertEqual(len(alerts), 1)


if __name__ == "__main__":
    unittest.main()
