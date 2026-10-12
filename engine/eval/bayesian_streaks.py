"""
Bayesian streak engine — Beta-Binomial belief updating per team per event.

Queries fact_team_events (via fact_head_to_head_events) for CURRENT-SEASON
event flags, and computes posterior P(event) for each team from raw counts:
k successes out of n matches, with a Beta prior derived from league-wide
base rates:  P = (alpha0 + k) / (alpha0 + beta0 + n).

Each belief is labelled with its streak context ("home", "away" or "all").
For upcoming fixtures, both the venue context (home team -> home games,
away team -> away games) and the overall ("all") context are computed, and
the highest-probability belief per event is kept, with n and k reported
alongside so the numbers are verifiable.

The output is a calibrated probability per (team, event, context) triple
that can feed into the combined predictor alongside Poisson and Mismatch.

Usage:
    python -m engine.eval.bayesian_streaks
    python -m engine.eval.bayesian_streaks --team-id 42 --context home
    python -m engine.eval.bayesian_streaks --upcoming --days 7
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2

from etl.src.logger import get_logger

logger = get_logger(__name__)

EVENTS_TABLE = "raw.engine_head_to_head_events"
H2H_TABLE = "raw.engine_head_to_head"
from engine.eval.db import FIXTURES_TABLE  # noqa: E402  (warehouse team ids)

PRIOR_STRENGTH = 15.0
MIN_MATCHES = 3


@dataclass
class BayesianBelief:
    team_id: int
    team_name: str
    league: str
    event_name: str
    context: str  # streak type: "home", "away" or "all"
    alpha: float
    beta: float
    posterior: float
    league_base_rate: float
    n_matches: int  # n — total matches in this context (current season)
    k_matches: int  # k — matches where the streak event applied
    recent_streak: int
    trend: str
    last_event_utc: object = None  # most-recent event in this league; picks current division


def _get_conn():
    from engine.eval.db import get_conn
    return get_conn()


def _event_ts(t) -> int:
    """Sort key (epoch ns, UTC) for picking a team's most-recent league. NaT/None -> 0."""
    if t is None or pd.isna(t):
        return 0
    return pd.Timestamp(t).value


def load_event_history(conn, team_id: Optional[int] = None,
                       context: str = "all",
                       current_season_only: bool = True) -> pd.DataFrame:
    where = ["e.h2h_context_scope = %(context)s"]
    params: dict = {"context": context}

    if current_season_only:
        where.append("e.is_current")

    if team_id is not None:
        where.append("e.team_id = %(team_id)s")
        params["team_id"] = team_id

    sql = f"""
        SELECT
            dl.league_name || ' (' || dc.country_name || ')' AS league,
            e.team_id,
            e.team_name,
            e.event_name,
            e.event_flag,
            e.kickoff_utc,
            e.fixture_id
        FROM {EVENTS_TABLE} e
        JOIN dim.dim_league_seasons dls ON dls.league_season_id = e.league_season_id
        JOIN dim.dim_leagues dl ON dl.league_id = dls.league_id
        JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
        WHERE {' AND '.join(where)}
        ORDER BY e.team_id, e.event_name, e.kickoff_utc, e.fixture_id
    """
    return pd.read_sql(sql, conn, params=params)


def compute_league_base_rates(conn, context: str = "all",
                              current_season_only: bool = True) -> pd.DataFrame:
    season_filter = "AND e.is_current" if current_season_only else ""
    sql = f"""
        SELECT
            dl.league_name || ' (' || dc.country_name || ')' AS league,
            e.event_name,
            AVG(e.event_flag::float) AS base_rate,
            COUNT(*) AS n
        FROM {EVENTS_TABLE} e
        JOIN dim.dim_league_seasons dls ON dls.league_season_id = e.league_season_id
        JOIN dim.dim_leagues dl ON dl.league_id = dls.league_id
        JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
        WHERE e.h2h_context_scope = %(context)s {season_filter}
        GROUP BY 1, e.event_name
    """
    return pd.read_sql(sql, conn, params={"context": context})


def compute_beliefs(
    event_df: pd.DataFrame,
    base_rates: pd.DataFrame,
    prior_strength: float = PRIOR_STRENGTH,
    context: str = "all",
) -> list[BayesianBelief]:
    rate_lookup = {
        (r["league"], r["event_name"]): r["base_rate"]
        for _, r in base_rates.iterrows()
    }

    beliefs = []
    for (team_id, team_name, league, event_name), grp in event_df.groupby(
        ["team_id", "team_name", "league", "event_name"]
    ):
        grp = grp[grp["event_flag"].notna()]
        n = len(grp)
        if n < MIN_MATCHES:
            continue

        base_rate = rate_lookup.get((league, event_name), 0.5)
        alpha_prior = prior_strength * base_rate
        beta_prior = prior_strength * (1 - base_rate)

        flags = grp["event_flag"].values.astype(float)
        k = int(flags.sum())

        alpha_post = alpha_prior + k
        beta_post = beta_prior + (n - k)
        posterior = alpha_post / (alpha_post + beta_post)

        # Streak/trend from the same context-scoped flags as k/n, so a HOME
        # row's streak counts home matches only (user decision, July 22 2026).
        form_flags = flags
        fn = len(form_flags)

        # Count the full trailing run in this season/context, not just the
        # latest ten matches. The probability boost has its own separate cap.
        streak = 0
        for f in reversed(form_flags):
            if f == 1:
                streak += 1
            else:
                break

        recent_rate = np.mean(form_flags[-min(5, fn):])
        if recent_rate > posterior + 0.10:
            trend = "hot"
        elif recent_rate < posterior - 0.10:
            trend = "cold"
        else:
            trend = "stable"

        last_event = None
        if "kickoff_utc" in grp.columns:
            last_event = pd.to_datetime(grp["kickoff_utc"], utc=True).max()
        beliefs.append(BayesianBelief(
            team_id=int(team_id),
            team_name=str(team_name),
            league=str(league),
            event_name=str(event_name),
            context=context,
            alpha=round(alpha_post, 3),
            beta=round(beta_post, 3),
            posterior=round(posterior, 4),
            league_base_rate=round(base_rate, 4),
            n_matches=n,
            k_matches=k,
            recent_streak=streak,
            trend=trend,
            last_event_utc=last_event,
        ))

    return beliefs


def beliefs_to_df(beliefs: list[BayesianBelief]) -> pd.DataFrame:
    if not beliefs:
        return pd.DataFrame()
    return pd.DataFrame([b.__dict__ for b in beliefs])


BETTING_EVENTS = {
    "score_1goal", "score_2goals", "concede_1", "clean_sheet",
    "Over_1_5", "Over_2_5", "Over_3_5", "Under_2_5", "Under_3_5",
    "win", "loss", "unbeaten", "BTTS",
}

# Display gate: show every streak event whose final score (calibrated
# posterior + streak boost) is at least this. Applied to the displayed
# value, not the raw posterior, so the cutoff matches what the user sees.
MIN_POSTERIOR_INTERESTING = 0.75

# Context selection: prefer the venue-specific belief (home/away) once it has
# a mature sample of its own; below this, fall back to the overall belief.
# Note the overall context always has >= the venue's n (venue is a subset),
# so "most data" alone would always pick "all" — this keeps venue streaks
# usable without cherry-picking the higher probability.
VENUE_PREFER_MIN = 6

# Streak boost: an active streak of >= 5 straight matches (overall form) tilts
# the belief up in log-odds space, capped so it can't run away.
STREAK_BOOST_MIN = 5
STREAK_BOOST_STEP = 0.10   # log-odds per streak match at/beyond the minimum
STREAK_BOOST_CAP = 0.30    # max log-odds tilt (streak 7+)
STREAK_BOOST_P_CEIL = 0.97


def apply_streak_boost(p: float, streak: int) -> float:
    """Boost probability p for an active streak of >= STREAK_BOOST_MIN matches.

    Works in log-odds so the same tilt means less as p approaches 1:
    at p=0.75 a 7-match streak adds ~5pp, at p=0.90 only ~2.5pp.
    """
    if streak < STREAK_BOOST_MIN or p >= STREAK_BOOST_P_CEIL:
        return p
    tilt = min(STREAK_BOOST_CAP, STREAK_BOOST_STEP * (streak - STREAK_BOOST_MIN + 1))
    log_odds = np.log(p / (1 - p)) + tilt
    return min(STREAK_BOOST_P_CEIL, float(1 / (1 + np.exp(-log_odds))))

# Legacy fallback map — superseded by calibration_maps.json (fitted by
# calibration_wf on held-out folds). Kept only for when that file is absent.
CALIBRATION_MAP = [
    (0.65, 0.628),
    (0.70, 0.671),
    (0.75, 0.723),
    (0.80, 0.789),
    (0.85, 0.845),
    (0.90, 0.850),
    (0.95, 0.829),
    (1.00, 0.900),
]

from engine.eval.calibration_maps import apply_points, load_points

_BAYESIAN_POINTS = load_points("bayesian")


def calibrate_posterior(raw: float) -> float:
    """Recalibrate a raw posterior using the fitted walk-forward map.

    Uses the monotonic map from calibration_maps.json when available;
    falls back to the legacy piecewise CALIBRATION_MAP otherwise.
    """
    if _BAYESIAN_POINTS:
        return apply_points(_BAYESIAN_POINTS, raw)
    if raw <= CALIBRATION_MAP[0][0]:
        return raw
    for i in range(1, len(CALIBRATION_MAP)):
        lo_x, lo_y = CALIBRATION_MAP[i - 1]
        hi_x, hi_y = CALIBRATION_MAP[i]
        if raw <= hi_x:
            t = (raw - lo_x) / (hi_x - lo_x)
            return lo_y + t * (hi_y - lo_y)
    return raw


def get_upcoming_beliefs(
    conn, days: int = 7, context: str = "all",
    prior_strength: float = PRIOR_STRENGTH,
) -> pd.DataFrame:
    """Compute beliefs for teams with upcoming unplayed fixtures."""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today + timedelta(days=days)
    upcoming_sql = f"""
        SELECT DISTINCT team_id FROM (
            SELECT home_team_id AS team_id FROM {FIXTURES_TABLE}
            WHERE fixture_status NOT IN ('FT','AET','PEN')
              AND kickoff_utc BETWEEN %(today)s AND %(cutoff)s
            UNION
            SELECT away_team_id AS team_id FROM {FIXTURES_TABLE}
            WHERE fixture_status NOT IN ('FT','AET','PEN')
              AND kickoff_utc BETWEEN %(today)s AND %(cutoff)s
        ) t
        WHERE team_id IS NOT NULL
    """
    upcoming_teams = pd.read_sql(upcoming_sql, conn, params={"today": today, "cutoff": cutoff})
    if upcoming_teams.empty:
        logger.warning("No upcoming fixtures in the next %d days", days)
        return pd.DataFrame()

    h2h_ids = set(int(t) for t in upcoming_teams["team_id"].tolist())

    event_df = load_event_history(conn, context=context)
    event_df = event_df[event_df["team_id"].isin(h2h_ids)]

    base_rates = compute_league_base_rates(conn, context=context)
    beliefs = compute_beliefs(event_df, base_rates, prior_strength, context=context)
    return beliefs_to_df(beliefs)


def _index_latest_by_event(beliefs: list[BayesianBelief]) -> dict[int, dict[str, BayesianBelief]]:
    """Index beliefs as {team_id: {event_name: belief}}.

    A team can have history in multiple divisions (promotion/relegation),
    producing one belief per (league, event). Keep only the belief from the
    team's most-recently-active league so lower-division form doesn't leak
    in and events aren't duplicated.
    """
    idx: dict[int, dict[str, BayesianBelief]] = {}
    for b in beliefs:
        by_event = idx.setdefault(b.team_id, {})
        prev = by_event.get(b.event_name)
        if prev is None or _event_ts(b.last_event_utc) > _event_ts(prev.last_event_utc):
            by_event[b.event_name] = b
    return idx


def get_fixture_beliefs(
    conn, days: int = 7,
    prior_strength: float = PRIOR_STRENGTH,
    include_below_threshold: bool = False,
) -> dict[int, dict]:
    """Return Bayesian beliefs keyed by fixture_id for combined predictor.

    For each upcoming fixture, computes current-season beliefs for both teams
    in two contexts — the venue context (home team -> home games, away team ->
    away games) and the overall "all" context. Per event, the venue belief is
    primary when its sample is mature (n >= VENUE_PREFER_MIN), the overall
    belief otherwise; a mature venue primary keeps the overall belief as a
    secondary entry (primary=False). Each is labelled with its context and
    raw k/n counts. Consensus consumers should use primary entries only.
    Active streaks >= STREAK_BOOST_MIN add a capped log-odds boost on top of
    the calibrated posterior. By default only beliefs at or above
    MIN_POSTERIOR_INTERESTING are returned. Set include_below_threshold=True
    for fixture-analysis/audit consumers that need every computed event and
    its display eligibility. Returns:
    {fixture_id: {"home": [...], "away": [...]}}.
    """
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today + timedelta(days=days)
    fixture_sql = f"""
        SELECT DISTINCT
            f.fixture_id,
            f.home_team_id  AS team_id,
            f.away_team_id  AS opponent_id,
            f.home_team_name AS team_name,
            f.away_team_name AS opponent_name,
            f.kickoff_utc
        FROM {FIXTURES_TABLE} f
        WHERE f.fixture_status NOT IN ('FT','AET','PEN')
          AND f.kickoff_utc BETWEEN %(today)s AND %(cutoff)s
    """
    fixtures = pd.read_sql(fixture_sql, conn, params={"today": today, "cutoff": cutoff})
    if fixtures.empty:
        return {}

    # fact_fixtures carries warehouse team ids (same space as the event
    # history); NULL means the team is not in the dimensions yet.
    team_cols = fixtures[["team_id", "opponent_id"]]
    h2h_team_ids = {int(t) for t in team_cols.stack().dropna().tolist()}
    n_unmapped = int(team_cols.isna().sum().sum())
    if n_unmapped:
        logger.info("%d fixture sides have no warehouse team id", n_unmapped)

    # All event types are included; consumers that feed the consensus pool
    # filter to BETTING_EVENTS themselves. Streak/trend are scoped to each
    # belief's own context (a HOME row shows a home-only streak).
    all_event_df = load_event_history(conn, context="all")
    all_event_df = all_event_df[all_event_df["team_id"].isin(h2h_team_ids)]

    idx_by_ctx: dict[str, dict[int, dict[str, BayesianBelief]]] = {}
    base_rates_all = compute_league_base_rates(conn, context="all")
    idx_by_ctx["all"] = _index_latest_by_event(compute_beliefs(
        all_event_df, base_rates_all, prior_strength, context="all"))

    for ctx in ("home", "away"):
        event_df = load_event_history(conn, context=ctx)
        event_df = event_df[event_df["team_id"].isin(h2h_team_ids)]
        base_rates = compute_league_base_rates(conn, context=ctx)
        idx_by_ctx[ctx] = _index_latest_by_event(compute_beliefs(
            event_df, base_rates, prior_strength, context=ctx))

    result = {}
    for _, fix in fixtures.iterrows():
        fid = int(fix["fixture_id"])
        entry = {"home": [], "away": []}

        for side in ("home", "away"):
            team_col = "team_id" if side == "home" else "opponent_id"
            if pd.isna(fix[team_col]):
                continue
            h2h_tid = int(fix[team_col])

            venue = idx_by_ctx[side].get(h2h_tid, {})
            overall = idx_by_ctx["all"].get(h2h_tid, {})

            # Per event: the venue-context belief is primary when its sample
            # is mature (n >= VENUE_PREFER_MIN), else the overall belief is.
            # Selection depends only on data volume, never on the probability
            # itself, so it avoids the optimism of picking the max posterior.
            # When a venue belief is primary, the overall belief is kept too
            # (primary=False) so the all-context streak stays visible —
            # consumers that feed consensus should use primary entries only.
            for event_name in set(venue) | set(overall):
                venue_b = venue.get(event_name)
                overall_b = overall.get(event_name)
                if venue_b is not None and venue_b.n_matches >= VENUE_PREFER_MIN:
                    chosen = [(venue_b, True)]
                    if overall_b is not None:
                        chosen.append((overall_b, False))
                else:
                    chosen = [(overall_b or venue_b, True)]
                for b, primary in chosen:
                    calibrated = calibrate_posterior(b.posterior)
                    boosted = apply_streak_boost(calibrated, b.recent_streak)
                    display_eligible = boosted >= MIN_POSTERIOR_INTERESTING
                    if not display_eligible and not include_below_threshold:
                        continue
                    entry[side].append({
                        "event": b.event_name,
                        "posterior": boosted,
                        "posterior_raw": b.posterior,
                        "posterior_calibrated": calibrated,
                        "boost": round(boosted - calibrated, 4) if boosted > calibrated else 0,
                        "base_rate": b.league_base_rate,
                        "alpha": b.alpha,
                        "beta": b.beta,
                        "n": b.n_matches,
                        "k": b.k_matches,
                        "league": b.league,
                        "context": b.context,
                        "primary": primary,
                        "display_eligible": display_eligible,
                        "display_threshold": MIN_POSTERIOR_INTERESTING,
                        "streak": b.recent_streak,
                        "trend": b.trend,
                        "team": b.team_name,
                    })

            entry[side].sort(key=lambda d: d["posterior"], reverse=True)

        if entry["home"] or entry["away"]:
            result[fid] = entry

    return result


def print_beliefs(df: pd.DataFrame, event_filter: Optional[str] = None) -> None:
    if df.empty:
        print("No beliefs computed.")
        return

    if event_filter:
        df = df[df["event_name"].str.contains(event_filter, case=False)]

    print(f"\n{'='*100}")
    print(f"  BAYESIAN STREAK BELIEFS  ({len(df)} team-events)  — current season only")
    print(f"  Prior strength: {PRIOR_STRENGTH} | posterior = (a0 + k) / (a0 + b0 + n)")
    print(f"{'='*100}")

    for league in sorted(df["league"].unique()):
        lg = df[df["league"] == league]
        print(f"\n── {league} ──")
        for _, row in lg.sort_values(["team_name", "event_name"]).iterrows():
            delta = row["posterior"] - row["league_base_rate"]
            sign = "+" if delta >= 0 else ""
            trend_marker = {"hot": " ↑", "cold": " ↓", "stable": ""}[row["trend"]]
            streak_str = f" [{row['recent_streak']}]" if row["recent_streak"] >= 2 else ""
            ctx = str(row["context"]).upper()
            print(
                f"  {row['team_name']:<25s}  {row['event_name']:<16s}  [{ctx:<4s}]  "
                f"P={row['posterior']:.0%}  base={row['league_base_rate']:.0%}  "
                f"({sign}{delta:.0%})  k={row['k_matches']}/n={row['n_matches']}"
                f"{streak_str}{trend_marker}"
            )


def run(
    team_id: Optional[int] = None,
    context: str = "all",
    upcoming: bool = False,
    days: int = 7,
    event_filter: Optional[str] = None,
    conn=None,
) -> pd.DataFrame:
    close_conn = False
    if conn is None:
        conn = _get_conn()
        close_conn = True

    try:
        if upcoming:
            df = get_upcoming_beliefs(conn, days=days, context=context)
        else:
            event_df = load_event_history(conn, team_id=team_id, context=context)
            base_rates = compute_league_base_rates(conn, context=context)
            beliefs = compute_beliefs(event_df, base_rates, context=context)
            df = beliefs_to_df(beliefs)

        print_beliefs(df, event_filter=event_filter)
        return df
    finally:
        if close_conn:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bayesian streak beliefs")
    parser.add_argument("--team-id", type=int, help="Filter to one team")
    parser.add_argument("--context", default="all",
                        choices=["all", "home", "away"])
    parser.add_argument("--upcoming", action="store_true",
                        help="Only teams with upcoming fixtures")
    parser.add_argument("--days", type=int, default=7,
                        help="Days ahead for --upcoming")
    parser.add_argument("--event", type=str, default=None,
                        help="Filter events (substring match)")
    args = parser.parse_args()

    run(
        team_id=args.team_id,
        context=args.context,
        upcoming=args.upcoming,
        days=args.days,
        event_filter=args.event,
    )
