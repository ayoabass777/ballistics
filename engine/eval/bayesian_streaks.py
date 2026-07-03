"""
Bayesian streak engine — Beta-Binomial belief updating per team per event.

Queries fact_team_events (via fact_head_to_head_events) for historical event
flags, computes posterior P(event) for each team using a Beta prior derived
from league-wide base rates, and applies exponential recency weighting so
the posterior tracks recent form.

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

EVENTS_TABLE = "raw.fact_head_to_head_events"
H2H_TABLE = "raw.fact_head_to_head"

HALF_LIFE = 8
PRIOR_STRENGTH = 15.0
MIN_MATCHES = 3


@dataclass
class BayesianBelief:
    team_id: int
    team_name: str
    league: str
    event_name: str
    context: str
    alpha: float
    beta: float
    posterior: float
    league_base_rate: float
    n_matches: int
    recent_streak: int
    trend: str


def _get_conn():
    return psycopg2.connect(
        dbname="ballistics",
        user="ballistics_user",
        password="ronaldo",
        host="localhost",
        port=5432,
    )


def _decay_weights(n: int, half_life: int = HALF_LIFE) -> np.ndarray:
    """Exponential decay weights, most recent match = index n-1 gets weight 1."""
    indices = np.arange(n)
    return np.power(0.5, (n - 1 - indices) / half_life)


def load_event_history(conn, team_id: Optional[int] = None,
                       context: str = "all") -> pd.DataFrame:
    where = ["e.h2h_context_scope = %(context)s"]
    params: dict = {"context": context}

    if team_id is not None:
        where.append("e.team_id = %(team_id)s")
        params["team_id"] = team_id

    sql = f"""
        SELECT
            e.league_name AS league,
            e.team_id,
            e.team_name,
            e.event_name,
            e.event_flag,
            e.kickoff_utc,
            e.fixture_id
        FROM {EVENTS_TABLE} e
        WHERE {' AND '.join(where)}
        ORDER BY e.team_id, e.event_name, e.kickoff_utc, e.fixture_id
    """
    return pd.read_sql(sql, conn, params=params)


def compute_league_base_rates(conn, context: str = "all",
                              current_season_only: bool = True) -> pd.DataFrame:
    season_filter = "AND is_current" if current_season_only else ""
    sql = f"""
        SELECT
            league_name AS league,
            event_name,
            AVG(event_flag::float) AS base_rate,
            COUNT(*) AS n
        FROM {EVENTS_TABLE}
        WHERE h2h_context_scope = %(context)s {season_filter}
        GROUP BY league_name, event_name
    """
    return pd.read_sql(sql, conn, params={"context": context})


def compute_beliefs(
    event_df: pd.DataFrame,
    base_rates: pd.DataFrame,
    prior_strength: float = PRIOR_STRENGTH,
    half_life: int = HALF_LIFE,
    overall_df: pd.DataFrame | None = None,
) -> list[BayesianBelief]:
    rate_lookup = {
        (r["league"], r["event_name"]): r["base_rate"]
        for _, r in base_rates.iterrows()
    }

    # Pre-group overall (all-context) data for streak/trend if provided
    overall_groups = {}
    if overall_df is not None and not overall_df.empty:
        for (tid, ename), grp in overall_df.groupby(["team_id", "event_name"]):
            overall_groups[(tid, ename)] = grp["event_flag"].values.astype(float)

    beliefs = []
    for (team_id, team_name, league, event_name), grp in event_df.groupby(
        ["team_id", "team_name", "league", "event_name"]
    ):
        n = len(grp)
        if n < MIN_MATCHES:
            continue

        base_rate = rate_lookup.get((league, event_name), 0.5)
        alpha_prior = prior_strength * base_rate
        beta_prior = prior_strength * (1 - base_rate)

        flags = grp["event_flag"].values.astype(float)
        weights = _decay_weights(n, half_life)

        weighted_successes = np.sum(flags * weights)
        weighted_total = np.sum(weights)

        alpha_post = alpha_prior + weighted_successes
        beta_post = beta_prior + (weighted_total - weighted_successes)
        posterior = alpha_post / (alpha_post + beta_post)

        # Use overall (all-context) flags for streak/trend when available
        form_flags = overall_groups.get((team_id, event_name), flags)
        fn = len(form_flags)

        tail_flags = form_flags[-min(10, fn):]
        streak = 0
        for f in reversed(tail_flags):
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

        ctx = grp.iloc[0].get("context", "all") if "context" in grp.columns else "all"
        beliefs.append(BayesianBelief(
            team_id=int(team_id),
            team_name=str(team_name),
            league=str(league),
            event_name=str(event_name),
            context=ctx,
            alpha=round(alpha_post, 3),
            beta=round(beta_post, 3),
            posterior=round(posterior, 4),
            league_base_rate=round(base_rate, 4),
            n_matches=n,
            recent_streak=streak,
            trend=trend,
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

MIN_POSTERIOR_INTERESTING = 0.65

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


def calibrate_posterior(raw: float) -> float:
    """Piecewise-linear recalibration from walk-forward backtest."""
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
    half_life: int = HALF_LIFE,
) -> pd.DataFrame:
    """Compute beliefs for teams with upcoming unplayed fixtures."""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today + timedelta(days=days)
    upcoming_sql = """
        SELECT DISTINCT team_id FROM (
            SELECT home_team_id AS team_id FROM raw.raw_fixtures
            WHERE fixture_status NOT IN ('FT','AET','PEN')
              AND kickoff_utc BETWEEN %(today)s AND %(cutoff)s
            UNION
            SELECT away_team_id AS team_id FROM raw.raw_fixtures
            WHERE fixture_status NOT IN ('FT','AET','PEN')
              AND kickoff_utc BETWEEN %(today)s AND %(cutoff)s
        ) t
    """
    upcoming_teams = pd.read_sql(upcoming_sql, conn, params={"today": today, "cutoff": cutoff})
    if upcoming_teams.empty:
        logger.warning("No upcoming fixtures in the next %d days", days)
        return pd.DataFrame()

    rf_ids = upcoming_teams["team_id"].tolist()
    rf_id_list = ",".join(str(i) for i in rf_ids)
    id_map_df = pd.read_sql(f"""
        SELECT DISTINCT r.home_team_id AS rf_id, h.team_id AS h2h_id
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head h ON h.fixture_id = r.fixture_id AND h.is_home
        WHERE r.home_team_id IN ({rf_id_list})
        UNION
        SELECT DISTINCT r.away_team_id AS rf_id, h.team_id AS h2h_id
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head h ON h.fixture_id = r.fixture_id AND NOT h.is_home
        WHERE r.away_team_id IN ({rf_id_list})
    """, conn)
    h2h_ids = set(id_map_df["h2h_id"].tolist())

    event_df = load_event_history(conn, context=context)
    event_df = event_df[event_df["team_id"].isin(h2h_ids)]

    base_rates = compute_league_base_rates(conn, context=context)
    beliefs = compute_beliefs(event_df, base_rates, prior_strength, half_life)
    return beliefs_to_df(beliefs)


def get_fixture_beliefs(
    conn, days: int = 7,
    prior_strength: float = PRIOR_STRENGTH,
    half_life: int = HALF_LIFE,
) -> dict[int, dict]:
    """Return Bayesian beliefs keyed by fixture_id for combined predictor.

    For each upcoming fixture, computes beliefs for both home and away teams
    using the appropriate context (home/away), filters to betting-relevant
    events, and returns a dict: {fixture_id: {"home": [...], "away": [...]}}.
    """
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today + timedelta(days=days)
    fixture_sql = """
        SELECT DISTINCT
            f.fixture_id,
            f.home_team_id  AS team_id,
            f.away_team_id  AS opponent_id,
            f.home_team_name AS team_name,
            f.away_team_name AS opponent_name,
            f.kickoff_utc
        FROM raw.raw_fixtures f
        WHERE f.fixture_status NOT IN ('FT','AET','PEN')
          AND f.kickoff_utc BETWEEN %(today)s AND %(cutoff)s
    """
    fixtures = pd.read_sql(fixture_sql, conn, params={"today": today, "cutoff": cutoff})
    if fixtures.empty:
        return {}

    # raw_fixtures and fact_head_to_head use different team ID spaces.
    # Build a mapping: rf_team_id -> h2h_team_id
    rf_ids = set(fixtures["team_id"].tolist() + fixtures["opponent_id"].tolist())
    rf_id_list = ",".join(str(i) for i in rf_ids)
    id_map_df = pd.read_sql(f"""
        SELECT DISTINCT r.home_team_id AS rf_id, h.team_id AS h2h_id
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head h ON h.fixture_id = r.fixture_id AND h.is_home
        WHERE r.home_team_id IN ({rf_id_list})
        UNION
        SELECT DISTINCT r.away_team_id AS rf_id, h.team_id AS h2h_id
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head h ON h.fixture_id = r.fixture_id AND NOT h.is_home
        WHERE r.away_team_id IN ({rf_id_list})
    """, conn)
    rf_to_h2h = dict(zip(id_map_df["rf_id"], id_map_df["h2h_id"]))
    h2h_team_ids = set(rf_to_h2h.values())
    logger.info("ID map: %d rf_ids -> %d h2h_ids (%d unmapped)",
                len(rf_ids), len(h2h_team_ids), len(rf_ids) - len(rf_to_h2h))

    # Load "all" context events once for streak/trend (overall recent form)
    all_event_df = load_event_history(conn, context="all")
    all_event_df = all_event_df[
        all_event_df["team_id"].isin(h2h_team_ids)
        & all_event_df["event_name"].isin(BETTING_EVENTS)
    ]

    result = {}
    for ctx in ("home", "away"):
        event_df = load_event_history(conn, context=ctx)
        event_df = event_df[
            event_df["team_id"].isin(h2h_team_ids)
            & event_df["event_name"].isin(BETTING_EVENTS)
        ]
        base_rates = compute_league_base_rates(conn, context=ctx)
        beliefs = compute_beliefs(event_df, base_rates, prior_strength, half_life,
                                  overall_df=all_event_df)

        for b in beliefs:
            b.context = ctx

        for _, fix in fixtures.iterrows():
            fid = int(fix["fixture_id"])
            if fid not in result:
                result[fid] = {"home": [], "away": []}

            rf_col = "team_id" if ctx == "home" else "opponent_id"
            rf_tid = int(fix[rf_col])
            h2h_tid = rf_to_h2h.get(rf_tid)
            if h2h_tid is None:
                continue
            side = ctx

            for b in beliefs:
                if b.team_id == h2h_tid and b.posterior >= MIN_POSTERIOR_INTERESTING:
                    result[fid][side].append({
                        "event": b.event_name,
                        "posterior": calibrate_posterior(b.posterior),
                        "posterior_raw": b.posterior,
                        "base_rate": b.league_base_rate,
                        "n": b.n_matches,
                        "streak": b.recent_streak,
                        "trend": b.trend,
                        "team": b.team_name,
                    })

    for fid in list(result):
        if not result[fid]["home"] and not result[fid]["away"]:
            del result[fid]

    return result


def print_beliefs(df: pd.DataFrame, event_filter: Optional[str] = None) -> None:
    if df.empty:
        print("No beliefs computed.")
        return

    if event_filter:
        df = df[df["event_name"].str.contains(event_filter, case=False)]

    print(f"\n{'='*100}")
    print(f"  BAYESIAN STREAK BELIEFS  ({len(df)} team-events)")
    print(f"  Prior strength: {PRIOR_STRENGTH} | Half-life: {HALF_LIFE} matches")
    print(f"{'='*100}")

    for league in sorted(df["league"].unique()):
        lg = df[df["league"] == league]
        print(f"\n── {league} ──")
        for _, row in lg.sort_values(["team_name", "event_name"]).iterrows():
            delta = row["posterior"] - row["league_base_rate"]
            sign = "+" if delta >= 0 else ""
            trend_marker = {"hot": " ↑", "cold": " ↓", "stable": ""}[row["trend"]]
            streak_str = f" [{row['recent_streak']}]" if row["recent_streak"] >= 2 else ""
            print(
                f"  {row['team_name']:<25s}  {row['event_name']:<16s}  "
                f"P={row['posterior']:.0%}  base={row['league_base_rate']:.0%}  "
                f"({sign}{delta:.0%})  n={row['n_matches']}"
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
            beliefs = compute_beliefs(event_df, base_rates)
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
