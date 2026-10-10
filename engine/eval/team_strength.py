"""
Team attack/defense percentiles — within-league measurement.

Computes per-game scoring and conceding rates for every team, applies
empirical-Bayes shrinkage toward the league mean so small-sample teams
don't get extreme percentiles, then percentile-ranks each team within
its own league.

Higher attack_percentile  = more goals scored (stronger attack).
Higher defense_percentile = fewer goals conceded (stronger defense).

Usage:
    python -m engine.eval.team_strength
    python -m engine.eval.team_strength --csv DIR
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import percentileofscore

from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)

SOURCE_TABLE = "raw.fact_head_to_head"
MIN_GAMES_CONFIDENT: int = 6


# ── data ────────────────────────────────────────────────────────────────────

def _with_league_key(df: pd.DataFrame) -> pd.DataFrame:
    """Rank within the real league-season, not just the display league name."""
    out = df.copy()
    if "league_season_id" in out.columns:
        out["_league_key"] = out["league_season_id"].astype(str)
    else:
        out["_league_key"] = out["league"].astype(str)
    return out


def load_team_df(conn, current_season_only: bool = True,
                 cutoff=None) -> pd.DataFrame:
    """Load played team-match rows. `cutoff` restricts to kickoffs before
    that datetime — required for walk-forward backtests so strength never
    sees matches after the fold's training cutoff."""
    season_filter = "AND f.is_current" if current_season_only else ""
    cutoff_filter = "AND f.kickoff_utc < %(cutoff)s" if cutoff is not None else ""
    sql = f"""
        SELECT
            f.league_season_id,
            dl.league_name || ' (' || dc.country_name || ') ' || dls.season AS league,
            f.team_id, f.team_name,
            f.goals_for, f.goals_against,
            f.is_home, f.kickoff_utc
        FROM {SOURCE_TABLE} f
        JOIN dim.dim_league_seasons dls ON dls.league_season_id = f.league_season_id
        JOIN dim.dim_leagues dl ON dl.league_id = dls.league_id
        JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
        WHERE f.is_played {season_filter} {cutoff_filter}
        ORDER BY f.kickoff_utc
    """
    params = {"cutoff": cutoff} if cutoff is not None else None
    df = pd.read_sql(sql, conn, params=params)
    if df.empty and cutoff is None:
        raise RuntimeError("No played matches found.")
    return df


def load_team_df_from_matches(df: pd.DataFrame) -> pd.DataFrame:
    """Build team-level rows from fixture-level DataFrame (for testing).

    Expects columns: league, home_team_id, away_team_id,
    home_goals, away_goals, kickoff_utc.
    """
    home = df[["league", "home_team_id", "home_goals", "away_goals", "kickoff_utc"]].rename(
        columns={"home_team_id": "team_id", "home_goals": "goals_for",
                 "away_goals": "goals_against"})
    home["is_home"] = True
    away = df[["league", "away_team_id", "away_goals", "home_goals", "kickoff_utc"]].rename(
        columns={"away_team_id": "team_id", "away_goals": "goals_for",
                 "home_goals": "goals_against"})
    away["is_home"] = False
    return pd.concat([home, away], ignore_index=True).sort_values("kickoff_utc")


# ── empirical-Bayes shrinkage ───────────────────────────────────────────────

def _shrink_rate(observed_total: float, n_games: int,
                 league_mean: float, league_var: float) -> float:
    """Shrink an observed per-game rate toward the league mean.

    Gamma-Poisson shrinkage: goal rates are Poisson counts, so the
    conjugate prior is Gamma(alpha, beta) with mean m = alpha/beta and
    between-team variance v = alpha/beta². Moment matching gives
    beta = m/v, and beta acts as the prior strength in games (kappa).
    Shrunk rate = (kappa * league_mean + total) / (kappa + n).

    (The previous Beta-Binomial formula m(1-m)/v assumed a proportion in
    [0,1]; goal rates exceed 1, so it went negative and clamped to a
    one-game prior — i.e. shrinkage was effectively disabled.)
    """
    if n_games == 0:
        return league_mean
    observed_mean = observed_total / n_games
    if league_var <= 0 or league_mean <= 0:
        return observed_mean
    kappa = max(league_mean / league_var, 1.0)
    return (kappa * league_mean + n_games * observed_mean) / (kappa + n_games)


# ── core computation ────────────────────────────────────────────────────────

def _compute_split_agg(df: pd.DataFrame, is_home: bool) -> pd.DataFrame:
    """Aggregate attack/defense for home-only or away-only games."""
    split = df[df["is_home"] == is_home]
    if split.empty:
        return pd.DataFrame()
    agg_fields = {
        "league": ("league", "first"),
        "games": ("goals_for", "count"),
        "scored": ("goals_for", "sum"),
        "conceded": ("goals_against", "sum"),
    }
    if "league_season_id" in split.columns:
        agg_fields["league_season_id"] = ("league_season_id", "first")
    agg = (
        split.groupby(["_league_key", "team_id"])
        .agg(**agg_fields)
        .reset_index()
    )
    agg["attack_rate"] = agg["scored"] / agg["games"]
    agg["defense_rate"] = agg["conceded"] / agg["games"]
    return agg


def _shrink_and_percentile(
    agg: pd.DataFrame, league_stats: pd.DataFrame,
) -> pd.DataFrame:
    """Apply shrinkage + within-league percentile to an aggregated DataFrame."""
    agg = agg.merge(league_stats, on="_league_key", how="left")
    agg["attack_shrunk"] = agg.apply(
        lambda r: _shrink_rate(
            r["scored"], r["games"],
            r["league_attack_mean"], r["league_attack_var"]),
        axis=1)
    agg["defense_shrunk"] = agg.apply(
        lambda r: _shrink_rate(
            r["conceded"], r["games"],
            r["league_defense_mean"], r["league_defense_var"]),
        axis=1)

    rows = []
    for _, grp in agg.groupby("_league_key"):
        if len(grp) < 2:
            continue
        league = grp["league"].iloc[0]
        atk_vals = grp["attack_shrunk"].values
        def_vals = grp["defense_shrunk"].values
        league_size = len(grp)

        atk_ranked = grp.sort_values("attack_shrunk", ascending=False).reset_index(drop=True)
        def_ranked = grp.sort_values("defense_shrunk", ascending=True).reset_index(drop=True)
        atk_rank_map = {r["team_id"]: i + 1 for i, (_, r) in enumerate(atk_ranked.iterrows())}
        def_rank_map = {r["team_id"]: i + 1 for i, (_, r) in enumerate(def_ranked.iterrows())}

        for _, row in grp.iterrows():
            atk_pct = percentileofscore(atk_vals, row["attack_shrunk"], kind="rank")
            def_pct = percentileofscore(def_vals, row["defense_shrunk"], kind="rank")
            def_pct = 100.0 - def_pct + (100.0 / len(def_vals))
            def_pct = min(max(def_pct, 0.0), 100.0)
            rows.append({
                "_league_key": row["_league_key"],
                "league": league,
                "league_season_id": row.get("league_season_id"),
                "team_id": row["team_id"],
                "attack_rate": round(row["attack_rate"], 3),
                "defense_rate": round(row["defense_rate"], 3),
                "attack_shrunk": round(row["attack_shrunk"], 3),
                "defense_shrunk": round(row["defense_shrunk"], 3),
                "attack_percentile": round(atk_pct, 1),
                "defense_percentile": round(def_pct, 1),
                "attack_rank": atk_rank_map[row["team_id"]],
                "defense_rank": def_rank_map[row["team_id"]],
                "league_size": league_size,
            })
    return pd.DataFrame(rows)


def _league_stats(agg: pd.DataFrame) -> pd.DataFrame:
    """Per-league mean/var of attack_rate & defense_rate — the shrinkage prior.

    Computed from whatever slice is passed (overall, home-only, or away-only) so
    each context shrinks toward its OWN baseline, not the overall one.
    """
    return (
        agg.groupby("_league_key")
        .agg(
            league_attack_mean=("attack_rate", "mean"),
            league_attack_var=("attack_rate", "var"),
            league_defense_mean=("defense_rate", "mean"),
            league_defense_var=("defense_rate", "var"),
        )
        .reset_index()
    )


def compute_team_strength(df: pd.DataFrame, split_prior: str = "own") -> pd.DataFrame:
    """split_prior: 'own' (correct) shrinks home/away splits toward their own
    home/away league baseline; 'overall' reproduces the pre-fix behaviour
    (splits shrunk toward the overall mean) — used only for the gate backtest."""
    """Compute attack/defense percentiles for every team within its league.

    Returns overall, home-only, and away-only strength metrics.
    Home/away columns are prefixed with home_ / away_.

    Parameters
    ----------
    df : DataFrame with columns: league, team_id, goals_for, goals_against, is_home.
         One row per team per match (both home and away appearances).

    Returns
    -------
    DataFrame with columns: league, team_id, games_played, attack_rate,
    defense_rate, attack_shrunk, defense_shrunk, attack_percentile,
    defense_percentile, low_confidence, plus home_attack_shrunk,
    home_defense_shrunk, home_attack_percentile, home_defense_percentile,
    away_attack_shrunk, away_defense_shrunk, away_attack_percentile,
    away_defense_percentile.
    """
    df = _with_league_key(df)

    # Overall aggregation
    agg_fields = {
        "league": ("league", "first"),
        "games_played": ("goals_for", "count"),
        "total_scored": ("goals_for", "sum"),
        "total_conceded": ("goals_against", "sum"),
    }
    if "league_season_id" in df.columns:
        agg_fields["league_season_id"] = ("league_season_id", "first")
    team_agg = (
        df.groupby(["_league_key", "team_id"])
        .agg(**agg_fields)
        .reset_index()
    )
    team_agg["attack_rate"] = team_agg["total_scored"] / team_agg["games_played"]
    team_agg["defense_rate"] = team_agg["total_conceded"] / team_agg["games_played"]

    league_stats = _league_stats(team_agg)
    team_agg = team_agg.merge(league_stats, on="_league_key")

    team_agg["attack_shrunk"] = team_agg.apply(
        lambda r: _shrink_rate(
            r["total_scored"], r["games_played"],
            r["league_attack_mean"], r["league_attack_var"]),
        axis=1)
    team_agg["defense_shrunk"] = team_agg.apply(
        lambda r: _shrink_rate(
            r["total_conceded"], r["games_played"],
            r["league_defense_mean"], r["league_defense_var"]),
        axis=1)

    rows = []
    for _, grp in team_agg.groupby("_league_key"):
        league = grp["league"].iloc[0]
        attack_vals = grp["attack_shrunk"].values
        defense_vals = grp["defense_shrunk"].values
        league_size = len(grp)

        atk_ranked = grp.sort_values("attack_shrunk", ascending=False).reset_index(drop=True)
        def_ranked = grp.sort_values("defense_shrunk", ascending=True).reset_index(drop=True)
        atk_rank_map = {row["team_id"]: i + 1 for i, (_, row) in enumerate(atk_ranked.iterrows())}
        def_rank_map = {row["team_id"]: i + 1 for i, (_, row) in enumerate(def_ranked.iterrows())}

        for _, row in grp.iterrows():
            atk_pct = percentileofscore(attack_vals, row["attack_shrunk"], kind="rank")
            def_pct = percentileofscore(
                defense_vals, row["defense_shrunk"], kind="rank")
            # invert defense: lower conceded = higher percentile
            def_pct = 100.0 - def_pct + (100.0 / len(defense_vals))
            def_pct = min(max(def_pct, 0.0), 100.0)

            rows.append({
                "_league_key": row["_league_key"],
                "league": league,
                "league_season_id": row.get("league_season_id"),
                "team_id": row["team_id"],
                "games_played": int(row["games_played"]),
                "attack_rate": round(row["attack_rate"], 3),
                "defense_rate": round(row["defense_rate"], 3),
                "attack_shrunk": round(row["attack_shrunk"], 3),
                "defense_shrunk": round(row["defense_shrunk"], 3),
                "attack_percentile": round(atk_pct, 1),
                "defense_percentile": round(def_pct, 1),
                "attack_rank": atk_rank_map[row["team_id"]],
                "defense_rank": def_rank_map[row["team_id"]],
                "league_size": league_size,
                "low_confidence": row["games_played"] < MIN_GAMES_CONFIDENT,
            })

    result = pd.DataFrame(rows)

    # Home/away splits
    home_agg = _compute_split_agg(df, is_home=True)
    away_agg = _compute_split_agg(df, is_home=False)

    if not home_agg.empty:
        # shrink home-only rates toward the league's HOME baseline, not overall
        home_ls = league_stats if split_prior == "overall" else _league_stats(home_agg)
        home_strength = _shrink_and_percentile(home_agg, home_ls)
        home_strength = home_strength.rename(columns={
            "attack_rate": "home_attack_rate",
            "defense_rate": "home_defense_rate",
            "attack_shrunk": "home_attack_shrunk",
            "defense_shrunk": "home_defense_shrunk",
            "attack_percentile": "home_attack_percentile",
            "defense_percentile": "home_defense_percentile",
            "attack_rank": "home_attack_rank",
            "defense_rank": "home_defense_rank",
            "league_size": "home_league_size",
        })
        result = result.merge(
            home_strength[["_league_key", "team_id", "home_attack_rate",
                           "home_defense_rate", "home_attack_shrunk",
                           "home_defense_shrunk", "home_attack_percentile",
                           "home_defense_percentile", "home_attack_rank",
                           "home_defense_rank", "home_league_size"]],
            on=["_league_key", "team_id"], how="left")

    if not away_agg.empty:
        # shrink away-only rates toward the league's AWAY baseline, not overall
        away_ls = league_stats if split_prior == "overall" else _league_stats(away_agg)
        away_strength = _shrink_and_percentile(away_agg, away_ls)
        away_strength = away_strength.rename(columns={
            "attack_rate": "away_attack_rate",
            "defense_rate": "away_defense_rate",
            "attack_shrunk": "away_attack_shrunk",
            "defense_shrunk": "away_defense_shrunk",
            "attack_percentile": "away_attack_percentile",
            "defense_percentile": "away_defense_percentile",
            "attack_rank": "away_attack_rank",
            "defense_rank": "away_defense_rank",
            "league_size": "away_league_size",
        })
        result = result.merge(
            away_strength[["_league_key", "team_id", "away_attack_rate",
                           "away_defense_rate", "away_attack_shrunk",
                           "away_defense_shrunk", "away_attack_percentile",
                           "away_defense_percentile", "away_attack_rank",
                           "away_defense_rank", "away_league_size"]],
            on=["_league_key", "team_id"], how="left")

    return result.drop(columns=["_league_key"], errors="ignore")


# ── reporting ───────────────────────────────────────────────────────────────

def run(csv_dir: Optional[str] = None, conn=None,
        df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    close_conn = False
    if df is None:
        if conn is None:
            conn = get_db_connection()
            close_conn = True
        df = load_team_df(conn)

    result = compute_team_strength(df)

    n_teams = len(result)
    n_leagues = result["league"].nunique()
    n_low = result["low_confidence"].sum()

    print(f"\n{'='*72}")
    print(f"  TEAM ATTACK/DEFENSE PERCENTILES  ({n_teams} teams, {n_leagues} leagues)")
    print(f"  Low-confidence (<{MIN_GAMES_CONFIDENT} games): {n_low}")
    print(f"  Higher attack_percentile = stronger attack (more goals scored)")
    print(f"  Higher defense_percentile = stronger defense (fewer conceded)")
    print(f"{'='*72}\n")

    for league in sorted(result["league"].unique()):
        lg = result[result["league"] == league].sort_values(
            "attack_percentile", ascending=False)
        print(f"── {league} ({len(lg)} teams) ──\n")
        with pd.option_context("display.max_rows", None, "display.width", 200,
                               "display.float_format", "{:.1f}".format):
            display_cols = ["team_id", "games_played", "attack_rate",
                           "defense_rate", "attack_shrunk", "defense_shrunk",
                           "attack_percentile", "attack_rank",
                           "defense_percentile", "defense_rank",
                           "league_size", "low_confidence"]
            print(lg[display_cols].to_string(index=False))
        print()

    if csv_dir:
        out = Path(csv_dir)
        out.mkdir(parents=True, exist_ok=True)
        result.to_csv(out / "team_strength.csv", index=False)
        print(f"CSV written to {out / 'team_strength.csv'}")

    if close_conn:
        conn.close()

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Team attack/defense percentiles (within-league)."
    )
    parser.add_argument("--csv", metavar="DIR",
                        help="Write result CSV to this directory.")
    args = parser.parse_args()
    run(csv_dir=args.csv)
