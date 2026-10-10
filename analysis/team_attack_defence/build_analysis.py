"""Build the league attack/defence analysis used by the companion report.

The analysis deliberately separates two jobs:

* historical description: empirical-Bayes goals-for/goals-against rates for
  every team-season, ranked only against teams in the same league-season;
* current prediction signal: the repository's time-decayed Dixon-Coles model,
  which adjusts each attack and defence for opponent quality and home advantage.

The script is read-only against Postgres. It writes reviewed CSV/JSON snapshots
under ``analysis/team_attack_defence/output`` and refreshes the report's reviewed
data payload when the report project is present.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from engine.eval.db import get_conn
from engine.eval.predict_dixon_coles import XI, fit_league_dc


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
REPORT_DATA_FILE = Path(__file__).resolve().parent / "report" / "src" / "data.json"
REPORT_ID = "report:2b8cf57f-2b55-4974-ab84-47fb906c9e74"
CALIBRATION_REPORT = ROOT / "engine" / "eval" / "calibration_report.json"
STRENGTH_PRIOR_MATCHES = 6.0
ACTIVE_LEAGUE_MAX_STALENESS_DAYS = 120

MATCH_SQL = """
SELECT
    rf.fixture_id,
    rf.api_fixture_id,
    rf.api_league_id,
    dl.league_id,
    dls.league_season_id,
    dc.country_name,
    dl.league_name,
    dl.tier,
    rf.season,
    rf.kickoff_utc,
    ht.team_id AS home_team_id,
    at.team_id AS away_team_id,
    ht.team_name AS home_team,
    at.team_name AS away_team,
    rf.home_team_fulltime_goal AS home_goals,
    rf.away_team_fulltime_goal AS away_goals,
    xg.home_xg,
    xg.away_xg,
    rf.updated_at
FROM raw.raw_fixtures rf
JOIN dim.dim_leagues dl
  ON dl.api_league_id = rf.api_league_id
JOIN dim.dim_countries dc
  ON dc.country_id = dl.country_id
JOIN dim.dim_league_seasons dls
  ON dls.league_id = dl.league_id
 AND dls.season = rf.season
JOIN dim.dim_teams ht
  ON ht.api_team_id = rf.home_team_id
JOIN dim.dim_teams at
  ON at.api_team_id = rf.away_team_id
LEFT JOIN raw.fixture_xg xg
  ON xg.fixture_id = rf.fixture_id
WHERE rf.fixture_status = 'FT'
ORDER BY rf.kickoff_utc, rf.fixture_id
"""

ELO_SQL = """
SELECT DISTINCT ON (team_id)
    team_id,
    rating_after AS elo_rating,
    kickoff_utc AS elo_as_of
FROM elo.team_elo_history
ORDER BY team_id, kickoff_utc DESC, fixture_id DESC
"""


def _json_ready(records: list[dict]) -> list[dict]:
    """Convert pandas/numpy values to strict JSON-compatible primitives."""
    clean = []
    for record in records:
        row = {}
        for key, value in record.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, (np.integer,)):
                row[key] = int(value)
            elif isinstance(value, (np.floating,)):
                row[key] = float(value)
            elif isinstance(value, (pd.Timestamp,)):
                row[key] = value.isoformat()
            else:
                row[key] = value
        clean.append(row)
    return clean


def load_inputs(conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    matches = pd.read_sql(MATCH_SQL, conn)
    matches["kickoff_utc"] = pd.to_datetime(matches["kickoff_utc"], utc=True)
    matches["league"] = matches["country_name"] + " — " + matches["league_name"]
    elo = pd.read_sql(ELO_SQL, conn)
    elo["elo_as_of"] = pd.to_datetime(elo["elo_as_of"], utc=True)
    return matches, elo


def quality_summary(conn, matches: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    raw_checks = pd.read_sql(
        """
        SELECT
            COUNT(*) FILTER (WHERE fixture_status = 'FT') AS ft_fixtures,
            COUNT(*) FILTER (
                WHERE fixture_status = 'FT'
                  AND (home_team_fulltime_goal IS NULL
                       OR away_team_fulltime_goal IS NULL)
            ) AS ft_missing_goals,
            COUNT(*) FILTER (
                WHERE fixture_status = 'FT' AND kickoff_utc > NOW()
            ) AS future_ft,
            COUNT(*) - COUNT(DISTINCT api_fixture_id) AS duplicate_api_fixture_ids,
            MIN(kickoff_utc) FILTER (WHERE fixture_status = 'FT') AS first_ft,
            MAX(kickoff_utc) FILTER (WHERE fixture_status = 'FT') AS last_ft
        FROM raw.raw_fixtures
        """,
        conn,
    ).iloc[0]
    fact_checks = pd.read_sql(
        """
        SELECT
            COUNT(DISTINCT fixture_id) AS fact_fixtures,
            MAX(kickoff_utc) AS fact_last_ft
        FROM raw.fact_head_to_head
        WHERE is_played
        """,
        conn,
    ).iloc[0]

    raw_count = int(raw_checks["ft_fixtures"])
    fact_count = int(fact_checks["fact_fixtures"])
    fact_coverage = fact_count / raw_count if raw_count else math.nan
    last_raw = pd.Timestamp(raw_checks["last_ft"])
    last_fact = pd.Timestamp(fact_checks["fact_last_ft"])

    rows = pd.DataFrame(
        [
            {
                "check": "Finished fixtures with complete goals",
                "value": raw_count - int(raw_checks["ft_missing_goals"]),
                "rate": 1 - int(raw_checks["ft_missing_goals"]) / raw_count,
                "severity": "pass" if int(raw_checks["ft_missing_goals"]) == 0 else "high",
                "interpretation": "Raw FT fixtures are usable for goal-rate analysis.",
            },
            {
                "check": "Duplicate API fixture IDs",
                "value": int(raw_checks["duplicate_api_fixture_ids"]),
                "rate": int(raw_checks["duplicate_api_fixture_ids"]) / len(matches),
                "severity": "pass" if int(raw_checks["duplicate_api_fixture_ids"]) == 0 else "critical",
                "interpretation": "No fixture duplication at the raw source grain.",
            },
            {
                "check": "Future-dated finished fixtures",
                "value": int(raw_checks["future_ft"]),
                "rate": int(raw_checks["future_ft"]) / raw_count,
                "severity": "pass" if int(raw_checks["future_ft"]) == 0 else "high",
                "interpretation": "No obvious future-result leakage in the raw source.",
            },
            {
                "check": "Transformed fact coverage of raw FT fixtures",
                "value": fact_count,
                "rate": fact_coverage,
                "severity": "high" if fact_coverage < 0.95 else "pass",
                "interpretation": "The transformed team-match fact is incomplete, so this analysis uses raw fixtures.",
            },
            {
                "check": "Transformed fact freshness lag (days)",
                "value": int((last_raw - last_fact).days),
                "rate": None,
                "severity": "medium" if last_raw > last_fact else "pass",
                "interpretation": "The raw source is fresher than the transformed fact.",
            },
        ]
    )
    summary = {
        "raw_ft_fixtures": raw_count,
        "raw_first_ft": pd.Timestamp(raw_checks["first_ft"]).isoformat(),
        "raw_last_ft": last_raw.isoformat(),
        "fact_fixtures": fact_count,
        "fact_coverage": round(float(fact_coverage), 4),
        "fact_last_ft": last_fact.isoformat(),
    }
    return rows, summary


def make_team_match_rows(matches: pd.DataFrame) -> pd.DataFrame:
    common = [
        "fixture_id", "api_league_id", "league_id", "league_season_id",
        "country_name", "league_name", "league", "tier", "season", "kickoff_utc",
    ]
    home = matches[common + ["home_team_id", "home_team", "home_goals", "away_goals"]].rename(
        columns={
            "home_team_id": "team_id",
            "home_team": "team_name",
            "home_goals": "goals_for",
            "away_goals": "goals_against",
        }
    )
    home["is_home"] = True
    away = matches[common + ["away_team_id", "away_team", "away_goals", "home_goals"]].rename(
        columns={
            "away_team_id": "team_id",
            "away_team": "team_name",
            "away_goals": "goals_for",
            "home_goals": "goals_against",
        }
    )
    away["is_home"] = False
    return pd.concat([home, away], ignore_index=True)


def _gamma_poisson_shrink(total: pd.Series, games: pd.Series,
                          league_mean: pd.Series, league_var: pd.Series) -> pd.Series:
    observed = total / games
    kappa = (league_mean / league_var.replace(0, np.nan)).clip(lower=1.0)
    shrunk = (kappa * league_mean + total) / (kappa + games)
    return shrunk.fillna(observed)


def historical_strength(team_matches: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    keys = [
        "api_league_id", "league_season_id", "country_name", "league_name",
        "league", "tier", "season", "team_id", "team_name",
    ]
    agg = (
        team_matches.groupby(keys, dropna=False)
        .agg(
            games=("fixture_id", "nunique"),
            goals_for=("goals_for", "sum"),
            goals_against=("goals_against", "sum"),
        )
        .reset_index()
    )
    agg["goals_for_per_game"] = agg["goals_for"] / agg["games"]
    agg["goals_against_per_game"] = agg["goals_against"] / agg["games"]
    league_keys = ["league_season_id"]
    stats = (
        agg.groupby(league_keys)
        .agg(
            attack_mean=("goals_for_per_game", "mean"),
            attack_var=("goals_for_per_game", "var"),
            defence_mean=("goals_against_per_game", "mean"),
            defence_var=("goals_against_per_game", "var"),
        )
        .reset_index()
    )
    agg = agg.merge(stats, on="league_season_id", how="left")
    agg["attack_rate_eb"] = _gamma_poisson_shrink(
        agg["goals_for"], agg["games"], agg["attack_mean"], agg["attack_var"]
    )
    agg["defence_conceded_eb"] = _gamma_poisson_shrink(
        agg["goals_against"], agg["games"], agg["defence_mean"], agg["defence_var"]
    )
    agg["attack_rank"] = agg.groupby("league_season_id")["attack_rate_eb"].rank(
        ascending=False, method="min"
    ).astype(int)
    agg["defence_rank"] = agg.groupby("league_season_id")["defence_conceded_eb"].rank(
        ascending=True, method="min"
    ).astype(int)
    agg["attack_percentile"] = agg.groupby("league_season_id")["attack_rate_eb"].rank(
        pct=True, method="average"
    ) * 100
    agg["defence_percentile"] = (
        1 - agg.groupby("league_season_id")["defence_conceded_eb"].rank(
            pct=True, method="average"
        )
    ) * 100 + 100 / agg.groupby("league_season_id")["team_id"].transform("count")

    for column in [
        "goals_for_per_game", "goals_against_per_game", "attack_rate_eb",
        "defence_conceded_eb", "attack_percentile", "defence_percentile",
    ]:
        agg[column] = agg[column].round(3)

    leaders = []
    for (_, _), group in agg.groupby(["api_league_id", "season"]):
        attack = group.sort_values(["attack_rank", "team_name"]).iloc[0]
        defence = group.sort_values(["defence_rank", "team_name"]).iloc[0]
        leaders.append(
            {
                "country": attack["country_name"],
                "league": attack["league_name"],
                "league_label": attack["league"],
                "api_league_id": int(attack["api_league_id"]),
                "season": int(attack["season"]),
                "attack_leader": attack["team_name"],
                "attack_goals_per_game_eb": round(float(attack["attack_rate_eb"]), 3),
                "attack_games": int(attack["games"]),
                "defence_leader": defence["team_name"],
                "defence_goals_allowed_per_game_eb": round(float(defence["defence_conceded_eb"]), 3),
                "defence_games": int(defence["games"]),
            }
        )
    leaders_df = pd.DataFrame(leaders).sort_values(["country", "league", "season"])

    attack_wins = (
        leaders_df.groupby(["api_league_id", "league_label", "attack_leader"])
        .agg(attack_leading_seasons=("season", "count"), attack_first=("season", "min"), attack_last=("season", "max"))
        .reset_index()
        .rename(columns={"attack_leader": "team_name"})
    )
    defence_wins = (
        leaders_df.groupby(["api_league_id", "league_label", "defence_leader"])
        .agg(defence_leading_seasons=("season", "count"), defence_first=("season", "min"), defence_last=("season", "max"))
        .reset_index()
        .rename(columns={"defence_leader": "team_name"})
    )
    persistent = attack_wins.merge(
        defence_wins, on=["api_league_id", "league_label", "team_name"], how="outer"
    ).fillna({"attack_leading_seasons": 0, "defence_leading_seasons": 0})
    persistent["attack_leading_seasons"] = persistent["attack_leading_seasons"].astype(int)
    persistent["defence_leading_seasons"] = persistent["defence_leading_seasons"].astype(int)
    persistent["total_leading_seasons"] = (
        persistent["attack_leading_seasons"] + persistent["defence_leading_seasons"]
    )
    persistent = persistent[persistent["total_leading_seasons"] >= 2].sort_values(
        ["total_leading_seasons", "attack_leading_seasons", "defence_leading_seasons"],
        ascending=False,
    )
    return agg, leaders_df, persistent


def current_predictive_strength(matches: pd.DataFrame, elo: pd.DataFrame,
                                conn) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    blended = matches.copy()
    blended["home_goals"] = blended["home_goals"].astype(float)
    blended["away_goals"] = blended["away_goals"].astype(float)
    home_xg_mask = blended["home_xg"].notna()
    away_xg_mask = blended["away_xg"].notna()
    blended.loc[home_xg_mask, "home_goals"] = (
        0.5 * blended.loc[home_xg_mask, "home_goals"]
        + 0.5 * blended.loc[home_xg_mask, "home_xg"]
    )
    blended.loc[away_xg_mask, "away_goals"] = (
        0.5 * blended.loc[away_xg_mask, "away_goals"]
        + 0.5 * blended.loc[away_xg_mask, "away_xg"]
    )
    reference_time = matches["kickoff_utc"].max() + pd.Timedelta(days=1)
    latest_seasons = matches.groupby("api_league_id")["season"].max().to_dict()
    results: list[dict] = []

    for league_id, league_matches in blended.groupby("api_league_id"):
        params = fit_league_dc(league_matches, reference_time)
        if params is None:
            continue
        latest_season = int(latest_seasons[league_id])
        current_matches = league_matches[league_matches["season"] == latest_season]
        if (reference_time - current_matches["kickoff_utc"].max()).days > ACTIVE_LEAGUE_MAX_STALENESS_DAYS:
            continue
        current_team_ids = sorted(
            set(current_matches["home_team_id"]) | set(current_matches["away_team_id"])
        )
        attack_values = {team_id: params["attack"][team_id] for team_id in current_team_ids}
        defence_values = {team_id: params["defence"][team_id] for team_id in current_team_ids}
        attack_center = float(np.mean(list(attack_values.values())))
        defence_center = float(np.mean(list(defence_values.values())))

        team_names = pd.concat(
            [
                current_matches[["kickoff_utc", "home_team_id", "home_team"]].rename(
                    columns={"home_team_id": "team_id", "home_team": "team_name"}
                ),
                current_matches[["kickoff_utc", "away_team_id", "away_team"]].rename(
                    columns={"away_team_id": "team_id", "away_team": "team_name"}
                ),
            ],
            ignore_index=True,
        ).sort_values("kickoff_utc").drop_duplicates("team_id", keep="last").set_index("team_id")["team_name"]

        actual_current_matches = matches[
            (matches["api_league_id"] == league_id)
            & (matches["season"] == latest_season)
        ]
        actual_team_rows = make_team_match_rows(actual_current_matches)
        actual_team_agg = actual_team_rows.groupby("team_id").agg(
            current_games=("fixture_id", "nunique"),
            current_goals_for_per_game=("goals_for", "mean"),
            current_goals_against_per_game=("goals_against", "mean"),
        )
        xg_covered = int(current_matches["home_xg"].notna().sum())
        league_xg_coverage = xg_covered / len(current_matches) if len(current_matches) else 0.0

        days_ago = np.clip(
            (reference_time - league_matches["kickoff_utc"]).dt.days.to_numpy(), 0, None
        )
        league_weights = np.exp(-XI * days_ago)
        effective = {team_id: 0.0 for team_id in current_team_ids}
        for home_id, away_id, weight in zip(
            league_matches["home_team_id"], league_matches["away_team_id"], league_weights
        ):
            if home_id in effective:
                effective[home_id] += float(weight)
            if away_id in effective:
                effective[away_id] += float(weight)

        attack_log_scores = {}
        defence_log_scores = {}
        for team_id in current_team_ids:
            reliability = effective[team_id] / (effective[team_id] + STRENGTH_PRIOR_MATCHES)
            attack_log_scores[team_id] = (
                attack_values[team_id] - attack_center
            ) * reliability
            defence_log_scores[team_id] = -(
                defence_values[team_id] - defence_center
            ) * reliability
        attack_log_center = float(np.mean(list(attack_log_scores.values())))
        defence_log_center = float(np.mean(list(defence_log_scores.values())))

        first = current_matches.iloc[0]
        for team_id in current_team_ids:
            attack_index = 100 * math.exp(attack_log_scores[team_id] - attack_log_center)
            defence_index = 100 * math.exp(defence_log_scores[team_id] - defence_log_center)
            games = int(actual_team_agg.loc[team_id, "current_games"])
            effective_matches = effective[team_id]
            confidence = "high" if effective_matches >= 12 else "medium" if effective_matches >= 6 else "low"
            results.append(
                {
                    "country": first["country_name"],
                    "league": first["league_name"],
                    "league_label": first["league"],
                    "api_league_id": int(league_id),
                    "tier": None if pd.isna(first["tier"]) else int(first["tier"]),
                    "season": latest_season,
                    "team_id": int(team_id),
                    "team_name": team_names.loc[team_id],
                    "current_games": games,
                    "effective_matches": round(effective_matches, 2),
                    "confidence": confidence,
                    "current_goals_for_per_game": round(float(actual_team_agg.loc[team_id, "current_goals_for_per_game"]), 3),
                    "current_goals_against_per_game": round(float(actual_team_agg.loc[team_id, "current_goals_against_per_game"]), 3),
                    "attack_index": round(attack_index, 1),
                    "defence_index": round(defence_index, 1),
                    "xg_coverage": round(league_xg_coverage, 3),
                    "model_matches": int(params["n_matches"]),
                    "as_of": current_matches["kickoff_utc"].max().date().isoformat(),
                }
            )

    current = pd.DataFrame(results).merge(elo, on="team_id", how="left")
    current["attack_percentile"] = current.groupby("api_league_id")["attack_index"].rank(
        pct=True, method="average"
    ) * 100
    current["defence_percentile"] = current.groupby("api_league_id")["defence_index"].rank(
        pct=True, method="average"
    ) * 100
    current["elo_percentile"] = current.groupby("api_league_id")["elo_rating"].rank(
        pct=True, method="average"
    ) * 100
    current["attack_rank"] = current.groupby("api_league_id")["attack_index"].rank(
        ascending=False, method="min"
    ).astype(int)
    current["defence_rank"] = current.groupby("api_league_id")["defence_index"].rank(
        ascending=False, method="min"
    ).astype(int)
    current["elo_rank"] = current.groupby("api_league_id")["elo_rating"].rank(
        ascending=False, method="min"
    ).astype("Int64")
    current["attack_vs_elo_gap"] = current["attack_percentile"] - current["elo_percentile"]
    current["defence_vs_elo_gap"] = current["defence_percentile"] - current["elo_percentile"]
    current["specialist_gap"] = current[["attack_vs_elo_gap", "defence_vs_elo_gap"]].max(axis=1)
    current["specialty"] = np.where(
        current["attack_vs_elo_gap"] >= current["defence_vs_elo_gap"], "attack", "defence"
    )
    numeric_round = [
        "attack_percentile", "defence_percentile", "elo_percentile",
        "attack_vs_elo_gap", "defence_vs_elo_gap", "specialist_gap", "elo_rating",
    ]
    current[numeric_round] = current[numeric_round].round(1)
    current = current.sort_values(["country", "league", "attack_rank", "team_name"])

    leaders = []
    for league_id, group in current.groupby("api_league_id"):
        attack = group.sort_values(["attack_rank", "team_name"]).iloc[0]
        defence = group.sort_values(["defence_rank", "team_name"]).iloc[0]
        leaders.append(
            {
                "country": attack["country"],
                "league": attack["league"],
                "league_label": attack["league_label"],
                "api_league_id": int(league_id),
                "season": int(attack["season"]),
                "attack_leader": attack["team_name"],
                "attack_index": float(attack["attack_index"]),
                "attack_elo_rank": int(attack["elo_rank"]) if pd.notna(attack["elo_rank"]) else None,
                "defence_leader": defence["team_name"],
                "defence_index": float(defence["defence_index"]),
                "defence_elo_rank": int(defence["elo_rank"]) if pd.notna(defence["elo_rank"]) else None,
                "teams": int(len(group)),
                "as_of": max(group["as_of"]),
            }
        )
    leaders_df = pd.DataFrame(leaders).sort_values(["country", "league"])

    watchlist = current[
        (current["confidence"] != "low")
        & (current[["attack_percentile", "defence_percentile"]].max(axis=1) >= 75)
        & (current["specialist_gap"] >= 15)
    ].sort_values(["specialist_gap", "effective_matches"], ascending=[False, False])
    return current, leaders_df, watchlist


def league_coverage(matches: pd.DataFrame) -> pd.DataFrame:
    coverage = (
        matches.groupby(["api_league_id", "country_name", "league_name", "league", "tier"], dropna=False)
        .agg(
            seasons=("season", "nunique"),
            first_season=("season", "min"),
            latest_season=("season", "max"),
            fixtures=("fixture_id", "nunique"),
            teams=("home_team_id", "nunique"),
            first_match=("kickoff_utc", "min"),
            last_match=("kickoff_utc", "max"),
        )
        .reset_index()
    )
    coverage["first_match"] = coverage["first_match"].dt.date.astype(str)
    coverage["last_match"] = coverage["last_match"].dt.date.astype(str)
    return coverage.sort_values(["country_name", "league_name"])


def calibration_rows() -> tuple[pd.DataFrame, dict]:
    if not CALIBRATION_REPORT.exists():
        return pd.DataFrame(), {}
    report = json.loads(CALIBRATION_REPORT.read_text())
    rows = []
    for model, values in report.get("per_model", {}).items():
        rows.append(
            {
                "model": model,
                "predictions": values.get("n"),
                "brier_score": values.get("brier"),
                "average_predicted": values.get("avg_predicted"),
                "actual_hit_rate": values.get("actual_hit_rate"),
                "calibration_gap": values.get("overconfidence"),
            }
        )
    meta = {
        "method": report.get("method"),
        "n": report.get("n"),
        "unique_fixtures": report.get("unique_fixtures"),
        "n_folds": report.get("n_folds"),
        "n_fit_folds": report.get("n_fit_folds"),
        "overall_brier": report.get("brier_score"),
    }
    return pd.DataFrame(rows), meta


def build_snapshot(tables: dict[str, pd.DataFrame], overview: dict) -> dict:
    component_map = {
        "overview": ["report-summary", "coverage-kpis"],
        "league_leaders": ["league-leaders-table"],
        "current_ratings": ["current-strength-scatter", "current-ratings-table"],
        "specialist_watchlist": ["specialist-watchlist"],
        "historical_leaders": ["historical-leaders-table", "historical-persistence-chart"],
        "persistent_leaders": ["historical-persistence-chart", "persistent-leaders-table"],
        "league_coverage": ["coverage-table"],
        "model_validation": ["model-validation"],
        "data_quality": ["data-quality-note", "data-quality-table"],
    }
    labels = {
        "overview": "Ballistics analysis summary",
        "league_leaders": "Current opponent-adjusted league leaders",
        "current_ratings": "Current team attack, defence, and Elo comparison",
        "specialist_watchlist": "Non-Elo attack/defence specialist watchlist",
        "historical_leaders": "Historical league-season leaders",
        "persistent_leaders": "Repeat historical leaders",
        "league_coverage": "Historical fixture coverage by league",
        "model_validation": "Existing leak-free model calibration evidence",
        "data_quality": "Source data-quality checks",
    }

    queries = {}
    for query_id, table in tables.items():
        definitions = []
        if query_id in {"current_ratings", "league_leaders", "specialist_watchlist"}:
            definitions.extend(
                [
                    {
                        "label": "Attack index",
                        "definition": "Time-decayed Dixon-Coles attack strength, adjusted for opponent and home advantage, shrunk toward neutral with a six-effective-match prior, and re-centred so the current league geometric mean is 100; higher is stronger.",
                        "formula": "100 * exp(center((team_attack - league_mean_attack) * effective_matches / (effective_matches + 6)))",
                        "componentIds": component_map[query_id],
                        "sourceLineage": [{"tables": ["raw.raw_fixtures", "raw.fixture_xg"]}],
                    },
                    {
                        "label": "Defence index",
                        "definition": "Inverse Dixon-Coles defensive concession effect, adjusted for opponent and home advantage, shrunk toward neutral with a six-effective-match prior, and re-centred so the current league geometric mean is 100; higher is stronger.",
                        "formula": "100 * exp(center(-(team_defence - league_mean_defence) * effective_matches / (effective_matches + 6)))",
                        "componentIds": component_map[query_id],
                        "sourceLineage": [{"tables": ["raw.raw_fixtures", "raw.fixture_xg"]}],
                    },
                    {
                        "label": "Specialist gap",
                        "definition": "The larger of attack percentile minus Elo percentile and defence percentile minus Elo percentile. It identifies a specialised unit that Elo may obscure; it is not proof of bookmaker value.",
                        "formula": "max(attack_percentile - elo_percentile, defence_percentile - elo_percentile)",
                        "componentIds": component_map[query_id],
                        "sourceLineage": [{"tables": ["raw.raw_fixtures", "elo.team_elo_history"]}],
                    },
                ]
            )
        if query_id in {"historical_leaders", "persistent_leaders"}:
            definitions.append(
                {
                    "label": "Historical season leader",
                    "definition": "Best empirical-Bayes shrunk goals scored rate (attack) or lowest shrunk goals conceded rate (defence) within the same league-season.",
                    "componentIds": component_map[query_id],
                    "sourceLineage": [{"tables": ["raw.raw_fixtures"]}],
                }
            )
        if query_id == "overview":
            definitions.extend(
                [
                    {
                        "label": "Completed fixtures",
                        "definition": "Distinct raw fixtures with final-time status and complete home and away goals.",
                        "componentIds": [
                            "coverage-kpis:fixtures", "coverage-kpis:leagues",
                            "coverage-kpis:teams", "coverage-kpis:specialists", "report-summary",
                        ],
                        "sourceLineage": [{"tables": ["raw.raw_fixtures"]}],
                    },
                    {
                        "label": "Transformed fact coverage",
                        "definition": "Distinct played fixtures in raw.fact_head_to_head divided by completed fixtures in raw.raw_fixtures.",
                        "formula": "fact_fixtures / raw_ft_fixtures",
                        "componentIds": ["coverage-kpis:fixtures", "data-quality-note"],
                        "sourceLineage": [{"tables": ["raw.fact_head_to_head", "raw.raw_fixtures"]}],
                    },
                ]
            )
        if query_id == "model_validation":
            definitions.append(
                {
                    "label": "Brier score",
                    "definition": "Mean squared error of predicted probabilities against binary outcomes in the existing leak-free walk-forward evaluation; lower is better.",
                    "componentIds": ["model-validation"],
                    "sourceLineage": [{"files": ["engine/eval/calibration_report.json"]}],
                }
            )
        source = {
            "label": labels[query_id],
            "filters": [
                f"Completed fixtures only; data through {overview['data_as_of']}",
                "Strengths are compared within the same league, never by raw goals across leagues.",
            ],
            "metricDefinitions": definitions,
        }
        if query_id == "model_validation":
            source["files"] = ["engine/eval/calibration_report.json"]
        elif query_id in {"current_ratings", "league_leaders", "specialist_watchlist"}:
            source["tables"] = [
                "raw.raw_fixtures", "raw.fixture_xg", "elo.team_elo_history",
                "dim.dim_leagues", "dim.dim_league_seasons", "dim.dim_teams",
            ]
        elif query_id in {"overview", "data_quality"}:
            source["tables"] = [
                "raw.raw_fixtures", "raw.fact_head_to_head",
                "dim.dim_leagues", "dim.dim_league_seasons", "dim.dim_teams",
            ]
        else:
            source["tables"] = [
                "raw.raw_fixtures", "dim.dim_leagues",
                "dim.dim_league_seasons", "dim.dim_teams",
            ]
        queries[query_id] = {
            "rows": _json_ready(table.to_dict(orient="records")),
            "source": source,
        }
    return {
        "id": REPORT_ID,
        "surface": "report",
        "title": "Attack and defence reveal specialists that Elo compresses",
        "generatedAt": pd.Timestamp.now(tz="UTC").isoformat(),
        "buildStatus": "complete",
        "status": "reviewed",
        "report": {"asOf": overview["data_as_of"]},
        "filters": [],
        "queries": queries,
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        matches, elo = load_inputs(conn)
        quality, quality_meta = quality_summary(conn, matches)
        team_matches = make_team_match_rows(matches)
        historical, historical_leaders, persistent = historical_strength(team_matches)
        current, league_leaders, watchlist = current_predictive_strength(matches, elo, conn)
        coverage = league_coverage(matches)
        calibration, calibration_meta = calibration_rows()
    finally:
        conn.close()

    overview = {
        "data_as_of": matches["kickoff_utc"].max().date().isoformat(),
        "first_match": matches["kickoff_utc"].min().date().isoformat(),
        "finished_fixtures": int(matches["fixture_id"].nunique()),
        "leagues": int(matches["api_league_id"].nunique()),
        "league_seasons": int(matches["league_season_id"].nunique()),
        "teams": int(pd.unique(pd.concat([matches["home_team_id"], matches["away_team_id"]])).size),
        "current_rated_teams": int(len(current)),
        "current_rated_leagues": int(current["api_league_id"].nunique()),
        "xg_fixtures": int(matches["home_xg"].notna().sum()),
        "watchlist_teams": int(len(watchlist)),
        **quality_meta,
        "calibration": calibration_meta,
    }
    tables = {
        "overview": pd.DataFrame([overview]),
        "league_leaders": league_leaders,
        "current_ratings": current,
        "specialist_watchlist": watchlist.head(60),
        "historical_leaders": historical_leaders,
        "persistent_leaders": persistent,
        "league_coverage": coverage,
        "model_validation": calibration,
        "data_quality": quality,
    }

    for name, table in tables.items():
        table.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
    historical.to_csv(OUTPUT_DIR / "historical_team_strength.csv", index=False)
    (OUTPUT_DIR / "overview.json").write_text(json.dumps(overview, indent=2, default=str))
    snapshot = build_snapshot(tables, overview)
    (OUTPUT_DIR / "reviewed_snapshot.json").write_text(json.dumps(snapshot, indent=2, allow_nan=False))
    if REPORT_DATA_FILE.parent.exists():
        REPORT_DATA_FILE.write_text(json.dumps(snapshot, indent=2, allow_nan=False))

    print(json.dumps(overview, indent=2, default=str))
    print("\nTop specialist gaps (medium/high confidence):")
    display = watchlist[
        [
            "league_label", "team_name", "specialty", "attack_percentile",
            "defence_percentile", "elo_percentile", "specialist_gap", "confidence",
        ]
    ].head(20)
    print(display.to_string(index=False))


if __name__ == "__main__":
    main()
