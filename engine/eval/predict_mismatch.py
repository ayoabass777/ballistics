"""
Mismatch predictor — team-level market picks driven by Elo gaps + H2H.

Separate from the Poisson GLM over/under model. This predicts:
  - Favourite to win (1X2 result)
  - Draw
  - Favourite to score (Over 0.5 team goals)
  - Over 1.5 match goals
  - Over 2.5 match goals

Confidence for goals markets is derived from league-specific empirical hit
rates at each Elo gap bracket. Win/draw confidence combines league-level
gap rates with a per-matchup H2H adjustment.

Usage:
    python -m engine.eval.predict_mismatch
    python -m engine.eval.predict_mismatch --days 14 --min-gap 150
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import yaml

from engine.eval.calibration_maps import apply_points, load_points
from engine.eval.team_strength import load_team_df, compute_team_strength
from etl.src.logger import get_logger

logger = get_logger(__name__)

_MISMATCH_POINTS = load_points("mismatch")


def _calibrate_conf(raw: float) -> float:
    """Map a raw empirical confidence through the fitted calibration map.

    Identity when calibration_maps.json is absent. Not applied to the
    draw market — the map is fitted on >=0.60 confidences only.
    """
    if _MISMATCH_POINTS:
        return apply_points(_MISMATCH_POINTS, raw)
    return raw

SOURCE_TABLE = "raw.fact_head_to_head"
ELO_TABLE = "elo.team_elo_history"

GAP_BINS = [
    (100, 200, "100-200"),
    (200, 300, "200-300"),
    (300, 9999, "300+"),
]

MIN_SAMPLE = 20
MIN_CONFIDENCE = 0.70


H2H_MIN_MATCHES = 3
H2H_DOMINANCE_THRESHOLD = 0.6


@dataclass
class MismatchPick:
    fixture_id: int
    kickoff: datetime
    home_team: str
    away_team: str
    league: str
    favourite: str
    underdog: str
    fav_elo: float
    dog_elo: float
    gap: float
    gap_bin: str
    market: str
    confidence: float
    sample_size: int
    fav_is_home: bool
    h2h_wins: int = 0
    h2h_draws: int = 0
    h2h_losses: int = 0
    h2h_boost: str = ""
    strength_adj: float = 0.0
    fav_attack_pct: float = 0.0
    dog_defense_pct: float = 0.0


def _get_conn():
    return psycopg2.connect(
        dbname="ballistics",
        user="ballistics_user",
        password="ronaldo",
        host="localhost",
        port=5432,
    )


def _build_team_id_map(conn) -> dict:
    df = pd.read_sql(f"""
        SELECT DISTINCT r.home_team_id AS rf_id, f.team_id AS fh_id
        FROM raw.raw_fixtures r
        JOIN {SOURCE_TABLE} f ON f.fixture_id = r.fixture_id AND f.is_home
        UNION
        SELECT DISTINCT r.away_team_id, f.opponent_id
        FROM raw.raw_fixtures r
        JOIN {SOURCE_TABLE} f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    return dict(zip(df["rf_id"], df["fh_id"]))


def _get_latest_elo(conn, team_ids: list, id_map: dict) -> dict:
    if not team_ids:
        return {}
    df = pd.read_sql(f"""
        SELECT DISTINCT ON (team_id)
            team_id, COALESCE(rating_after_flat, rating_after) AS rating
        FROM {ELO_TABLE}
        ORDER BY team_id, kickoff_utc DESC
    """, conn)
    elo_by_fh = dict(zip(df["team_id"], df["rating"]))
    result = {}
    for rf_id in team_ids:
        fh_id = id_map.get(rf_id, rf_id)
        if fh_id in elo_by_fh:
            result[rf_id] = elo_by_fh[fh_id]
    return result


def _load_league_map(conn) -> dict:
    df = pd.read_sql("""
        SELECT DISTINCT r.api_league_id, f.league_name
        FROM raw.raw_fixtures r
        JOIN raw.fact_head_to_head f ON f.fixture_id = r.fixture_id AND f.is_home
    """, conn)
    return dict(zip(df["api_league_id"], df["league_name"]))


def build_hit_rates(conn) -> pd.DataFrame:
    """Compute empirical hit rates per league and gap bin."""
    sql = f"""
        WITH matches AS (
            SELECT
                f.fixture_id, f.league_name,
                f.goals_for + f.goals_against AS total,
                ABS(
                    COALESCE(eh.rating_before_flat, eh.rating_before)
                  - COALESCE(ea.rating_before_flat, ea.rating_before)
                ) AS gap,
                CASE
                    WHEN COALESCE(eh.rating_before_flat, eh.rating_before)
                       > COALESCE(ea.rating_before_flat, ea.rating_before)
                    THEN f.goals_for ELSE f.goals_against
                END AS fav_goals,
                CASE
                    WHEN COALESCE(eh.rating_before_flat, eh.rating_before)
                       > COALESCE(ea.rating_before_flat, ea.rating_before)
                    THEN f.goals_against ELSE f.goals_for
                END AS dog_goals
            FROM {SOURCE_TABLE} f
            JOIN {ELO_TABLE} eh
                ON eh.team_id = f.team_id AND eh.fixture_id = f.fixture_id
            JOIN {ELO_TABLE} ea
                ON ea.team_id = f.opponent_id AND ea.fixture_id = f.fixture_id
            WHERE f.is_played AND f.is_home
        )
        SELECT
            league_name,
            gap,
            fav_goals,
            dog_goals,
            total
        FROM matches
        WHERE gap >= 100
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        return pd.DataFrame()

    rows = []
    for league, grp in df.groupby("league_name"):
        for lo, hi, label in GAP_BINS:
            bin_df = grp[(grp["gap"] >= lo) & (grp["gap"] < hi)]
            n = len(bin_df)
            if n < MIN_SAMPLE:
                continue
            rows.append({
                "league": league,
                "gap_bin": label,
                "gap_lo": lo,
                "gap_hi": hi,
                "n": n,
                "fav_scores_pct": (bin_df["fav_goals"] >= 1).mean(),
                "fav_over15_pct": (bin_df["fav_goals"] >= 2).mean(),
                "over15_pct": (bin_df["total"] > 1.5).mean(),
                "over25_pct": (bin_df["total"] > 2.5).mean(),
                "fav_win_pct": (bin_df["fav_goals"] > bin_df["dog_goals"]).mean(),
                "draw_pct": (bin_df["fav_goals"] == bin_df["dog_goals"]).mean(),
                "avg_fav_goals": bin_df["fav_goals"].mean(),
                "avg_total": bin_df["total"].mean(),
            })

    for lo, hi, label in GAP_BINS:
        bin_df = df[(df["gap"] >= lo) & (df["gap"] < hi)]
        n = len(bin_df)
        if n < MIN_SAMPLE:
            continue
        rows.append({
            "league": "__global__",
            "gap_bin": label,
            "gap_lo": lo,
            "gap_hi": hi,
            "n": n,
            "fav_scores_pct": (bin_df["fav_goals"] >= 1).mean(),
            "fav_over15_pct": (bin_df["fav_goals"] >= 2).mean(),
            "over15_pct": (bin_df["total"] > 1.5).mean(),
            "over25_pct": (bin_df["total"] > 2.5).mean(),
            "fav_win_pct": (bin_df["fav_goals"] > bin_df["dog_goals"]).mean(),
            "draw_pct": (bin_df["fav_goals"] == bin_df["dog_goals"]).mean(),
            "avg_fav_goals": bin_df["fav_goals"].mean(),
            "avg_total": bin_df["total"].mean(),
        })

    return pd.DataFrame(rows)


def _get_gap_bin(gap: float) -> tuple[str, int, int] | None:
    for lo, hi, label in GAP_BINS:
        if lo <= gap < hi:
            return label, lo, hi
    return None


def _lookup_hit_rate(
    hit_rates: pd.DataFrame, league: str, gap_bin: str, market: str
) -> tuple[float, int]:
    col_map = {
        "fav_to_score": "fav_scores_pct",
        "fav_over_1.5": "fav_over15_pct",
        "over_1.5": "over15_pct",
        "over_2.5": "over25_pct",
        "fav_win": "fav_win_pct",
        "draw": "draw_pct",
    }
    col = col_map[market]
    row = hit_rates[(hit_rates["league"] == league) & (hit_rates["gap_bin"] == gap_bin)]
    if not row.empty:
        return float(row.iloc[0][col]), int(row.iloc[0]["n"])
    row = hit_rates[(hit_rates["league"] == "__global__") & (hit_rates["gap_bin"] == gap_bin)]
    if not row.empty:
        return float(row.iloc[0][col]), int(row.iloc[0]["n"])
    return 0.0, 0


def _load_h2h_records(conn, id_map: dict, matchups: list[tuple[int, int]],
                      cutoff=None) -> dict:
    """Load H2H win/draw/loss for favourite vs underdog pairs.

    matchups: list of (fav_rf_id, dog_rf_id).
    `cutoff` restricts to matches before that datetime (walk-forward safety).
    Returns {(fav_rf_id, dog_rf_id): (wins, draws, losses)}.
    """
    if not matchups:
        return {}
    fav_fh_ids = {rf: id_map.get(rf, rf) for rf, _ in matchups}
    dog_fh_ids = {rf: id_map.get(rf, rf) for _, rf in matchups}
    pairs_to_query = set()
    for fav_rf, dog_rf in matchups:
        pairs_to_query.add((fav_fh_ids[fav_rf], dog_fh_ids[dog_rf]))

    all_fh = set()
    for a, b in pairs_to_query:
        all_fh.add(a)
        all_fh.add(b)

    cutoff_filter = "AND kickoff_utc < %(cutoff)s" if cutoff is not None else ""
    df = pd.read_sql(f"""
        SELECT team_id, opponent_id, goals_for, goals_against
        FROM {SOURCE_TABLE}
        WHERE team_id IN ({','.join(str(x) for x in all_fh)})
          AND opponent_id IN ({','.join(str(x) for x in all_fh)})
          AND is_played {cutoff_filter}
    """, conn, params={"cutoff": cutoff} if cutoff is not None else None)

    pair_results = {}
    for fav_fh, dog_fh in pairs_to_query:
        games = df[(df["team_id"] == fav_fh) & (df["opponent_id"] == dog_fh)]
        w = int((games["goals_for"] > games["goals_against"]).sum())
        d = int((games["goals_for"] == games["goals_against"]).sum())
        l = int((games["goals_for"] < games["goals_against"]).sum())
        pair_results[(fav_fh, dog_fh)] = (w, d, l)

    result = {}
    for fav_rf, dog_rf in matchups:
        key = (fav_fh_ids[fav_rf], dog_fh_ids[dog_rf])
        result[(fav_rf, dog_rf)] = pair_results.get(key, (0, 0, 0))
    return result


def _load_team_strength(conn, id_map: dict, cutoff=None) -> dict:
    """Load attack/defense shrunk rates keyed by rf_id.

    `cutoff` restricts to matches before that datetime (walk-forward safety);
    an empty window yields no strength data and therefore no adjustments.
    Returns {rf_id: {"attack": float, "defense": float, "league": str}}.
    """
    df = load_team_df(conn, cutoff=cutoff)
    if df.empty:
        return {}
    strength = compute_team_strength(df)
    # strength is keyed by fh team_id — invert id_map for lookup
    fh_to_rf = {}
    for rf_id, fh_id in id_map.items():
        fh_to_rf.setdefault(fh_id, rf_id)

    result = {}
    for _, row in strength.iterrows():
        fh_id = row["team_id"]
        rf_id = fh_to_rf.get(fh_id, fh_id)
        entry = {
            "attack": row["attack_shrunk"],
            "defense": row["defense_shrunk"],
            "league": row["league"],
            "attack_pct": row["attack_percentile"],
            "defense_pct": row["defense_percentile"],
            "attack_rank": int(row["attack_rank"]),
            "defense_rank": int(row["defense_rank"]),
            "league_size": int(row["league_size"]),
        }
        for prefix in ("home", "away"):
            for stat in ("attack_shrunk", "defense_shrunk",
                         "attack_percentile", "defense_percentile",
                         "attack_rank", "defense_rank", "league_size"):
                col = f"{prefix}_{stat}"
                if col in row.index and pd.notna(row.get(col)):
                    if "shrunk" in stat:
                        entry[col] = round(float(row[col]), 3)
                    elif "rank" in stat or "size" in stat:
                        entry[col] = int(row[col])
                    else:
                        entry[col] = round(float(row[col]), 1)
                else:
                    entry[col] = None
        result[rf_id] = entry
    return result


def _strength_adjustment(
    market: str,
    fav_strength: dict | None,
    dog_strength: dict | None,
) -> float:
    """Compute a confidence adjustment based on team attack/defense quality.

    Fav to score / Fav Over 1.5:
        Boost if fav has strong attack (pct > 60) or dog has weak defense (pct < 40).
        Penalize if fav has weak attack (pct < 40) or dog has strong defense (pct > 60).

    Over 1.5 (match total):
        Boost if both attacks are strong or both defenses are weak.
        Penalize if both attacks are weak or both defenses are strong.

    Returns adjustment in [-0.08, +0.08] range.
    """
    if fav_strength is None or dog_strength is None:
        return 0.0

    fav_atk = fav_strength["attack_pct"]
    fav_def = fav_strength["defense_pct"]
    dog_atk = dog_strength["attack_pct"]
    dog_def = dog_strength["defense_pct"]

    if market in ("fav_to_score", "fav_over_1.5"):
        # fav attack vs dog defense
        atk_signal = (fav_atk - 50) / 50  # -1 to +1
        def_signal = (50 - dog_def) / 50  # positive = weak defense = good for scorer
        raw = (atk_signal + def_signal) / 2
        return round(max(min(raw * 0.08, 0.08), -0.08), 4)

    if market == "over_1.5":
        # both teams' attacking power vs both teams' defensive weakness
        atk_signal = ((fav_atk + dog_atk) / 2 - 50) / 50
        def_signal = (50 - (fav_def + dog_def) / 2) / 50
        raw = (atk_signal + def_signal) / 2
        return round(max(min(raw * 0.08, 0.08), -0.08), 4)

    return 0.0


def _h2h_adjustment(wins: int, draws: int, losses: int) -> tuple[float, str]:
    """Return (win_boost, label) based on H2H record.

    H2H dominant (>=60% wins in 3+ games): +0.05 to fav_win confidence.
    H2H mixed: no adjustment.
    H2H underdog-dominant: -0.05 penalty.
    """
    total = wins + draws + losses
    if total < H2H_MIN_MATCHES:
        return 0.0, ""
    win_rate = wins / total
    if win_rate >= H2H_DOMINANCE_THRESHOLD:
        return 0.05, "H2H+"
    if win_rate <= 0.3:
        return -0.05, "H2H-"
    return 0.0, "H2H~"


def predict_mismatch(
    days: int = 7, min_gap: int = 100,
) -> tuple[list[MismatchPick], dict]:
    conn = _get_conn()
    try:
        print("Building historical hit rates...")
        hit_rates = build_hit_rates(conn)
        if hit_rates.empty:
            print("  No historical mismatch data found.")
            return [], {}

        leagues_with_data = set(hit_rates[hit_rates["league"] != "__global__"]["league"])
        print(f"  {len(leagues_with_data)} leagues with league-specific rates")

        print("Loading upcoming fixtures...")
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        end = today + timedelta(days=days)
        upcoming = pd.read_sql("""
            SELECT fixture_id, kickoff_utc, home_team_id, home_team_name,
                   away_team_id, away_team_name, api_league_id
            FROM raw.raw_fixtures
            WHERE fixture_status = 'NS'
              AND kickoff_utc BETWEEN %s AND %s
            ORDER BY kickoff_utc
        """, conn, params=[today, end])

        if upcoming.empty:
            print("  No upcoming fixtures.")
            return [], {}

        league_map = _load_league_map(conn)
        upcoming["league"] = upcoming["api_league_id"].map(league_map)
        upcoming = upcoming[upcoming["league"].notna()].copy()

        id_map = _build_team_id_map(conn)
        all_teams = set(upcoming["home_team_id"].tolist() + upcoming["away_team_id"].tolist())
        elo_map = _get_latest_elo(conn, list(all_teams), id_map)

        # Pre-compute which fixtures have a gap above threshold
        fixture_info = []
        for _, row in upcoming.iterrows():
            h_elo = elo_map.get(row["home_team_id"], 1500.0)
            a_elo = elo_map.get(row["away_team_id"], 1500.0)
            gap = abs(h_elo - a_elo)
            if gap < min_gap:
                continue
            bin_info = _get_gap_bin(gap)
            if bin_info is None:
                continue
            fav_is_home = h_elo >= a_elo
            fav_rf = row["home_team_id"] if fav_is_home else row["away_team_id"]
            dog_rf = row["away_team_id"] if fav_is_home else row["home_team_id"]
            fixture_info.append((row, h_elo, a_elo, gap, bin_info, fav_is_home, fav_rf, dog_rf))

        # Batch-load H2H for all matchups
        matchups = [(fav_rf, dog_rf) for (_, _, _, _, _, _, fav_rf, dog_rf) in fixture_info]
        print(f"  Loading H2H for {len(matchups)} matchups...")
        h2h_map = _load_h2h_records(conn, id_map, matchups)

        # Load team strength (attack/defense)
        print("  Loading team strength...")
        strength_map = _load_team_strength(conn, id_map)
        teams_with_strength = sum(1 for fi in fixture_info if fi[6] in strength_map)
        print(f"  {teams_with_strength}/{len(fixture_info)} favourites have strength data")

        picks = []
        for row, h_elo, a_elo, gap, bin_info, fav_is_home, fav_rf, dog_rf in fixture_info:
            gap_bin, _, _ = bin_info
            fav_name = row["home_team_name"] if fav_is_home else row["away_team_name"]
            dog_name = row["away_team_name"] if fav_is_home else row["home_team_name"]
            fav_elo = max(h_elo, a_elo)
            dog_elo = min(h_elo, a_elo)

            h2h_w, h2h_d, h2h_l = h2h_map.get((fav_rf, dog_rf), (0, 0, 0))
            h2h_boost, h2h_label = _h2h_adjustment(h2h_w, h2h_d, h2h_l)

            fav_strength = strength_map.get(fav_rf)
            dog_strength = strength_map.get(dog_rf)

            # Goals markets (strength-adjusted)
            for market in ["fav_to_score", "fav_over_1.5", "over_1.5", "over_2.5"]:
                conf, n = _lookup_hit_rate(hit_rates, row["league"], gap_bin, market)
                str_adj = _strength_adjustment(market, fav_strength, dog_strength)
                adjusted = max(min(conf + str_adj, 0.99), 0.0)
                # Gate on RAW confidence (mirrors the walk-forward backtest),
                # then report the calibrated probability.
                if adjusted >= MIN_CONFIDENCE and n >= MIN_SAMPLE:
                    picks.append(MismatchPick(
                        fixture_id=int(row["fixture_id"]),
                        kickoff=row["kickoff_utc"],
                        home_team=row["home_team_name"],
                        away_team=row["away_team_name"],
                        league=row["league"],
                        favourite=fav_name,
                        underdog=dog_name,
                        fav_elo=round(fav_elo),
                        dog_elo=round(dog_elo),
                        gap=round(gap),
                        gap_bin=gap_bin,
                        market=market,
                        confidence=round(_calibrate_conf(adjusted), 3),
                        sample_size=n,
                        fav_is_home=fav_is_home,
                        h2h_wins=h2h_w,
                        h2h_draws=h2h_d,
                        h2h_losses=h2h_l,
                        strength_adj=round(str_adj, 4),
                        fav_attack_pct=fav_strength["attack_pct"] if fav_strength else 0,
                        dog_defense_pct=dog_strength["defense_pct"] if dog_strength else 0,
                    ))

            # Win market (with H2H adjustment)
            win_conf, win_n = _lookup_hit_rate(hit_rates, row["league"], gap_bin, "fav_win")
            adjusted_win = min(win_conf + h2h_boost, 0.95)
            if adjusted_win >= MIN_CONFIDENCE and win_n >= MIN_SAMPLE:
                picks.append(MismatchPick(
                    fixture_id=int(row["fixture_id"]),
                    kickoff=row["kickoff_utc"],
                    home_team=row["home_team_name"],
                    away_team=row["away_team_name"],
                    league=row["league"],
                    favourite=fav_name,
                    underdog=dog_name,
                    fav_elo=round(fav_elo),
                    dog_elo=round(dog_elo),
                    gap=round(gap),
                    gap_bin=gap_bin,
                    market="fav_win",
                    confidence=round(_calibrate_conf(adjusted_win), 3),
                    sample_size=win_n,
                    fav_is_home=fav_is_home,
                    h2h_wins=h2h_w,
                    h2h_draws=h2h_d,
                    h2h_losses=h2h_l,
                    h2h_boost=h2h_label,
                ))

            # Draw market — only for gap bins where draw rate is notable (100-200)
            draw_conf, draw_n = _lookup_hit_rate(hit_rates, row["league"], gap_bin, "draw")
            draw_adj = draw_conf - h2h_boost
            if draw_adj >= 0.25 and draw_n >= MIN_SAMPLE and gap < 200:
                picks.append(MismatchPick(
                    fixture_id=int(row["fixture_id"]),
                    kickoff=row["kickoff_utc"],
                    home_team=row["home_team_name"],
                    away_team=row["away_team_name"],
                    league=row["league"],
                    favourite=fav_name,
                    underdog=dog_name,
                    fav_elo=round(fav_elo),
                    dog_elo=round(dog_elo),
                    gap=round(gap),
                    gap_bin=gap_bin,
                    market="draw",
                    confidence=round(draw_adj, 3),
                    sample_size=draw_n,
                    fav_is_home=fav_is_home,
                    h2h_wins=h2h_w,
                    h2h_draws=h2h_d,
                    h2h_losses=h2h_l,
                    h2h_boost=h2h_label,
                ))

        picks.sort(key=lambda p: (-p.confidence, p.kickoff))

        # Build fixture-level strength profiles for downstream consumers
        fixture_strength = {}
        for row, _, _, _, _, fav_is_home, fav_rf, dog_rf in fixture_info:
            fid = int(row["fixture_id"])
            home_rf = row["home_team_id"]
            away_rf = row["away_team_id"]
            home_s = strength_map.get(home_rf)
            away_s = strength_map.get(away_rf)

            def _team_profile(s, label):
                if s is None:
                    return None
                league_size = s.get("league_size", 0)
                return {
                    "overall": {
                        "attack_shrunk": s["attack"],
                        "defense_shrunk": s["defense"],
                        "attack_pct": s["attack_pct"],
                        "defense_pct": s["defense_pct"],
                        "attack_rank": s.get("attack_rank"),
                        "defense_rank": s.get("defense_rank"),
                        "league_size": league_size,
                    },
                    "home": {
                        "attack_shrunk": s["home_attack_shrunk"],
                        "defense_shrunk": s["home_defense_shrunk"],
                        "attack_pct": s["home_attack_percentile"],
                        "defense_pct": s["home_defense_percentile"],
                        "attack_rank": s.get("home_attack_rank"),
                        "defense_rank": s.get("home_defense_rank"),
                        "league_size": s.get("home_league_size", league_size),
                    } if s.get("home_attack_shrunk") is not None else None,
                    "away": {
                        "attack_shrunk": s["away_attack_shrunk"],
                        "defense_shrunk": s["away_defense_shrunk"],
                        "attack_pct": s["away_attack_percentile"],
                        "defense_pct": s["away_defense_percentile"],
                        "attack_rank": s.get("away_attack_rank"),
                        "defense_rank": s.get("away_defense_rank"),
                        "league_size": s.get("away_league_size", league_size),
                    } if s.get("away_attack_shrunk") is not None else None,
                }

            fixture_strength[fid] = {
                "home_team": _team_profile(home_s, row["home_team_name"]),
                "away_team": _team_profile(away_s, row["away_team_name"]),
            }

        return picks, fixture_strength
    finally:
        conn.close()


def _market_label(market: str, fav: str) -> str:
    if market == "fav_to_score":
        return f"{fav} to score"
    if market == "fav_over_1.5":
        return f"{fav} Over 1.5"
    if market == "fav_win":
        return f"{fav} to win"
    if market == "draw":
        return "Draw"
    return f"Over {market.split('_')[1]}"


def print_picks(picks: list[MismatchPick]) -> None:
    if not picks:
        print("No mismatch picks.")
        return

    print(f"\n{'='*120}")
    print(f"  MISMATCH PICKS  (Elo gap >= 100, empirical confidence >= {MIN_CONFIDENCE:.0%})")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*120}")

    by_bin = {}
    for p in picks:
        by_bin.setdefault(p.gap_bin, []).append(p)

    for gap_bin in ["300+", "200-300", "100-200"]:
        group = by_bin.get(gap_bin, [])
        if not group:
            continue

        print(f"\n── Gap {gap_bin}  |  {len(group)} picks ──\n")
        print(f"  {'Date':>12s}  {'Match':<40s}  {'Market':<22s}  {'Conf':>5s}  {'Str':>5s}  {'H2H':>7s}  {'Gap':>4s}  League")
        print(f"  {'─'*12}  {'─'*40}  {'─'*22}  {'─'*5}  {'─'*5}  {'─'*7}  {'─'*4}  {'─'*20}")

        seen = set()
        for p in group:
            dt = pd.Timestamp(p.kickoff).strftime("%a %b %d %H:%M") if hasattr(p.kickoff, 'strftime') else str(p.kickoff)[:16]
            match = f"{p.home_team} vs {p.away_team}"
            market_str = _market_label(p.market, p.favourite)
            key = (p.fixture_id, p.market)
            if key in seen:
                continue
            seen.add(key)
            h2h_total = p.h2h_wins + p.h2h_draws + p.h2h_losses
            h2h_str = f"{p.h2h_wins}W{p.h2h_draws}D{p.h2h_losses}L" if h2h_total > 0 else "  -  "
            boost_str = f" {p.h2h_boost}" if p.h2h_boost else ""
            str_str = f"{p.strength_adj:+.0%}" if p.strength_adj != 0 else "  -"
            print(f"  {dt:>12s}  {match:<40s}  {market_str:<22s}  {p.confidence:>4.0%}  {str_str:>5s}  {h2h_str:>7s}{boost_str}  {p.gap:>4.0f}  {p.league}")


def save_picks_yaml(picks: list[MismatchPick]) -> str:
    ds = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = Path(__file__).parent / f"mismatch_picks_{ds}.yaml"

    data = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "model": "mismatch_elo_gap",
        "min_confidence": MIN_CONFIDENCE,
        "picks": [],
    }

    for p in picks:
        entry = {
            "fixture": f"{p.home_team} vs {p.away_team}",
            "kickoff": pd.Timestamp(p.kickoff).strftime("%Y-%m-%d %H:%M UTC") if hasattr(p.kickoff, 'strftime') else str(p.kickoff),
            "league": p.league,
            "market": _market_label(p.market, p.favourite),
            "favourite": p.favourite,
            "underdog": p.underdog,
            "fav_elo": p.fav_elo,
            "dog_elo": p.dog_elo,
            "gap": p.gap,
            "gap_bin": p.gap_bin,
            "confidence": p.confidence,
            "sample_size": p.sample_size,
        }
        h2h_total = p.h2h_wins + p.h2h_draws + p.h2h_losses
        if h2h_total > 0:
            entry["h2h"] = f"{p.h2h_wins}W-{p.h2h_draws}D-{p.h2h_losses}L"
        if p.h2h_boost:
            entry["h2h_boost"] = p.h2h_boost
        if p.strength_adj != 0:
            entry["strength_adj"] = p.strength_adj
        data["picks"].append(entry)

    with open(out_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\nMismatch picks saved to {out_path}")
    return str(out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Predict mismatch team-level picks")
    parser.add_argument("--days", type=int, default=7, help="Days ahead")
    parser.add_argument("--min-gap", type=int, default=100, help="Minimum Elo gap")
    args = parser.parse_args()

    picks, _ = predict_mismatch(days=args.days, min_gap=args.min_gap)
    print_picks(picks)
    if picks:
        save_picks_yaml(picks)
