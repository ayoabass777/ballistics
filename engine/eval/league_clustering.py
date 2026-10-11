"""
League scoring-style clustering — hierarchical, two methods.

1. Feature-based: Ward linkage on z-scored league summary features.
2. JSD-based: average linkage on Jensen-Shannon divergence between
   each league's full goal-count distribution.

Both produce a dendrogram and a flat-cluster cut at the largest
merge-height gap.

Usage:
    python -m engine.eval.league_clustering
    python -m engine.eval.league_clustering --csv DIR
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import jensenshannon, squareform
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)

SOURCE_TABLE = "raw.fact_head_to_head"
MIN_LEAGUE_MATCHES: int = 30

CLUSTER_FEATURES: List[str] = [
    "mean_goals", "std_goals", "draw_rate",
    "home_win_rate", "p_over_2_5", "p_btts",
]

MAX_GOALS_FOR_DIST: int = 10


# ── data ────────────────────────────────────────────────────────────────────

def load_match_df(conn) -> pd.DataFrame:
    sql = f"""
        SELECT
            f.league_name AS league,
            rf.api_league_id AS league_id,
            f.fixture_id,
            f.goals_for AS home_goals,
            f.goals_against AS away_goals,
            f.fulltime_result
        FROM {SOURCE_TABLE} f
        JOIN raw.raw_fixtures rf ON rf.fixture_id = f.fixture_id
        WHERE f.is_played AND f.is_home
        ORDER BY f.kickoff_utc
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        raise RuntimeError("No played matches found.")
    return df


# ── league profiles ─────────────────────────────────────────────────────────

def build_league_profiles(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-league summary features from fixture-level data.

    Expects columns: league, home_goals, away_goals, fulltime_result.
    One row per fixture (home perspective only).
    """
    df = df.copy()
    df["total_goals"] = df["home_goals"] + df["away_goals"]
    df["is_draw"] = (df["home_goals"] == df["away_goals"]).astype(int)
    df["home_win"] = (df["home_goals"] > df["away_goals"]).astype(int)
    df["over_2_5"] = (df["total_goals"] > 2.5).astype(int)
    df["btts"] = ((df["home_goals"] > 0) & (df["away_goals"] > 0)).astype(int)

    # Group on the unique league id when available — bare league_name conflates
    # same-named leagues across countries (e.g. Brazil vs Italy "Serie B",
    # Egypt vs England "Premier League"). Fall back to name if id is absent.
    key = "league_id" if "league_id" in df.columns else "league"
    rows = []
    for kval, g in df.groupby(key):
        n = len(g)
        if n < MIN_LEAGUE_MATCHES:
            continue
        row = {
            "league": g["league"].iloc[0] if "league" in g.columns else kval,
            "n_matches": n,
            "mean_goals": g["total_goals"].mean(),
            "std_goals": g["total_goals"].std(),
            "draw_rate": g["is_draw"].mean(),
            "home_win_rate": g["home_win"].mean(),
            "p_over_2_5": g["over_2_5"].mean(),
            "p_btts": g["btts"].mean(),
        }
        if key == "league_id":
            row["league_id"] = kval
        rows.append(row)
    return pd.DataFrame(rows)


def build_goal_distributions(df: pd.DataFrame) -> Dict[str, np.ndarray]:
    """Build normalized goal-count distributions per league.

    Returns dict mapping league name to probability vector of length
    MAX_GOALS_FOR_DIST + 1 (indices 0..MAX_GOALS_FOR_DIST, where the
    last bin is MAX_GOALS_FOR_DIST+).
    """
    df = df.copy()
    df["total_goals"] = df["home_goals"] + df["away_goals"]

    dists: Dict[str, np.ndarray] = {}
    for league, g in df.groupby("league"):
        if len(g) < MIN_LEAGUE_MATCHES:
            continue
        counts = np.zeros(MAX_GOALS_FOR_DIST + 1)
        for tg in g["total_goals"]:
            idx = min(int(tg), MAX_GOALS_FOR_DIST)
            counts[idx] += 1
        total = counts.sum()
        if total > 0:
            counts /= total
            # avoid exact zeros for JSD (replace with tiny epsilon)
            counts = np.maximum(counts, 1e-10)
            counts /= counts.sum()
        dists[league] = counts
    return dists


# ── clustering helpers ───────────────────────────────────────────────────────

def _cut_at_largest_gap(Z: np.ndarray, n_items: int,
                        min_k: int = 2, max_k: int = 6) -> int:
    """Cut the dendrogram at the largest merge-height gap."""
    dists = Z[:, 2]
    gaps = np.diff(dists)
    cut_idx = int(np.argmax(gaps))
    k = n_items - cut_idx - 1
    return max(min_k, min(k, max_k))


def _rank_clusters_by_mean_goals(profiles: pd.DataFrame) -> pd.DataFrame:
    """Relabel clusters so cluster 1 = lowest scoring, highest = most goals."""
    cluster_means = profiles.groupby("cluster")["mean_goals"].mean()
    rank = cluster_means.rank().astype(int)
    profiles["cluster"] = profiles["cluster"].map(rank)
    return profiles


def cluster_ward(profiles: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray, int]:
    """Feature-based hierarchical clustering with Ward linkage.

    Returns (profiles with 'cluster' column, linkage matrix Z, n_clusters).
    """
    profiles = profiles.copy()
    X = profiles[CLUSTER_FEATURES].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    Z = linkage(X_scaled, method="ward")
    n = len(profiles)
    k = _cut_at_largest_gap(Z, n)
    profiles["cluster"] = fcluster(Z, t=k, criterion="maxclust")
    profiles = _rank_clusters_by_mean_goals(profiles)

    return profiles.sort_values(["cluster", "mean_goals"]), Z, k


def cluster_jsd(profiles: pd.DataFrame,
                goal_dists: Dict[str, np.ndarray]) -> Tuple[pd.DataFrame, np.ndarray, int]:
    """JSD-based hierarchical clustering with average linkage.

    Returns (profiles with 'cluster_jsd' column, linkage matrix Z, n_clusters).
    """
    profiles = profiles.copy()
    leagues = profiles["league"].tolist()
    n = len(leagues)

    dist_matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(i + 1, n):
            d = jensenshannon(goal_dists[leagues[i]], goal_dists[leagues[j]])
            dist_matrix[i, j] = d
            dist_matrix[j, i] = d

    condensed = squareform(dist_matrix)
    Z = linkage(condensed, method="average")
    k = _cut_at_largest_gap(Z, n)
    profiles["cluster_jsd"] = fcluster(Z, t=k, criterion="maxclust")

    cluster_means = profiles.groupby("cluster_jsd")["mean_goals"].mean()
    rank = cluster_means.rank().astype(int)
    profiles["cluster_jsd"] = profiles["cluster_jsd"].map(rank)

    return profiles.sort_values(["cluster_jsd", "mean_goals"]), Z, k


# ── dendrogram text printer ──────────────────────────────────────────────────

def print_dendrogram_text(Z: np.ndarray, labels: List[str],
                          title: str = "Dendrogram") -> None:
    """Print the merge schedule from the linkage matrix."""
    n = len(labels)
    cluster_members: dict = {i: [labels[i]] for i in range(n)}

    print(f"  {'Step':>4}  {'Distance':>8}  Merged")
    print(f"  {'----':>4}  {'--------':>8}  {'------'}")
    for i, (a, b, dist, _) in enumerate(Z):
        a, b = int(a), int(b)
        members_a = cluster_members.get(a, [f"cluster_{a}"])
        members_b = cluster_members.get(b, [f"cluster_{b}"])
        merged = members_a + members_b
        cluster_members[n + i] = merged
        left = ", ".join(members_a[:3])
        right = ", ".join(members_b[:3])
        if len(members_a) > 3:
            left += f" +{len(members_a) - 3}"
        if len(members_b) > 3:
            right += f" +{len(members_b) - 3}"
        print(f"  {i + 1:4d}  {dist:8.4f}  ({left}) ⊕ ({right})")


# ── reporting ───────────────────────────────────────────────────────────────

def run(csv_dir: Optional[str] = None, conn=None,
        df: Optional[pd.DataFrame] = None) -> Dict:
    close_conn = False
    if df is None:
        if conn is None:
            conn = get_db_connection()
            close_conn = True
        df = load_match_df(conn)

    profiles = build_league_profiles(df)
    goal_dists = build_goal_distributions(df)
    n_leagues = len(profiles)

    print(f"\n{'='*72}")
    print(f"  LEAGUE SCORING-STYLE CLUSTERING  ({n_leagues} leagues)")
    print(f"  Features: {CLUSTER_FEATURES}")
    print(f"{'='*72}")

    # ── League profiles
    print(f"\n── League profiles ──\n")
    with pd.option_context("display.max_rows", None, "display.width", 180,
                           "display.float_format", "{:.3f}".format):
        print(profiles.sort_values("mean_goals").to_string(index=False))

    # ── Ward clustering
    print(f"\n── Ward linkage (feature-based, z-scored) ──\n")
    ward_df, Z_ward, k_ward = cluster_ward(profiles)
    ward_labels = profiles.sort_index()["league"].tolist()
    print_dendrogram_text(Z_ward, ward_labels, "Ward")
    print(f"\n  Cut → {k_ward} clusters\n")

    for cl in sorted(ward_df["cluster"].unique()):
        members = ward_df[ward_df["cluster"] == cl]["league"].tolist()
        avg_goals = ward_df[ward_df["cluster"] == cl]["mean_goals"].mean()
        print(f"  Cluster {cl} (avg {avg_goals:.2f} goals): {', '.join(members)}")

    if n_leagues >= 3:
        X_ward = StandardScaler().fit_transform(
            ward_df[CLUSTER_FEATURES].values)
        sil = silhouette_score(X_ward, ward_df["cluster"].values)
        print(f"\n  Silhouette score: {sil:.3f} (informational only, N={n_leagues})")

    # ── JSD clustering
    print(f"\n── JSD (goal-count distribution, average linkage) ──\n")
    jsd_df, Z_jsd, k_jsd = cluster_jsd(profiles, goal_dists)
    print_dendrogram_text(Z_jsd, ward_labels, "JSD")
    print(f"\n  Cut → {k_jsd} clusters\n")

    for cl in sorted(jsd_df["cluster_jsd"].unique()):
        members = jsd_df[jsd_df["cluster_jsd"] == cl]["league"].tolist()
        avg_goals = jsd_df[jsd_df["cluster_jsd"] == cl]["mean_goals"].mean()
        print(f"  Cluster {cl} (avg {avg_goals:.2f} goals): {', '.join(members)}")

    if n_leagues >= 3:
        leagues = profiles["league"].tolist()
        n = len(leagues)
        jsd_matrix = np.zeros((n, n))
        for i in range(n):
            for j in range(i + 1, n):
                d = jensenshannon(goal_dists[leagues[i]], goal_dists[leagues[j]])
                jsd_matrix[i, j] = d
                jsd_matrix[j, i] = d
        sil_jsd = silhouette_score(jsd_matrix, jsd_df["cluster_jsd"].values,
                                   metric="precomputed")
        print(f"\n  Silhouette score: {sil_jsd:.3f} (informational only, N={n_leagues})")

    # ── Cluster comparison
    print(f"\n── Ward cluster averages ──\n")
    ward_agg = ward_df.groupby("cluster")[CLUSTER_FEATURES].mean().round(3)
    with pd.option_context("display.width", 160):
        print(ward_agg.to_string())

    print(f"\n── JSD cluster averages ──\n")
    jsd_agg = jsd_df.groupby("cluster_jsd")[CLUSTER_FEATURES].mean().round(3)
    with pd.option_context("display.width", 160):
        print(jsd_agg.to_string())

    if csv_dir:
        out = Path(csv_dir)
        out.mkdir(parents=True, exist_ok=True)
        ward_df.to_csv(out / "league_clusters_ward.csv", index=False)
        jsd_df.to_csv(out / "league_clusters_jsd.csv", index=False)
        print(f"\nCSVs written to {out}")

    if close_conn:
        conn.close()

    return {
        "profiles": profiles,
        "ward": (ward_df, Z_ward, k_ward),
        "jsd": (jsd_df, Z_jsd, k_jsd),
        "goal_distributions": goal_dists,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="League scoring-style clustering (hierarchical)."
    )
    parser.add_argument("--csv", metavar="DIR",
                        help="Write result CSVs to this directory.")
    args = parser.parse_args()
    run(csv_dir=args.csv)
