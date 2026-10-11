"""
Elo → goal-outcome diagnostic.

Measures how pre-match Elo gap affects goal outcomes per league from the
favorite's perspective.  Feature-selection tool: does Elo deserve a spot in
over-1.5 / over-2.5 classifiers, or does league scoring style dominate?

Outputs
-------
1. Per league × Elo-gap band summary table.
2. Global NegBin GLM: total_goals ~ abs_gap (+league FE).
3. Per-league gap slope.

Usage:
    python -m engine.eval.elo_goals              # print to stdout
    python -m engine.eval.elo_goals --csv DIR    # write CSVs
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from statsmodels.discrete.discrete_model import NegativeBinomial

from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__)

# ── config ──────────────────────────────────────────────────────────────────
GAP_EDGES: List[int] = [0, 50, 100, 200]
MIN_RELIABLE_N: int = 30
GAP_SCALE: float = 100.0  # report coefficient per +100 Elo

SOURCE_TABLE = "raw.fact_head_to_head"
ELO_TABLE = "elo.team_elo_history"


# ── data layer ──────────────────────────────────────────────────────────────

def _load_match_df(conn) -> pd.DataFrame:
    """
    Build one-row-per-match dataframe with pre-match Elo for both sides.

    Uses rating_before from elo.team_elo_history — the snapshot stored
    strictly before each fixture's Elo update.
    """
    sql = f"""
        SELECT
            f.fixture_id,
            f.league_name                     AS league,
            f.goals_for                       AS home_goals,
            f.goals_against                   AS away_goals,
            eh.rating_before                  AS home_elo,
            ea.rating_before                  AS away_elo
        FROM {SOURCE_TABLE} f
        JOIN {ELO_TABLE} eh
          ON eh.team_id    = f.team_id
         AND eh.fixture_id = f.fixture_id
        JOIN {ELO_TABLE} ea
          ON ea.team_id    = f.opponent_id
         AND ea.fixture_id = f.fixture_id
        WHERE f.is_played
          AND f.is_home
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        raise RuntimeError("No matches found — has elo_service been run?")
    return df


def build_analysis_df(conn=None, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Return an analysis-ready dataframe.  Accepts either a live DB connection
    or a pre-built dataframe (for testing).
    """
    if df is None:
        if conn is None:
            raise ValueError("Provide either conn or df")
        df = _load_match_df(conn)

    df = df.copy()
    df["abs_gap"] = (df["home_elo"] - df["away_elo"]).abs()
    df["total_goals"] = df["home_goals"] + df["away_goals"]

    home_is_fav = df["home_elo"] >= df["away_elo"]
    df["fav_goals"] = np.where(home_is_fav, df["home_goals"], df["away_goals"])
    df["dog_goals"] = np.where(home_is_fav, df["away_goals"], df["home_goals"])
    df["margin"] = df["fav_goals"] - df["dog_goals"]

    edges = GAP_EDGES + [np.inf]
    labels = [f"{lo}-{hi}" if hi != np.inf else f"{lo}+"
              for lo, hi in zip(edges[:-1], edges[1:])]
    df["gap_band"] = pd.cut(df["abs_gap"], bins=edges, labels=labels,
                            right=False, include_lowest=True)
    return df


# ── deliverable 1: per-league × gap-band table ─────────────────────────────

def _bucket_label(total: int) -> str:
    if total <= 1:
        return "0-1"
    if total <= 3:
        return "2-3"
    return "4+"


def gap_band_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Per league × gap_band summary with sample sizes and reliability flag."""
    rows = []
    for (league, band), g in df.groupby(["league", "gap_band"], observed=True):
        n = len(g)
        total = g["total_goals"]
        buckets = g["total_goals"].apply(_bucket_label).value_counts()
        rows.append({
            "league": league,
            "gap_band": band,
            "n": n,
            "mean_total": total.mean(),
            "std_total": total.std(),
            "p_over_1_5": (total > 1.5).mean(),
            "p_over_2_5": (total > 2.5).mean(),
            "pct_0_1": buckets.get("0-1", 0) / n,
            "pct_2_3": buckets.get("2-3", 0) / n,
            "pct_4_plus": buckets.get("4+", 0) / n,
            "mean_margin": g["margin"].mean(),
            "fav_goals_for": g["fav_goals"].mean(),
            "fav_goals_against": g["dog_goals"].mean(),
            "reliable": n >= MIN_RELIABLE_N,
        })
    return pd.DataFrame(rows)


# ── deliverable 2: global NegBin GLM ───────────────────────────────────────

@dataclass
class GLMResult:
    model_label: str
    gap_coef_per_100: float
    gap_pvalue: float
    alpha: float
    aic: float
    n: int
    poisson_aic: float
    overdispersed: bool


def _fit_negbin(y: pd.Series, X: pd.DataFrame, label: str) -> GLMResult:
    """Fit NegBin by MLE, compare AIC to Poisson.  Falls back to Poisson
    coefficients when NegBin Hessian is singular (alpha ≈ 0)."""
    import warnings
    X_c = sm.add_constant(X.astype(float))

    pois = sm.GLM(y.values, X_c.values,
                  family=sm.families.Poisson()).fit()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        nb = NegativeBinomial(y.values, X_c.values).fit(disp=0, maxiter=300)

    gap_idx = list(X.columns).index("abs_gap") + 1  # +1 for constant

    nb_pval = nb.pvalues[gap_idx] if nb.pvalues is not None else float("nan")
    if np.isnan(nb_pval):
        raw_coef = pois.params[gap_idx]
        raw_pval = pois.pvalues[gap_idx]
    else:
        raw_coef = nb.params[gap_idx]
        raw_pval = nb_pval

    return GLMResult(
        model_label=label,
        gap_coef_per_100=raw_coef * GAP_SCALE,
        gap_pvalue=raw_pval,
        alpha=float(nb.params[-1]),
        aic=float(nb.aic),
        n=len(y),
        poisson_aic=float(pois.aic),
        overdispersed=bool(nb.aic < pois.aic),
    )


def global_glm(df: pd.DataFrame) -> Tuple[GLMResult, GLMResult]:
    """
    Fit total_goals ~ abs_gap (gap-only) and ~ abs_gap + C(league).
    Returns (gap_only, gap_plus_league).
    """
    y = df["total_goals"]

    X_gap = df[["abs_gap"]].copy()
    gap_only = _fit_negbin(y, X_gap, "gap_only")

    league_dummies = pd.get_dummies(df["league"], prefix="league", drop_first=True,
                                    dtype=float)
    X_full = pd.concat([df[["abs_gap"]].reset_index(drop=True),
                        league_dummies.reset_index(drop=True)], axis=1)
    gap_league = _fit_negbin(y, X_full, "gap_plus_league")

    return gap_only, gap_league


# ── deliverable 3: per-league gap slope ─────────────────────────────────────

def per_league_slopes(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for league, g in df.groupby("league"):
        if len(g) < MIN_RELIABLE_N:
            continue
        y = g["total_goals"]
        X = g[["abs_gap"]].copy()
        try:
            res = _fit_negbin(y, X, league)
            rows.append({
                "league": league,
                "n": res.n,
                "gap_coef_per_100": res.gap_coef_per_100,
                "gap_pvalue": res.gap_pvalue,
                "alpha": res.alpha,
                "negbin_aic": res.aic,
                "poisson_aic": res.poisson_aic,
                "overdispersed": res.overdispersed,
            })
        except Exception as e:
            logger.warning("NegBin failed for %s (n=%d): %s", league, len(g), e)
    return pd.DataFrame(rows)


# ── deliverable 4: league scoring-style clustering ──────────────────────────

CLUSTER_FEATURES = [
    "mean_total", "std_total", "p_over_1_5", "p_over_2_5",
    "pct_0_1", "pct_4_plus", "mean_margin", "fav_goals_for",
]
N_CLUSTERS_RANGE = range(2, 7)


def _league_profile(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-league scoring profile (across all gap bands)."""
    rows = []
    for league, g in df.groupby("league"):
        total = g["total_goals"]
        n = len(g)
        if n < MIN_RELIABLE_N:
            continue
        rows.append({
            "league": league,
            "n": n,
            "mean_total": total.mean(),
            "std_total": total.std(),
            "p_over_1_5": (total > 1.5).mean(),
            "p_over_2_5": (total > 2.5).mean(),
            "pct_0_1": (total <= 1).mean(),
            "pct_4_plus": (total >= 4).mean(),
            "mean_margin": g["margin"].mean(),
            "fav_goals_for": g["fav_goals"].mean(),
        })
    return pd.DataFrame(rows)


def cluster_leagues(df: pd.DataFrame, n_clusters: Optional[int] = None
                    ) -> Tuple[pd.DataFrame, int]:
    """
    K-Means clustering on league scoring profiles.

    If n_clusters is None, picks the k in [2,6] that minimises inertia
    drop-off (elbow heuristic via second-difference).

    Returns (profile_df with 'cluster' column, chosen k).
    """
    profiles = _league_profile(df)
    if len(profiles) < 3:
        profiles["cluster"] = 0
        return profiles, 1

    X = profiles[CLUSTER_FEATURES].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    if n_clusters is None:
        inertias = []
        for k in N_CLUSTERS_RANGE:
            if k >= len(profiles):
                break
            km = KMeans(n_clusters=k, n_init=10, random_state=42)
            km.fit(X_scaled)
            inertias.append((k, km.inertia_))
        if len(inertias) < 3:
            n_clusters = inertias[0][0]
        else:
            diffs = [inertias[i][1] - inertias[i+1][1] for i in range(len(inertias)-1)]
            second_diffs = [diffs[i] - diffs[i+1] for i in range(len(diffs)-1)]
            best_idx = int(np.argmax(second_diffs))
            n_clusters = inertias[best_idx + 1][0]

    km = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
    profiles["cluster"] = km.fit_predict(X_scaled)

    cluster_means = profiles.groupby("cluster")["mean_total"].mean()
    rank = cluster_means.rank().astype(int)
    profiles["cluster"] = profiles["cluster"].map(rank)

    return profiles.sort_values(["cluster", "mean_total"]), n_clusters


def cluster_leagues_hierarchical(
    df: pd.DataFrame, n_clusters: Optional[int] = None,
    method: str = "ward",
) -> Tuple[pd.DataFrame, np.ndarray]:
    """Hierarchical (agglomerative) clustering on league scoring profiles.

    Returns (profile_df with 'cluster' column, linkage matrix Z).
    If n_clusters is None, cuts at the biggest gap in the dendrogram.
    """
    profiles = _league_profile(df)
    if len(profiles) < 3:
        profiles["cluster"] = 0
        return profiles, np.array([])

    X = profiles[CLUSTER_FEATURES].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    Z = linkage(X_scaled, method=method)

    if n_clusters is None:
        dists = Z[:, 2]
        gaps = np.diff(dists)
        cut_idx = int(np.argmax(gaps))
        n_clusters = len(profiles) - cut_idx - 1
        n_clusters = max(2, min(n_clusters, 6))

    labels = fcluster(Z, t=n_clusters, criterion="maxclust")
    profiles["cluster"] = labels

    cluster_means = profiles.groupby("cluster")["mean_total"].mean()
    rank = cluster_means.rank().astype(int)
    profiles["cluster"] = profiles["cluster"].map(rank)

    return profiles.sort_values(["cluster", "mean_total"]), Z


def print_dendrogram_text(Z: np.ndarray, labels: List[str]) -> None:
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
            left += f" +{len(members_a)-3}"
        if len(members_b) > 3:
            right += f" +{len(members_b)-3}"
        print(f"  {i+1:4d}  {dist:8.3f}  ({left}) ⊕ ({right})")


# ── reporting ───────────────────────────────────────────────────────────────

def _format_glm(res: GLMResult) -> str:
    lines = [
        f"  Model: {res.model_label}  (n={res.n})",
        f"  gap coef (per +100 Elo): {res.gap_coef_per_100:+.4f}  "
        f"p={res.gap_pvalue:.4g}",
        f"  NegBin alpha: {res.alpha:.4f}   AIC: {res.aic:.1f}",
        f"  Poisson AIC:  {res.poisson_aic:.1f}  "
        f"→ overdispersed={'yes' if res.overdispersed else 'no'}",
    ]
    return "\n".join(lines)


def run(csv_dir: Optional[str] = None, conn=None,
        df: Optional[pd.DataFrame] = None) -> dict:
    """
    Full diagnostic.  Returns dict of result dataframes / objects.
    Prints a summary to stdout.  If csv_dir is given, writes CSVs.
    """
    close_conn = False
    if df is None:
        if conn is None:
            conn = get_db_connection()
            close_conn = True
        df = build_analysis_df(conn=conn)

    adf = build_analysis_df(df=df) if "abs_gap" not in df.columns else df

    print(f"\n{'='*72}")
    print(f"  ELO → GOAL OUTCOME DIAGNOSTIC   ({len(adf)} matches)")
    print(f"{'='*72}\n")

    band_df = gap_band_summary(adf)
    print("── 1. Per league × gap band ──\n")
    with pd.option_context("display.max_rows", None, "display.width", 140,
                           "display.float_format", "{:.3f}".format):
        print(band_df.to_string(index=False))

    print(f"\n── 2. Global NegBin GLM ──\n")
    gap_only, gap_league = global_glm(adf)
    print(_format_glm(gap_only))
    print()
    print(_format_glm(gap_league))
    survives = (gap_league.gap_pvalue < 0.05)
    print(f"\n  → Gap effect {'SURVIVES' if survives else 'DOES NOT SURVIVE'} "
          f"league fixed effects (p={gap_league.gap_pvalue:.4g})")

    print(f"\n── 3. Per-league gap slope ──\n")
    slope_df = per_league_slopes(adf)
    with pd.option_context("display.max_rows", None, "display.width", 140,
                           "display.float_format", "{:.4f}".format):
        print(slope_df.to_string(index=False))

    print(f"\n── 4. League scoring-style clusters ──\n")
    cluster_df, k = cluster_leagues(adf)
    print(f"  k = {k} clusters (elbow heuristic)\n")
    with pd.option_context("display.max_rows", None, "display.width", 160,
                           "display.float_format", "{:.3f}".format):
        print(cluster_df.to_string(index=False))

    print(f"\n  Cluster averages:")
    cluster_agg = (cluster_df.groupby("cluster")[CLUSTER_FEATURES]
                   .mean().round(3))
    with pd.option_context("display.width", 160):
        print(cluster_agg.to_string())

    print(f"\n── 5. Hierarchical clustering (Ward linkage) ──\n")
    hc_df, Z = cluster_leagues_hierarchical(adf)
    if Z.size > 0:
        print_dendrogram_text(Z, hc_df.sort_index()["league"].tolist())
        n_hc = hc_df["cluster"].nunique()
        print(f"\n  Cut → {n_hc} clusters\n")
        with pd.option_context("display.max_rows", None, "display.width", 160,
                               "display.float_format", "{:.3f}".format):
            print(hc_df.to_string(index=False))
        print(f"\n  Hierarchical cluster averages:")
        hc_agg = (hc_df.groupby("cluster")[CLUSTER_FEATURES].mean().round(3))
        with pd.option_context("display.width", 160):
            print(hc_agg.to_string())

    if csv_dir:
        out = Path(csv_dir)
        out.mkdir(parents=True, exist_ok=True)
        band_df.to_csv(out / "elo_gap_band_summary.csv", index=False)
        slope_df.to_csv(out / "elo_per_league_slopes.csv", index=False)
        cluster_df.to_csv(out / "elo_league_clusters.csv", index=False)
        hc_df.to_csv(out / "elo_league_clusters_hierarchical.csv", index=False)
        glm_rows = [
            {"model": r.model_label, "gap_coef_per_100": r.gap_coef_per_100,
             "gap_pvalue": r.gap_pvalue, "alpha": r.alpha, "aic": r.aic,
             "poisson_aic": r.poisson_aic, "overdispersed": r.overdispersed,
             "n": r.n}
            for r in (gap_only, gap_league)
        ]
        pd.DataFrame(glm_rows).to_csv(out / "elo_global_glm.csv", index=False)
        print(f"\nCSVs written to {out}")

    if close_conn:
        conn.close()

    return {
        "analysis_df": adf,
        "band_summary": band_df,
        "global_glm": (gap_only, gap_league),
        "league_slopes": slope_df,
        "league_clusters": cluster_df,
    }


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Elo → goal outcome diagnostic (feature selection)."
    )
    parser.add_argument("--csv", metavar="DIR",
                        help="Write result CSVs to this directory.")
    args = parser.parse_args()
    run(csv_dir=args.csv)
