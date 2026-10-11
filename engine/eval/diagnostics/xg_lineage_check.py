"""One-off check for the xG lineage switch (raw.fixture_xg -> dbt fact_fixture_xg).

Run from the repo root after `dbt run -s +fact_fixture_xg` (needs the local DB):
    python -m engine.eval.diagnostics.xg_lineage_check

Compares, on the same Dixon-Coles history:
  * fixtures blended when xG is read from raw.fixture_xg (old path)
  * fixtures blended when xG is read from raw.fact_fixture_xg (new path)
and lists any fixture that differs, with the reason. Read-only.
Expected: identical counts, except raw rows the fact drops on purpose
(not played, or missing one side's xG).
"""
import pandas as pd

from engine.eval.db import get_conn
from engine.eval import predict_dixon_coles as dc


def _blend_from(table: str, hist: pd.DataFrame, conn) -> pd.DataFrame:
    old = dc.XG_TABLE
    dc.XG_TABLE = table
    try:
        return dc.blend_xg(hist.copy(), conn)
    finally:
        dc.XG_TABLE = old


def main() -> None:
    conn = get_conn()
    try:
        hist = dc.load_dc_historical(conn)
        a = _blend_from("raw.fixture_xg", hist, conn)
        b = _blend_from("raw.fact_fixture_xg", hist, conn)
        raw = pd.read_sql("SELECT fixture_id, home_xg, away_xg FROM raw.fixture_xg", conn)
        fact = pd.read_sql("SELECT fixture_id FROM raw.fact_fixture_xg", conn)
        played = pd.read_sql(
            "SELECT fixture_id, fixture_status FROM raw.fact_fixtures", conn)
    finally:
        conn.close()

    print(f"DC history rows: {len(hist)}")
    print(f"blended via raw.fixture_xg:      {a.attrs.get('n_blended', 0)}")
    print(f"blended via raw.fact_fixture_xg: {b.attrs.get('n_blended', 0)}")
    goals_equal = (
        a[["home_goals", "away_goals"]].round(9).equals(b[["home_goals", "away_goals"]].round(9)))
    print(f"blended goal columns identical: {goals_equal}")

    only_raw = raw[~raw.fixture_id.isin(fact.fixture_id)].merge(played, on="fixture_id", how="left")
    if only_raw.empty:
        print("every raw.fixture_xg row is in the fact")
        return
    def reason(r):
        if r.fixture_status != "FT":
            return f"not played ({r.fixture_status})"
        if pd.isna(r.home_xg) or pd.isna(r.away_xg):
            return "one side missing xG"
        return "not in int_fixtures (league/team join)"
    only_raw["reason"] = only_raw.apply(reason, axis=1)
    print(f"\nraw rows not in fact: {len(only_raw)}")
    print(only_raw.groupby("reason").size().to_string())
    in_hist = only_raw[only_raw.fixture_id.isin(hist.fixture_id)]
    print(f"of which used by DC history (would change the fit): {len(in_hist)}")
    if len(in_hist):
        print(in_hist.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
