"""market_watch logs D120 rows to raw.market_watch (was engine/eval/watch/*.csv)."""
import json

from engine.eval.diagnostics import market_watch as mw

OVER = "Match Over 1.5"


def preds(n, prob, hits, models="dixon_coles"):
    return [{"market": OVER, "models": models, "predicted_prob": prob, "actual": i < hits}
            for i in range(n)]


class FakeConn:
    """Holds raw.market_watch rows as dicts; answers SELECT_SQL and INSERT_SQL."""

    def __init__(self, rows=()):
        self.rows = [dict(r) for r in rows]
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        pass


class FakeCursor:
    def __init__(self, conn):
        self.conn, self.result = conn, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        if sql is mw.SELECT_SQL:
            self.result = [(r["report_stamp"], r["source"], int(r["n"]), float(r["brier"]))
                           for r in self.conn.rows if r["market"] == params[0]]
        elif sql is mw.INSERT_SQL:
            row = dict(zip(mw.FIELDS, params))
            key = (row["market"], row["source"], row["report_stamp"])
            if all((r["market"], r["source"], r["report_stamp"]) != key for r in self.conn.rows):
                self.conn.rows.append(row)
        else:
            raise AssertionError(sql)

    def fetchall(self):
        return self.result


def row(stamp, source, n, brier):
    return {"logged_at": "x", "report_stamp": stamp, "market": OVER, "source": source,
            "n": n, "avg_predicted": 0.75, "hit_rate": 0.75, "overconfidence": 0,
            "brier": brier, "flag": ""}


def test_new_rows_flags_against_the_first_big_enough_baseline():
    existing = [row("r0", "all", 50, 0.10),            # too small to be the baseline
                row("r1", "all", 200, 0.15)]
    p = preds(200, 0.9, 150)                          # predicted 90%, hit 75%
    rows = mw.new_rows(p, "r2", existing, now="t")
    assert [r["source"] for r in rows] == ["all", "dixon_coles_only"]
    flag = rows[0]["flag"]
    assert "overconfidence +15.0 pts" in flag
    assert "Brier 0.1500 ->" in flag
    assert all(r["report_stamp"] == "r2" and r["logged_at"] == "t" for r in rows)


def test_new_rows_skips_a_logged_report_and_empty_slices():
    existing = [row("r2", "all", 200, 0.15)]
    rows = mw.new_rows(preds(10, 0.8, 8, models="bayesian"), "r2", existing)
    assert rows == []                                  # all: logged; DC alone: no picks


def test_run_writes_the_table_once_per_report(tmp_path):
    report = tmp_path / "calibration_report.json"
    report.write_text(json.dumps({"generated": "r9", "predictions": preds(120, 0.8, 96)}))
    conn = FakeConn([row("r1", "all", 200, 0.16)])
    mw.run(report, conn=conn)
    mw.run(report, conn=conn)
    assert [(r["report_stamp"], r["source"]) for r in conn.rows] == [
        ("r1", "all"), ("r9", "all"), ("r9", "dixon_coles_only")]
    assert conn.commits == 2


def test_import_csv_keeps_file_order_and_skips_rows_already_there(tmp_path):
    path = tmp_path / "match_over_1_5.csv"
    path.write_text(
        ",".join(mw.FIELDS) + "\n"
        "2026-10-12T18:24:12Z,r1,Match Over 1.5,all,3769,0.763,0.7854,-0.0223,0.1668,\n"
        "2026-11-15T00:24:12Z,r5,Match Over 1.5,dixon_coles_only,636,0.74,0.79,-0.05,0.17,"
        "overconfidence -5.0 pts\n")
    conn = FakeConn([row("r1", "all", 3769, 0.1668)])
    assert mw.import_csv(path, conn=conn) == 2
    assert [r["report_stamp"] for r in conn.rows] == ["r1", "r5"]
    assert conn.rows[1]["flag"] == "overconfidence -5.0 pts"
