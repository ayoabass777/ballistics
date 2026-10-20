"""The engine models league play: knockout games are excluded (2026-10-12).

Modelling code reads the engine_* views (fact tables minus is_knockout games).
Only the match API reads the full head-to-head table; fixture lookups by id
may use raw.fact_fixtures.
"""
import re
from pathlib import Path

ENGINE = Path("engine")
VIEWS = Path("dbt/football_pipeline/models/facts")
FULL_H2H_ALLOWED = {"engine/api/match_context.py"}


def _sources():
    for p in ENGINE.rglob("*.py"):
        if "diagnostics" in p.parts:
            continue
        yield str(p), p.read_text()


def test_no_modelling_code_reads_full_head_to_head():
    pat = re.compile(r"raw\.fact_head_to_head(_events)?\b")
    offenders = [p for p, src in _sources()
                 if p not in FULL_H2H_ALLOWED and pat.search(
                     "\n".join(l for l in src.splitlines() if not l.strip().startswith("#")))]
    # docstrings may mention the source table; SQL must not
    offenders = [p for p in offenders
                 if re.search(r"(FROM|JOIN|TABLE\s*=)\s*[\"']?raw\.fact_head_to_head", open(p).read())]
    assert offenders == []


def test_full_fixtures_only_for_id_lookups():
    for p, src in _sources():
        for m in re.finditer(r"FROM raw\.fact_fixtures\b(?P<rest>[^\"]{0,200})", src):
            rest = m.group("rest")
            if p in FULL_H2H_ALLOWED:
                continue
            assert "fixture_id = ANY" in rest or "fixture_id = %(fixture_id)s" in rest \
                or "is_played AND league_name" in rest \
                or "fixture_status = 'NS'" in rest and "scrape_sportybet" in p \
                or "fixture_status = 'FT'" in rest and "results_review" in p \
                or rest.strip().startswith('"""') or rest.strip() == "", (p, rest[:80])


def test_engine_views_drop_knockouts():
    for name in ("engine_fixtures", "engine_head_to_head", "engine_head_to_head_events"):
        sql = (VIEWS / f"{name}.sql").read_text()
        assert "is_knockout is not true" in sql, name
