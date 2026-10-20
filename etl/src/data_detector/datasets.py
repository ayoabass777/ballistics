"""Dataset tracker: the registry of datasets in the daily chain and their dependencies.

Source of truth is ``datasets.yml`` next to this file. ``sync_registry`` mirrors it
into ``data_detector.datasets`` and ``data_detector.dataset_dependencies`` so
lineage can be queried in SQL (and from dbt); the daily flow check calls it on
every run, so the tables never drift from the file.

CLI:
    python -m etl.src.data_detector.datasets validate
    python -m etl.src.data_detector.datasets lineage raw.consensus_pool
    python -m etl.src.data_detector.datasets sync
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import yaml

REGISTRY_PATH = Path(__file__).with_name("datasets.yml")
CHECK_TYPES = {"pipeline_task", "movement", "run_ds_rows", "derived", "sql_assert", "crest_urls", "dlq"}
LAYERS = {"source", "raw", "dbt", "engine", "api"}

DDL = """
CREATE SCHEMA IF NOT EXISTS data_detector;

CREATE TABLE IF NOT EXISTS data_detector.datasets (
  dataset_id    TEXT        PRIMARY KEY,
  layer         TEXT        NOT NULL,
  description   TEXT,
  producer_dag  TEXT,
  producer_task TEXT,
  check_spec    JSONB       NOT NULL,
  synced_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS data_detector.dataset_dependencies (
  dataset_id    TEXT        NOT NULL REFERENCES data_detector.datasets(dataset_id) ON DELETE CASCADE,
  upstream_id   TEXT        NOT NULL REFERENCES data_detector.datasets(dataset_id) ON DELETE CASCADE,
  PRIMARY KEY (dataset_id, upstream_id)
);
"""


@dataclass(frozen=True)
class Dataset:
    id: str
    layer: str
    description: str
    producer_dag: str | None
    producer_task: str | None
    upstream: tuple[str, ...]
    check: dict = field(hash=False, compare=False)


class Registry:
    def __init__(self, datasets: Iterable[Dataset]):
        self.datasets = {d.id: d for d in datasets}

    def __iter__(self):
        return iter(self.datasets.values())

    def __getitem__(self, dataset_id: str) -> Dataset:
        return self.datasets[dataset_id]

    def downstream(self, dataset_id: str) -> list[str]:
        """Every dataset that depends on dataset_id, directly or not."""
        out, frontier = [], [dataset_id]
        while frontier:
            current = frontier.pop()
            for d in self:
                if current in d.upstream and d.id not in out:
                    out.append(d.id)
                    frontier.append(d.id)
        return out

    def upstream_all(self, dataset_id: str) -> list[str]:
        out, frontier = [], list(self[dataset_id].upstream)
        while frontier:
            current = frontier.pop()
            if current not in out:
                out.append(current)
                frontier.extend(self[current].upstream)
        return out

    def topological(self) -> list[str]:
        """Upstream first. Validation guarantees there is no cycle."""
        order, seen = [], set()

        def visit(dataset_id: str) -> None:
            if dataset_id in seen:
                return
            seen.add(dataset_id)
            for up in self[dataset_id].upstream:
                visit(up)
            order.append(dataset_id)

        for d in self:
            visit(d.id)
        return order


def validate(raw: dict) -> list[str]:
    """Problems with a registry document; empty when it is usable."""
    errors = []
    items = raw.get("datasets") or []
    ids = [d.get("id") for d in items]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        errors.append(f"duplicate ids: {sorted(dupes)}")
    known = set(ids)
    for d in items:
        did = d.get("id")
        if d.get("layer") not in LAYERS:
            errors.append(f"{did}: layer must be one of {sorted(LAYERS)}")
        check = d.get("check") or {}
        if check.get("type") not in CHECK_TYPES:
            errors.append(f"{did}: check.type must be one of {sorted(CHECK_TYPES)}")
        if check.get("type") == "pipeline_task" and not (check.get("dag") and check.get("task")):
            errors.append(f"{did}: pipeline_task needs dag and task")
        if check.get("type") == "movement" and not (check.get("dag") and check.get("movement_type")):
            errors.append(f"{did}: movement needs dag and movement_type")
        if check.get("type") in ("sql_assert", "crest_urls") and not check.get("sql"):
            errors.append(f"{did}: {check.get('type')} needs sql")
        if check.get("type") == "dlq" and check.get("source") not in ("standings", "fixtures", "fixture_rows"):
            errors.append(f"{did}: dlq needs source standings, fixtures or fixture_rows")
        if check.get("type") == "run_ds_rows" and not (check.get("table") and check.get("column")):
            errors.append(f"{did}: run_ds_rows needs table and column")
        for up in d.get("upstream") or []:
            if up not in known:
                errors.append(f"{did}: unknown upstream {up}")
    if not errors:
        graph = {d["id"]: list(d.get("upstream") or []) for d in items}
        state: dict[str, int] = {}

        def has_cycle(node: str) -> bool:
            state[node] = 1
            for up in graph[node]:
                if state.get(up) == 1 or (state.get(up) is None and has_cycle(up)):
                    return True
            state[node] = 2
            return False

        if any(state.get(n) is None and has_cycle(n) for n in graph):
            errors.append("dependency cycle")
    return errors


def load_registry(path: Path = REGISTRY_PATH) -> Registry:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    errors = validate(raw)
    if errors:
        raise ValueError("invalid dataset registry: " + "; ".join(errors))
    return Registry(
        Dataset(
            id=d["id"], layer=d["layer"], description=d.get("description") or "",
            producer_dag=(d.get("producer") or {}).get("dag"),
            producer_task=(d.get("producer") or {}).get("task"),
            upstream=tuple(d.get("upstream") or ()), check=dict(d["check"]),
        )
        for d in raw["datasets"]
    )


def sync_registry(conn, registry: Registry) -> None:
    """Mirror the registry into the detector tables (one transaction)."""
    with conn.cursor() as cur:
        cur.execute(DDL)
        ids = list(registry.datasets)
        cur.execute("DELETE FROM data_detector.datasets WHERE NOT (dataset_id = ANY(%s))", (ids,))
        for d in registry:
            cur.execute(
                """INSERT INTO data_detector.datasets
                       (dataset_id, layer, description, producer_dag, producer_task, check_spec, synced_at)
                   VALUES (%s, %s, %s, %s, %s, %s, now())
                   ON CONFLICT (dataset_id) DO UPDATE SET
                       layer = EXCLUDED.layer, description = EXCLUDED.description,
                       producer_dag = EXCLUDED.producer_dag, producer_task = EXCLUDED.producer_task,
                       check_spec = EXCLUDED.check_spec, synced_at = now()""",
                (d.id, d.layer, d.description, d.producer_dag, d.producer_task, json.dumps(d.check)),
            )
        cur.execute("DELETE FROM data_detector.dataset_dependencies")
        for d in registry:
            for up in d.upstream:
                cur.execute(
                    "INSERT INTO data_detector.dataset_dependencies (dataset_id, upstream_id) VALUES (%s, %s)",
                    (d.id, up),
                )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataset registry and lineage.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate")
    lin = sub.add_parser("lineage")
    lin.add_argument("dataset_id")
    sub.add_parser("sync")
    args = parser.parse_args()
    registry = load_registry()
    if args.cmd == "validate":
        print(f"ok: {len(registry.datasets)} datasets")
    elif args.cmd == "lineage":
        print("upstream:  ", ", ".join(registry.upstream_all(args.dataset_id)) or "-")
        print("downstream:", ", ".join(registry.downstream(args.dataset_id)) or "-")
    elif args.cmd == "sync":
        from etl.src.extract_metadata import get_db_connection
        with get_db_connection() as conn:
            sync_registry(conn, registry)
        print(f"synced {len(registry.datasets)} datasets")


if __name__ == "__main__":
    main()


def effective_severity(dataset_id: str, results: dict, registry: "Registry") -> str:
    """Severity of a dataset's result for this check day.

    A check can downgrade its own result (e.g. crests not checked because the
    network was down) by setting result["severity"]; otherwise the registry's.
    """
    return (results.get(dataset_id) or {}).get("severity") or registry[dataset_id].check.get("severity", "error")
