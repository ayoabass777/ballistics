"""The research API image installs only engine/api/requirements.txt.

On 2026-11-18 match details started importing engine.eval.team_patterns,
which needs scipy, and every /api/match/<id> returned 500 on staging because
the image didn't have it. This walks every engine module the API imports
(directly or inside functions) and checks each third-party package it needs
is in the API's requirements.
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API_REQUIREMENTS = ROOT / "engine" / "api" / "requirements.txt"
# Import name -> distribution name, where they differ.
DIST = {"psycopg2": "psycopg2-binary"}
# Imported only in tests or behind guards the API never reaches.
IGNORE = {"dotenv"}


def requirements() -> set[str]:
    names = set()
    for line in API_REQUIREMENTS.read_text().splitlines():
        line = line.split("#")[0].strip()
        if line:
            names.add(line.split("==")[0].split(">=")[0].split("[")[0].strip().lower())
    return names


def module_path(name: str) -> Path | None:
    base = ROOT.joinpath(*name.split("."))
    for p in (base.with_suffix(".py"), base / "__init__.py"):
        if p.exists():
            return p
    return None


def imports(path: Path) -> set[str]:
    out = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.add(node.module)
            # "from engine.eval import x" may name a submodule
            out.update(f"{node.module}.{a.name}" for a in node.names)
    return out


def third_party_needed_by_api() -> dict[str, set[str]]:
    todo = [f"engine.api.{p.stem}" for p in (ROOT / "engine" / "api").glob("*.py")]
    seen, needs = set(), {}
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        path = module_path(name)
        if path is None:
            continue
        for imp in imports(path):
            top = imp.split(".")[0]
            if top == "engine":
                todo.append(imp)
            elif top not in sys.stdlib_module_names and top not in IGNORE:
                needs.setdefault(DIST.get(top, top).lower(), set()).add(name)
    return needs


def test_research_api_requirements_cover_its_imports():
    have = requirements()
    missing = {pkg: sorted(mods) for pkg, mods in third_party_needed_by_api().items()
               if pkg not in have}
    assert not missing, f"add to engine/api/requirements.txt: {missing}"
