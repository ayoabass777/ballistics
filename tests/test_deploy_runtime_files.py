"""deploy_staging.sh keeps the engine's runtime files across a deploy.

The deploy's forced checkout used to reset engine/eval/calibration_maps.json,
calibration_report.json and watch/*.csv to their committed copies, throwing
away the server's weekly refit. checkout_commit now keeps the server's
versions; these tests run it against a throwaway git repo.
"""
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "deploy_staging.sh"

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], check=True,
                          capture_output=True, text=True).stdout.strip()


def write(root, rel, text):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def commit(root, msg):
    git(root, "add", "-A")
    git(root, "-c", "user.name=t", "-c", "user.email=t@example.com",
        "commit", "--quiet", "-m", msg)
    return git(root, "rev-parse", "HEAD")


def checkout_commit(root, sha):
    subprocess.run(["bash", "-c", f'source "{SCRIPT}" && checkout_commit "$1" "$2"',
                    "_", str(root), sha], check=True, capture_output=True, text=True)


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "srv"
    root.mkdir()
    git(root, "init", "--quiet")
    for rel in ("data/metadata.yaml", "frontend_dev/Dockerfile", "API/Dockerfile"):
        write(root, rel, "x\n")
    write(root, "engine/eval/calibration_maps.json", '{"maps": "committed v1"}\n')
    write(root, "engine/eval/calibration_report.json", '{"report": "committed v1"}\n')
    write(root, "engine/eval/watch/match_over_1_5.csv", "row,committed\n")
    write(root, "engine/eval/model.py", "VERSION = 1\n")
    first = commit(root, "first")
    # The next release changes code and, as a local refit would, the
    # committed maps.
    write(root, "engine/eval/model.py", "VERSION = 2\n")
    write(root, "engine/eval/calibration_maps.json", '{"maps": "committed v2"}\n')
    second = commit(root, "second")
    git(root, "checkout", "--quiet", "--detach", first)
    return root, first, second


def refit_on_server(root):
    write(root, "engine/eval/calibration_maps.json", '{"maps": "server refit"}\n')
    write(root, "engine/eval/calibration_report.json", '{"report": "server refit"}\n')
    write(root, "engine/eval/watch/match_over_1_5.csv", "row,committed\nrow,server\n")
    # An untracked watch file the engine container (root) owns: the deploy
    # must leave it alone rather than copy over it.
    extra = root / "engine/eval/watch/other_market.csv"
    extra.write_text("row,server only\n")
    extra.chmod(0o444)


def read(root, rel):
    return (root / rel).read_text()


def test_deploy_keeps_the_servers_refit(repo):
    root, _, second = repo
    refit_on_server(root)

    checkout_commit(root, second)

    assert git(root, "rev-parse", "HEAD") == second
    assert read(root, "engine/eval/model.py") == "VERSION = 2\n"
    assert read(root, "engine/eval/calibration_maps.json") == '{"maps": "server refit"}\n'
    assert read(root, "engine/eval/calibration_report.json") == '{"report": "server refit"}\n'
    assert read(root, "engine/eval/watch/match_over_1_5.csv") == "row,committed\nrow,server\n"
    assert read(root, "engine/eval/watch/other_market.csv") == "row,server only\n"
    assert not (root / "deploy/runtime-keep").exists()


def test_fresh_server_starts_from_the_committed_files(tmp_path):
    root = tmp_path / "srv"
    source = tmp_path / "src"
    source.mkdir()
    git(source, "init", "--quiet")
    for rel in ("data/metadata.yaml", "frontend_dev/Dockerfile", "API/Dockerfile"):
        write(source, rel, "x\n")
    write(source, "engine/eval/calibration_maps.json", '{"maps": "committed"}\n')
    sha = commit(source, "first")
    # What push_commit does on a new server: an empty repo, then the commit.
    git(tmp_path, "init", "--quiet", str(root))
    git(source, "push", "--quiet", str(root), f"{sha}:refs/deploy/release")

    checkout_commit(root, sha)

    assert read(root, "engine/eval/calibration_maps.json") == '{"maps": "committed"}\n'
    assert not (root / "deploy/runtime-keep").exists()


def test_interrupted_deploy_restores_from_the_kept_copies(repo):
    root, _, second = repo
    refit_on_server(root)
    # A deploy that stopped after copying the files aside and after the
    # checkout reset them, but before putting them back.
    keep = root / "deploy/runtime-keep/engine/eval"
    keep.mkdir(parents=True)
    shutil.copy(root / "engine/eval/calibration_maps.json", keep)
    git(root, "checkout", "--quiet", "--", "engine/eval/calibration_maps.json")
    assert "committed v1" in read(root, "engine/eval/calibration_maps.json")

    checkout_commit(root, second)

    assert read(root, "engine/eval/calibration_maps.json") == '{"maps": "server refit"}\n'
    assert not (root / "deploy/runtime-keep").exists()
