#!/usr/bin/env bash
# Refit calibration on the rebuilt history, then rerun the engine for today.
# Keeps a copy of the old calibration maps/report and prints a before/after
# summary. Run from the repo root:  ./scripts/refit_and_rerun.sh [YYYY-MM-DD]
set -euo pipefail
ROOT="$(pwd)"; set -a; source .env; set +a
DS="${1:-$(date -u +%Y-%m-%d)}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BK="engine/eval/calibration_backup_$STAMP"
mkdir -p "$BK"
cp engine/eval/calibration_maps.json engine/eval/calibration_report.json "$BK"/ 2>/dev/null || true
echo "== old calibration saved to $BK"

echo "== refit calibration (walk-forward; can take a while)"
docker run --rm --network ballistics_network \
  -v "$ROOT/engine:/opt/engine" -v "$ROOT/etl:/opt/etl" -v "$ROOT/data:/opt/data" \
  -e DB_NAME -e DB_USER -e DB_PASSWORD -e DB_PORT \
  -e DB_HOST=ballistics_postgres -e ENGINE_DB_HOST=ballistics_postgres -w /opt \
  ballistics-engine:latest python -m engine.eval.calibration_wf

echo "== calibration report: before vs after"
python3 - "$BK/calibration_report.json" engine/eval/calibration_report.json <<'PY' || true
import json, sys
def load(p):
    try: return json.load(open(p))
    except Exception: return {}
old, new = load(sys.argv[1]), load(sys.argv[2])
def view(r):
    v = {"overall": {"n": r.get("n"), "brier": r.get("brier_score"),
                     "predicted": r.get("avg_predicted"), "hit": r.get("overall_hit_rate"),
                     "overconf": r.get("overconfidence")}}
    for m, d in (r.get("per_model") or {}).items():
        v[m] = {"n": d.get("n"), "brier": d.get("brier"), "predicted": d.get("avg_predicted"),
                "hit": d.get("actual_hit_rate"), "overconf": d.get("overconfidence")}
    return v
o, n = view(old), view(new)
print(f"  {'':<12}{'n':>14}{'brier':>18}{'predicted':>18}{'hit rate':>18}{'overconf':>18}")
for k in [x for x in ["overall", "mismatch", "dixon_coles", "bayesian"] if x in o or x in n]:
    cells = "".join(f"{str(o.get(k,{}).get(f,'-')) + ' > ' + str(n.get(k,{}).get(f,'-')):>18}" if f != "n"
                    else f"{str(o.get(k,{}).get(f,'-')) + '>' + str(n.get(k,{}).get(f,'-')):>14}"
                    for f in ["n", "brier", "predicted", "hit", "overconf"])
    print(f"  {k:<12}{cells}")
PY

echo "== market watch (D120)"
python3 -m engine.eval.diagnostics.market_watch || true

echo "== rerun engine for $DS"
./scripts/rerun_engine.sh "$DS"
echo "== done. New export: engine/eval/consensus_slips_$DS.json"
