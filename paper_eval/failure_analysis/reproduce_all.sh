#!/usr/bin/env bash
# ==============================================================================
# SpatialBench Failure & Timeout Analysis: Master Reproduction Suite
# ==============================================================================
# Reproduces the diagnostic experiments investigating why products failed or
# timed out on Scale Factor 10 (SF10) SpatialBench queries.
#
# Usage:
#   ./reproduce_all.sh               # Run fast diagnostic reproductions
#   ./reproduce_all.sh --full        # Run all reproductions including scaled sweeps
# ==============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common/env.sh"

MODE="${1:---fast}"

echo "=================================================================="
echo " SpatialBench Failure & Timeout Reproduction Suite"
echo " Host         : $(hostname -f)"
echo " SF10 Dataset : $SF10_DIR"
echo " Mode         : $MODE"
echo "=================================================================="

# 1. Environment capture
echo ""
echo ">>> Step 1: Capturing Environment and Dataset Signatures <<<"
bash "$SCRIPT_DIR/env/capture_env.sh"

# 2. Dataset statistics
echo ""
echo ">>> Step 2: Extracting Dataset Geometry Characteristics <<<"
if [ ! -f "$SCRIPT_DIR/01_dataset/dataset_stats_sf10.json" ]; then
    "$PY" "$SCRIPT_DIR/01_dataset/dataset_stats.py" "$SF10_DIR" "$SCRIPT_DIR/01_dataset/dataset_stats_sf10.json"
fi

# 3. ClickHouse failure replays & plan extraction
echo ""
echo ">>> Step 3: ClickHouse Diagnostics (Q4, Q10, Q12 error replay & plans) <<<"
bash "$SCRIPT_DIR/02_clickhouse/run_clickhouse.sh" B
bash "$SCRIPT_DIR/02_clickhouse/run_clickhouse.sh" A

# 4. DuckDB failure diagnostics & optimizer rewrite proof
echo ""
echo ">>> Step 4: DuckDB Diagnostics (Plans, Q9/Q11 optimizer bypass, Q10 polygon test, Q12 lateral OOM) <<<"
bash "$SCRIPT_DIR/03_duckdb/run_duckdb.sh" A
"$PY" "$SCRIPT_DIR/03_duckdb/duckdb_diag.py" run "$SF10_DIR" "$SCRIPT_DIR/03_duckdb/results" q9 \
    --sql "$SCRIPT_DIR/03_duckdb/sql/q9_rewrite_where.sql" \
    --set disabled_optimizers=filter_pushdown,cte_filter_pusher --tag q9_rewrite
"$PY" "$SCRIPT_DIR/03_duckdb/duckdb_diag.py" run "$SF10_DIR" "$SCRIPT_DIR/03_duckdb/results" q10 \
    --sql "$SCRIPT_DIR/03_duckdb/sql/q10_no_county.sql" --limit-table trip --limit 1000000 --tag no_county_1M
"$PY" "$SCRIPT_DIR/03_duckdb/duckdb_diag.py" run "$SF10_DIR" "$SCRIPT_DIR/03_duckdb/results" q10 \
    --sql "$SCRIPT_DIR/03_duckdb/sql/q10_county_only.sql" --limit-table trip --limit 1000000 --tag county_only_1M
"$PY" "$SCRIPT_DIR/03_duckdb/duckdb_diag.py" analyze "$SF10_DIR" "$SCRIPT_DIR/03_duckdb/plans" q12 \
    --limit-table trip --limit 100 --tag trip100

# 5. PostGIS failure diagnostics & KNN fix proof
echo ""
echo ">>> Step 5: PostGIS Diagnostics (Q9 GEOS TopologyException & Q12 KNN operator) <<<"
bash "$SCRIPT_DIR/04_postgis/run_postgis.sh" A
bash "$SCRIPT_DIR/04_postgis/run_postgis.sh" B
"$PY" "$SCRIPT_DIR/common/diag_run.py" --name q12_orig_trip1000 --timeout 120 \
    --record "$SCRIPT_DIR/04_postgis/results/diag_runs.jsonl" \
    --stdout "$SCRIPT_DIR/04_postgis/logs/q12_orig_trip1000.out" --stderr "$SCRIPT_DIR/04_postgis/logs/q12_orig_trip1000.err" \
    -- "$PY" "$SCRIPT_DIR/04_postgis/pg_diag.py" analyze "$SCRIPT_DIR/04_postgis/plans" q12_orig_trip1000 \
    --sql "$SCRIPT_DIR/04_postgis/sql/q12_orig_trip1000.sql" --timeout 100
"$PY" "$SCRIPT_DIR/common/diag_run.py" --name q12_knn_trip1000 --timeout 60 \
    --record "$SCRIPT_DIR/04_postgis/results/diag_runs.jsonl" \
    --stdout "$SCRIPT_DIR/04_postgis/logs/q12_knn_trip1000.out" --stderr "$SCRIPT_DIR/04_postgis/logs/q12_knn_trip1000.err" \
    -- "$PY" "$SCRIPT_DIR/04_postgis/pg_diag.py" analyze "$SCRIPT_DIR/04_postgis/plans" q12_knn_trip1000 \
    --sql "$SCRIPT_DIR/04_postgis/sql/q12_knn_trip1000.sql" --timeout 50

# 6. SedonaDB plan extraction
echo ""
echo ">>> Step 6: SedonaDB Execution Plan Extraction <<<"
"$PY" "$SCRIPT_DIR/07_sedonadb/sedonadb_diag.py" "$SF10_DIR" "$SCRIPT_DIR/07_sedonadb/plans"

# 7. Generate publication-ready figures
echo ""
echo ">>> Step 7: Generating Publication Figures <<<"
"$PY" "$SCRIPT_DIR/plot_failures.py"

echo ""
echo "=================================================================="
echo " [SUCCESS] SpatialBench Failure Analysis Reproduction Completed"
echo " All experimental logs, plans, and plots are stored in:"
echo "   $SCRIPT_DIR"
echo "=================================================================="
