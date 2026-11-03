#!/usr/bin/env bash
# ==============================================================================
# Sedona SpatialBench: One-Hit Reproduction Runner
# ==============================================================================
# Reproduces Section 5 experiments from the PVLDB 2027 paper in a single command.
#
# Usage:
#   ./run_me.sh
#   ./run_me.sh --sf "1 10" --engines "sedonadb,duckdb" --runs 3 --mode default
# ==============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Source configuration
if [ -f "$SCRIPT_DIR/config.env" ]; then
    source "$SCRIPT_DIR/config.env"
else
    echo "[ERROR] Missing config.env in $SCRIPT_DIR"
    exit 1
fi

CUSTOM_SF=""
CUSTOM_ENGINES=""
CUSTOM_RUNS=""
CUSTOM_TIMEOUT=""
CUSTOM_MODE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf)
            CUSTOM_SF="$2"
            shift 2
            ;;
        --engines)
            CUSTOM_ENGINES="$2"
            shift 2
            ;;
        --runs)
            CUSTOM_RUNS="$2"
            shift 2
            ;;
        --timeout)
            CUSTOM_TIMEOUT="$2"
            shift 2
            ;;
        --mode)
            CUSTOM_MODE="$2"
            shift 2
            ;;
        --help|-h)
            echo "Sedona SpatialBench One-Hit Reproducer"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --sf \"1 10\"          Space-separated scale factors (default from config.env: ${SCALE_FACTORS[*]})"
            echo "  --engines list        Comma-separated engines (default: $BENCHMARK_ENGINES)"
            echo "  --runs n              Number of runs per query (default: $BENCHMARK_RUNS)"
            echo "  --timeout sec         Timeout limit in seconds (default: $QUERY_TIMEOUT, max 600s)"
            echo "  --mode mode           Execution mode: default or optimized (default: default)"
            echo "  --help, -h            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Run '$0 --help' for available options."
            exit 1
            ;;
    esac
done

if [ -n "$CUSTOM_SF" ]; then
    read -ra TARGET_SCALE_FACTORS <<< "$CUSTOM_SF"
else
    TARGET_SCALE_FACTORS=("${SCALE_FACTORS[@]}")
fi

TARGET_ENGINES="${CUSTOM_ENGINES:-${BENCHMARK_ENGINES:-$DEFAULT_ENGINES}}"
TARGET_RUNS="${CUSTOM_RUNS:-${BENCHMARK_RUNS:-${DEFAULT_RUNS:-3}}}"
TARGET_TIMEOUT="${CUSTOM_TIMEOUT:-${QUERY_TIMEOUT:-${DEFAULT_TIMEOUT:-600}}}"
TARGET_MODE="${CUSTOM_MODE:-default}"

echo "=================================================================="
echo " Sedona SpatialBench Reproduction Suite (PVLDB 2027)"
echo " Scale Factors : ${TARGET_SCALE_FACTORS[*]}"
echo " Target Engines: $TARGET_ENGINES"
echo " Runs Per Query: $TARGET_RUNS"
echo " Query Timeout : ${TARGET_TIMEOUT}s (hard 10-minute limit enforced)"
echo " Execution Mode: $TARGET_MODE"
echo " Python Exec   : $PYTHON_EXEC"
echo "=================================================================="

# ------------------------------------------------------------------------------
# Step 1: Verify Environment
# ------------------------------------------------------------------------------
echo ""
echo ">>> Step 1: Verifying Environment & Dependencies <<<"
if [ -f "$SCRIPT_DIR/setup_env.sh" ]; then
    bash "$SCRIPT_DIR/setup_env.sh" || {
        echo "[WARN] Environment check reported warnings. Continuing..."
    }
fi

# ------------------------------------------------------------------------------
# Step 2: Download / Verify Datasets
# ------------------------------------------------------------------------------
echo ""
echo ">>> Step 2: Checking / Downloading Datasets via Hugging Face <<<"
bash "$SCRIPT_DIR/download_data.sh"

# ------------------------------------------------------------------------------
# Step 3: Run Benchmarks for each Scale Factor
# ------------------------------------------------------------------------------
echo ""
echo ">>> Step 3: Running Benchmarks across Scale Factors <<<"
for SF in "${TARGET_SCALE_FACTORS[@]}"; do
    echo ""
    echo "=================================================================="
    echo " Executing SpatialBench on Scale Factor: SF${SF}"
    echo "=================================================================="
    bash "$SCRIPT_DIR/run_benchmarks.sh" \
        --sf "$SF" \
        --engines "$TARGET_ENGINES" \
        --runs "$TARGET_RUNS" \
        --timeout "$TARGET_TIMEOUT" \
        --mode "$TARGET_MODE"
done

# ------------------------------------------------------------------------------
# Step 4: Summary of Generated Artifacts
# ------------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo " [SUCCESS] Sedona SpatialBench Reproduction Completed"
echo "=================================================================="
echo "Results and figures generated:"
for SF in "${TARGET_SCALE_FACTORS[@]}"; do
    echo "  Scale Factor SF${SF}:"
    echo "    - Markdown Summary : $SCRIPT_DIR/plots/summary_sf${SF}_${TARGET_MODE}.md"
    echo "    - Execution Times  : $SCRIPT_DIR/plots/runtimes_sf${SF}_${TARGET_MODE}.pdf (.png)"
    echo "    - Category Totals  : $SCRIPT_DIR/plots/categories_sf${SF}.pdf (.png)"
done
echo ""
echo "Raw JSON data : $SCRIPT_DIR/results/"
echo "Execution logs: $SCRIPT_DIR/logs/"
echo "=================================================================="
