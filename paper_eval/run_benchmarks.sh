#!/usr/bin/env bash
# Runner script for reproducing Section 5 SpatialBench experiments
# Usage: ./run_benchmarks.sh [--sf 1|10] [--engines duckdb,sedonadb] [--queries q1,q2] [--runs 3] [--timeout 300] [--mode default|optimized]

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(cd "$SCRIPT_DIR/../benchmark" && pwd)"
source "$SCRIPT_DIR/config.env"

SF="10"
ENGINES="$DEFAULT_ENGINES"
QUERIES="$DEFAULT_QUERIES"
RUNS="$DEFAULT_RUNS"
TIMEOUT="${DEFAULT_TIMEOUT:-600}"
MAX_TIMEOUT="${MAX_TIMEOUT:-600}"
MODE="default"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf)
            SF="$2"
            shift 2
            ;;
        --engines)
            ENGINES="$2"
            shift 2
            ;;
        --queries)
            QUERIES="$2"
            shift 2
            ;;
        --runs)
            RUNS="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --mode)
            MODE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 [--sf 1|10] [--engines list] [--queries list] [--runs n] [--timeout sec] [--mode default|optimized]"
            exit 1
            ;;
    esac
done

# Enforce 10-minute hard running time limit
if [ "$TIMEOUT" -gt "$MAX_TIMEOUT" ]; then
    echo "[WARN] Requested timeout (${TIMEOUT}s) exceeds 10-minute limit (${MAX_TIMEOUT}s). Enforcing ${MAX_TIMEOUT}s."
    TIMEOUT="$MAX_TIMEOUT"
fi

# Select dataset directory based on scale factor
if [ "$SF" == "1" ]; then
    if [ -d "$SCRIPT_DIR/$DATA_DIR_SF1" ]; then
        DATA_DIR="$SCRIPT_DIR/$DATA_DIR_SF1"
    elif [ -d "$DATA_DIR_SF1" ]; then
        DATA_DIR="$DATA_DIR_SF1"
    elif [ -d "/local/storage/liang/sedona-spatialbench/paper/benchmark-data-sf1" ]; then
        DATA_DIR="/local/storage/liang/sedona-spatialbench/paper/benchmark-data-sf1"
    else
        DATA_DIR="$SCRIPT_DIR/data_sf1"
    fi
elif [ "$SF" == "10" ]; then
    if [ -d "$SCRIPT_DIR/$DATA_DIR_SF10" ]; then
        DATA_DIR="$SCRIPT_DIR/$DATA_DIR_SF10"
    elif [ -d "$DATA_DIR_SF10" ]; then
        DATA_DIR="$DATA_DIR_SF10"
    elif [ -d "/local/storage/liang/sf10" ]; then
        DATA_DIR="/local/storage/liang/sf10"
    else
        DATA_DIR="$SCRIPT_DIR/data_sf10"
    fi
else
    if [ -d "$SCRIPT_DIR/benchmark-data-sf$SF" ]; then
        DATA_DIR="$SCRIPT_DIR/benchmark-data-sf$SF"
    elif [ -d "/local/storage/liang/sf$SF" ]; then
        DATA_DIR="/local/storage/liang/sf$SF"
    else
        DATA_DIR="$SCRIPT_DIR/data_sf$SF"
    fi
fi

if [ ! -d "$DATA_DIR" ]; then
    echo "[ERROR] Data directory does not exist: $DATA_DIR"
    exit 1
fi

mkdir -p "$SCRIPT_DIR/$RESULTS_DIR"
mkdir -p "$SCRIPT_DIR/$LOGS_DIR"
mkdir -p "$SCRIPT_DIR/$PLOTS_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
echo "=========================================================="
echo " Starting SpatialBench Experiment"
echo " Scale Factor : SF$SF"
echo " Engines      : $ENGINES"
echo " Queries      : $QUERIES"
echo " Runs         : $RUNS"
echo " Timeout      : ${TIMEOUT}s"
echo " Mode         : $MODE"
echo " Data Dir     : $DATA_DIR"
echo "=========================================================="

# Configure settings according to mode
if [ "$MODE" == "optimized" ]; then
    echo "[INFO] Applying optimized settings for 24-core workstation..."
    export OMP_NUM_THREADS=24
    export POLARS_MAX_THREADS=24
    export DUCKDB_THREADS=24
    export SEDONA_MAX_THREADS=24
fi

IFS=',' read -ra ENGINE_ARRAY <<< "$ENGINES"

for engine in "${ENGINE_ARRAY[@]}"; do
    engine=$(echo "$engine" | xargs)
    echo ""
    echo ">>> Running engine: $engine on SF$SF (mode: $MODE) <<<"
    OUTPUT_FILE="$SCRIPT_DIR/$RESULTS_DIR/${engine}_sf${SF}_${MODE}.json"
    LOG_FILE="$SCRIPT_DIR/$LOGS_DIR/${engine}_sf${SF}_${MODE}_${TIMESTAMP}.log"

    echo "    Output -> $OUTPUT_FILE"
    echo "    Log    -> $LOG_FILE"

    set +e
    $PYTHON_EXEC "$BENCHMARK_DIR/run_benchmark.py" \
        --data-dir "$DATA_DIR" \
        --engines "$engine" \
        --queries "$QUERIES" \
        --runs "$RUNS" \
        --timeout "$TIMEOUT" \
        --scale-factor "$SF" \
        --output "$OUTPUT_FILE" 2>&1 | tee "$LOG_FILE"
    status=$?
    set -e

    if [ $status -eq 0 ]; then
        echo "    [SUCCESS] $engine completed successfully."
    else
        echo "    [WARN] $engine exited with code $status. Check log: $LOG_FILE"
    fi
done

echo ""
echo "=========================================================="
echo " Summarizing and Plotting Benchmark Results"
echo "=========================================================="

$PYTHON_EXEC "$SCRIPT_DIR/summarize_and_plot.py" \
    --results-dir "$SCRIPT_DIR/$RESULTS_DIR" \
    --output-dir "$SCRIPT_DIR/$PLOTS_DIR" \
    --sf "$SF" \
    --mode "$MODE"

echo ""
echo "[INFO] All tasks completed. Results and figures generated in $PLOTS_DIR"
