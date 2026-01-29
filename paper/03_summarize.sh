#!/bin/bash
set -e
source config.env

echo "=== Step 3: Summarizing Results ==="

RESULTS_DIR="results"
SUMMARY_FILE="benchmark_summary.md"

if [ ! -d "$RESULTS_DIR" ]; then
    echo "Error: $RESULTS_DIR directory not found. Run benchmarks first."
    exit 1
fi

python ../benchmark/summarize_results.py \
    --results-dir "$RESULTS_DIR" \
    --timeout "$QUERY_TIMEOUT" \
    --runs "$BENCHMARK_RUNS" \
    --output "$SUMMARY_FILE"

echo "Summary generated at: $SUMMARY_FILE"
cat "$SUMMARY_FILE"
