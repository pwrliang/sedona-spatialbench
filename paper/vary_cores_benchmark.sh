#!/bin/bash
set -e
source config.env

engine="sedonadb_gpu"
SCALE_FACTOR=1

# Get the total number of processing units available
MAX_CORES=$(nproc)

for (( CPU_CORES=1; CPU_CORES<=MAX_CORES; CPU_CORES++ )); do
  echo "--------------------------------------------------"
  echo "Preparing benchmark run with $CPU_CORES CPU core(s)"
  echo "--------------------------------------------------"

  RESULTS_DIR="results_SF_${SCALE_FACTOR}_CPU_LIMIT_${CPU_CORES}"
  mkdir -p "$RESULTS_DIR"
  log_file="${RESULTS_DIR}/${engine}_results.json"

  if [[ -f "$log_file" ]]; then
    echo "${log_file} exists, skipping to next core count."
  else
    echo "Running benchmark for $engine..., SF = ${SCALE_FACTOR}, CPU_CORES = ${CPU_CORES}"

    # Calculate the zero-indexed core range for taskset (e.g., if CPU_CORES=4, range is 0-3)
    CORE_RANGE="0-$((CPU_CORES-1))"

    # Prepend taskset to the python execution
    taskset -c "$CORE_RANGE" python ../benchmark/run_benchmark.py \
        --data-dir "benchmark-data-sf${SCALE_FACTOR}" \
        --engines "$engine" \
        --timeout "$QUERY_TIMEOUT" \
        --runs "$BENCHMARK_RUNS" \
        --queries "q2,q11" \
        --scale-factor "$SCALE_FACTOR" \
        --output "$log_file"

    echo "Finished $engine for $CPU_CORES cores"
  fi
done

echo "All requested benchmarks completed."
