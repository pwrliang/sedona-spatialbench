#!/bin/bash
set -e
source config.env


# --- Helper Functions for Installation ---

install_duckdb() {
    echo "--- Installing DuckDB ---"
    if [ "$DUCKDB_NIGHTLY" = "true" ]; then
        echo "Installing DuckDB Nightly..."
        pip install "duckdb<1.5.0" --pre pyarrow pandas
    elif [ -n "$DUCKDB_VERSION" ]; then
        echo "Installing DuckDB v$DUCKDB_VERSION..."
        pip install "duckdb==$DUCKDB_VERSION" pyarrow pandas
    else
        echo "Installing DuckDB Latest..."
        pip install duckdb pyarrow pandas
    fi

    # Install spatial extension
    python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL spatial'); print('DuckDB spatial extension installed')"
}

install_geopandas() {
    echo "--- Installing GeoPandas ---"
    if [ -n "$GEOPANDAS_VERSION" ]; then
        pip install "geopandas==$GEOPANDAS_VERSION" pandas pyarrow shapely
    else
        pip install geopandas pandas pyarrow shapely
    fi
}

install_sedona() {
    echo "--- Installing SedonaDB ---"
    if [ "$SEDONADB_NIGHTLY" = "true" ]; then
        echo "Installing SedonaDB Nightly from Gemfury..."
        pip install "sedonadb[geopandas]" pandas pyarrow pyproj \
            --pre \
            --index-url https://repo.fury.io/sedona-nightlies/ \
            --extra-index-url https://pypi.org/simple/
    elif [ -n "$SEDONADB_VERSION" ]; then
        pip install "sedonadb[geopandas]==$SEDONADB_VERSION" pandas pyarrow pyproj
    else
        pip install "sedonadb[geopandas]" pandas pyarrow pyproj
    fi
}

install_spatial_polars() {
    echo "--- Installing Spatial Polars ---"
    if [ -n "$SPATIAL_POLARS_VERSION" ]; then
        pip install "spatial-polars[knn]==$SPATIAL_POLARS_VERSION" pyarrow
    else
        pip install "spatial-polars[knn]" pyarrow
    fi
}

install_apache_sedona() {
    echo "--- Installing Apache Sedona (Spark) ---"
    # Installs PySpark and the Sedona bindings
    if [ -n "$APACHE_SEDONA_VERSION" ]; then
        pip install "apache-sedona==$APACHE_SEDONA_VERSION" "pyspark==3.5.4"
    else
        pip install apache-sedona "pyspark==3.5.4"
    fi
}

install_pgstrom() {
    echo "--- Installing PG-Strom Client ---"
    # PG-Strom requires a running Postgres server with the extension compiled/loaded.
    # This function only installs the Python adapter needed for the benchmark script.
    pip install pyarrow "psycopg[binary]"
}

# --- Main Execution Loop ---

IFS=',' read -ra ENGINES <<< "$BENCHMARK_ENGINES"

function install() {
  for engine in "${ENGINES[@]}"; do
      # Trim whitespace
      engine=$(echo "$engine" | xargs)

      echo "=========================================="
      echo "Installing Engine: $engine"
      echo "=========================================="

      # 1. Install Dependencies
      case $engine in
          duckdb)
              install_duckdb
              ;;
          geopandas)
              install_geopandas
              ;;
          sedonadb)
              install_sedona
              ;;
          sedonadb_gpu)
              ;;
          spatial_polars)
              install_spatial_polars
              ;;
          apache_sedona)
              install_apache_sedona
              ;;
          pgstrom)
              install_pgstrom
              ;;
          *)
              echo "Unknown engine: $engine"
              continue
              ;;
      esac
  done
}

for engine in "${ENGINES[@]}"; do
    # Trim whitespace
    engine=$(echo "$engine" | xargs)

    echo "=========================================="
    echo "Processing Engine: $engine"
    echo "=========================================="

    # 2. Run Benchmark
    # Note: Added ../benchmark/ prefix to run_benchmark.py assuming script is run from a root dir
    # or separate scripts dir, matching the previous path logic in your snippet.
    # Adjust path if your structure is flat.
    # N.B. Running PGStrom will modify downloaded datasets, making other baselines cannot run with them
    for SCALE_FACTOR in "${SCALE_FACTORS[@]}"; do
      RESULTS_DIR="results_SF_${SCALE_FACTOR}"
      mkdir -p "$RESULTS_DIR"
      log_file="${RESULTS_DIR}/${engine}_results.json"

      if [[ -f "$log_file" ]]; then
        echo "${log_file} exists, skip"
      else
        echo "Running benchmark for $engine..., SF = ${SCALE_FACTOR}"
        python ../benchmark/run_benchmark.py \
            --data-dir "benchmark-data-sf${SCALE_FACTOR}" \
            --engines "$engine" \
            --timeout "$QUERY_TIMEOUT" \
            --runs "$BENCHMARK_RUNS" \
            --queries "q2,q4,q6,q9,q10,q11" \
            --scale-factor "$SCALE_FACTOR" \
            --output "${RESULTS_DIR}/${engine}_results.json"
        echo "Finished $engine"
      fi
    done
done

echo "All requested benchmarks completed."
