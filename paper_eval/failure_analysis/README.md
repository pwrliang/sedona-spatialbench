# SpatialBench Failure & Timeout Investigation (VLDB 2027)

This directory contains the complete reproduction suite, diagnostic scripts, physical execution plans, raw execution logs, and technical report investigating why modern spatial database systems failed or timed out on the **SpatialBench SF10 (Scale Factor 10, 60 Million Rows)** benchmark evaluation.

---

## Directory Structure

```
paper_eval/failure_analysis/
├── README.md                      # Navigation guide and reproduction instructions
├── REPORT_VLDB.md                 # Full technical report formatted for VLDB submission
├── reproduce_all.sh               # Master one-click reproduction script
├── plot_failures.py               # Generates camera-ready PNG & PDF figures
├── plots/                         # Generated publication figures
│   ├── duckdb_q9_optimization.pdf / .png
│   ├── postgis_q12_knn.pdf / .png
│   ├── duckdb_q10_vertex_complexity.pdf / .png
│   └── spatialbench_summary_matrix.pdf / .png
├── env/                           # Machine environment, versions, and data checksums
│   ├── environment.txt            # OS, kernel, CPU, RAM specifications
│   ├── pip_freeze.txt             # Exact Python package dependencies
│   ├── data_manifest.txt          # SF10 parquet files, sizes, and row counts
│   └── git_state.txt              # Exact git commit and diff at evaluation time
├── original_run/                  # Untouched bitwise snapshot of the original SF10 benchmark run
│   ├── results/                   # JSON benchmark outputs
│   ├── logs/                      # Raw engine stdout/stderr logs
│   ├── plots/                     # Original benchmark plots
│   └── SHA256SUMS                 # SHA256 checksums of all 87 original artifacts
├── 01_dataset/                    # Dataset characterization scripts and geometry statistics
│   ├── dataset_stats.py           # Table row counts, vertex distributions, validity checks
│   └── dataset_stats_sf10.json    # JSON report of SF10 dataset geometry characteristics
├── 02_clickhouse/                 # ClickHouse diagnostics, plans, and failure replays
│   ├── gen_sql.py                 # Generates benchmark and diagnostic ClickHouse SQL
│   ├── run_clickhouse.sh          # Replay script for errors (Q4, Q10, Q12) and plans
│   ├── plans/                     # EXPLAIN PLAN actions=1 and EXPLAIN PIPELINE for Q1-Q12
│   ├── logs/                      # Stderr logs for OOM (exit code 241) and syntax error (47)
│   └── results/                   # Scaled execution records (zones & trips scaling sweeps)
├── 03_duckdb/                     # DuckDB diagnostics, physical plans, and rewrite proofs
│   ├── duckdb_diag.py             # Plan extractor and micro-benchmark runner
│   ├── run_duckdb.sh              # Plan generation runner
│   ├── plans/                     # Physical execution plans for Q1-Q12
│   ├── sql/                       # Diagnostic rewrites (isolated joins, polygon splits)
│   └── results/                   # Profiling results verifying >2800x speedup on Q9
├── 04_postgis/                    # PostGIS diagnostics, indexed vs noindex plans, and KNN fix
│   ├── pg_diag.py                 # Plan extractor and query analyzer
│   ├── run_postgis.sh             # Plan generation and diagnostic execution runner
│   ├── load_hf_building.py        # Validated Hugging Face building table loader
│   ├── plans/                     # EXPLAIN (ANALYZE, BUFFERS) for indexed and unindexed modes
│   ├── sql/                       # Diagnostic SQL (KNN operator `<->` vs `ST_Distance`)
│   └── results/                   # KNN scaling results (22x to 53x speedup on Q12)
├── 05_geopandas/                  # GeoPandas memory profiling scripts
│   └── test_geopandas_mem.py      # Object heap footprint and OOM isolation
├── 07_sedonadb/                   # SedonaDB execution plans and spatial optimizer analysis
│   ├── sedonadb_diag.py           # Plan extractor using SedonaDB Python context
│   └── plans/                     # DataFusion logical and physical plans showing SpatialJoinExec
└── common/                        # Common utilities and environment settings
    ├── env.sh                     # Path configurations and database credentials
    └── diag_run.py                # Process runner with timeout, memory tracking, and logging
```

---

## Quick Start / Reproduction

To reproduce all fast diagnostic experiments, plan extractions, and publication figures in one command:

```bash
cd paper_eval/failure_analysis
bash reproduce_all.sh
```

To run individual engine diagnostics:

### 1. ClickHouse Diagnostics
```bash
# Replay Q4/Q10 OOM and Q12 syntax bug, and dump physical execution pipelines
bash 02_clickhouse/run_clickhouse.sh B
bash 02_clickhouse/run_clickhouse.sh A
```

### 2. DuckDB Diagnostics & Optimizer Verification
```bash
# Extract physical execution plans
bash 03_duckdb/run_duckdb.sh A

# Verify Q9 >2800x speedup by isolating spatial join from inequality join
/local/storage/liang/.anaconda3/envs/spatialbench/bin/python 03_duckdb/duckdb_diag.py run \
    /paper_eval/benchmark-data-sf10 03_duckdb/results q9 \
    --sql 03_duckdb/sql/q9_rewrite_where.sql \
    --set disabled_optimizers=filter_pushdown,cte_filter_pusher

# Profile Q10 polygon complexity (416k simple zones vs 38k county zones)
/local/storage/liang/.anaconda3/envs/spatialbench/bin/python 03_duckdb/duckdb_diag.py run \
    /paper_eval/benchmark-data-sf10 03_duckdb/results q10 \
    --sql 03_duckdb/sql/q10_no_county.sql --limit-table trip --limit 1000000

/local/storage/liang/.anaconda3/envs/spatialbench/bin/python 03_duckdb/duckdb_diag.py run \
    /paper_eval/benchmark-data-sf10 03_duckdb/results q10 \
    --sql 03_duckdb/sql/q10_county_only.sql --limit-table trip --limit 1000000
```

### 3. PostGIS Diagnostics & KNN Index Verification
```bash
# Extract indexed and unindexed execution plans
bash 04_postgis/run_postgis.sh A
bash 04_postgis/run_postgis.sh B

# Compare original Q12 ORDER BY ST_Distance (Seq Scan) vs GiST KNN operator `<->`
/local/storage/liang/.anaconda3/envs/spatialbench/bin/python 04_postgis/pg_diag.py analyze \
    04_postgis/plans q12_orig_trip1000 --sql 04_postgis/sql/q12_orig_trip1000.sql

/local/storage/liang/.anaconda3/envs/spatialbench/bin/python 04_postgis/pg_diag.py analyze \
    04_postgis/plans q12_knn_trip1000 --sql 04_postgis/sql/q12_knn_trip1000.sql
```

### 4. Regenerate Paper Figures
```bash
/local/storage/liang/.anaconda3/envs/spatialbench/bin/python plot_failures.py
```

---

## Technical Report
For the complete in-depth analysis, root cause breakdowns, and engine optimization recommendations, see [REPORT_VLDB.md](REPORT_VLDB.md).
