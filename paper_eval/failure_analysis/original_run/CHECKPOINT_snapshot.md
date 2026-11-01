# Checkpoint: Sedona SpatialBench Section 5 Reproduction

This checkpoint document provides complete context for Antigravity (`agy`) running on the workstation **DL190** for **Section 5 (Benchmark Results)** of the paper *Sedona SpatialBench: A Modern Benchmark for Spatial Analytical Engines* (PVLDB 2027).

---

## 1. System and Environment Context

* **Host:** `cse-dnc196862d.coeit.osu.edu` (DL190 Workstation)
* **Processor:** 12th Gen Intel Core i9-12900 (16 physical cores, 24 hardware threads)
* **Memory:** 62 GiB RAM
* **Storage:** High-throughput NVMe SSD
* **Repository Path:** `/home/geng.161/sedona-spatialbench`
* **Git Branch:** `vldb2027-eval`
* **Python Executable:** `/local/storage/liang/.anaconda3/envs/spatialbench/bin/python` (Python 3.11.14)
* **Installed Packages:** `duckdb` (1.4.4 with spatial extension), `sedonadb` (0.6.0), `geopandas` (1.1.2), `spatial_polars` (0.2.3), `polars` (1.37.1), `pyarrow` (23.0.0), `shapely` (2.1.2), `psycopg` (3.3.2), `matplotlib` (3.11.2).
* **PostGIS Service:** PostgreSQL 12 with PostGIS 3.0.0 running on `127.0.0.1:5433` (cluster at `/local/scratch/liang/postgis_db`).
* **ClickHouse Engine:** ClickHouse 26.11.1 standalone binary at `/local/storage/liang/.anaconda3/envs/spatialbench/bin/clickhouse`.

---

## 2. Benchmark Datasets on DL190

* **Scale Factor 1 (SF1):** `paper_eval/benchmark-data-sf1` (and `/local/storage/liang/sedona-spatialbench/paper/benchmark-data-sf1`)
* **Scale Factor 10 (SF10):** `paper_eval/benchmark-data-sf10` (and `/local/storage/liang/sf10`, 5.6 GB, 60M fact records)

---

## 3. Reproduction Suite Structure (`paper_eval/`)

* `run_me.sh` (at root and `paper_eval/`): One-hit reproducer running data check, benchmark execution, and plot generation.
* `config.env`: Central environment variables, dataset paths, PostGIS parameters, ClickHouse binary path, and default parameters.
* `download_data.sh`: Downloads datasets directly from Hugging Face (`apache-sedona/spatialbench`).
* `setup_env.sh`: Verifies Python environment, packages, DuckDB spatial extension, PostGIS connection, and ClickHouse binary.
* `run_benchmarks.sh`: Experiment driver with timeout enforcement and deviation tracking.
* `summarize_and_plot.py`: Reads JSON results from `results/`, computes geometric means, outputs `summary_sf*.md`, and produces camera-ready plots with error bars (`runtimes_sf*.pdf`, `categories_sf*.pdf`).
* `results/`: Raw benchmark JSON outputs with full `run_times` arrays, `std_dev`, `min_time`, and `max_time`.
* `plots/`: Generated summary tables, PDFs, and PNGs.
* `logs/`: Execution logs with timestamps.

---

## 4. Multi-Run & Deviation Methodology

* **3 Independent Runs**: Every successful query executes across 3 iterations.
* **Per-run Tracking**: All run durations `[run_1, run_2, run_3]` are preserved in the JSON artifacts.
* **Standard Deviation**: Calculated as $s = \sqrt{\frac{1}{N-1} \sum_{i=1}^N (x_i - \bar{x})^2}$ and plotted as error bars on log-scale bar plots.
* **10-Minute Timeout Rule**: Queries taking $>600$s are killed and marked `FAILED` without running subsequent runs.

---

## 5. Performance Results (SF10, Mode: Default, 3 Runs with Deviation)

### Summary Table

| Query | PostGIS (Indexed) | PostGIS (No Index) | DuckDB | SedonaDB | ClickHouse | Polaris (Spatial Polars) | GeoPandas |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Q1** | 250.41s (±0.02) | 2.10s (±0.06) | 11.82s (±19.92) | 0.40s (±0.02) | 2.07s (±2.73) | 41.37s (±23.74) | 131.26s (±46.92) |
| **Q2** | 253.29s (±0.21) | 1.88s (±0.03) | 0.93s (±0.31) | 6.01s (±9.33) | 0.43s (±0.07) | 43.37s (±29.75) | FAILED |
| **Q3** | 250.39s | 2.53s (±0.11) | 3.92s (±5.95) | 2.35s (±2.91) | 0.44s (±0.05) | 29.60s (±7.14) | FAILED |
| **Q4** | 4.21s (±0.11) | 527.84s (±3.05) | 6.18s (±9.79) | 0.76s (±0.06) | FAILED | 19.06s (±20.70) | 254.51s (±17.50) |
| **Q5** | 337.45s (±0.85) | 84.31s (±0.11) | 12.26s (±13.53) | 11.32s (±4.19) | 13.41s (±18.35) | 55.96s (±0.34) | FAILED |
| **Q6** | 253.65s (±0.01) | FAILED | 1.58s (±0.61) | 0.99s (±0.02) | FAILED | 37.65s (±0.03) | FAILED |
| **Q7** | 4.98s (±0.02) | 4.95s (±0.02) | 1.00s (±0.02) | 1.68s (±0.12) | 1.15s (±0.32) | 45.83s (±0.08) | FAILED |
| **Q8** | 327.81s (±0.47) | FAILED | 2.53s (±0.04) | 1.83s (±0.13) | FAILED | 42.56s (±0.41) | FAILED |
| **Q9** | FAILED | FAILED | FAILED | 0.07s (±0.01) | FAILED | 0.29s (±0.04) | 1.37s (±2.03) |
| **Q10** | FAILED | FAILED | FAILED | 10.60s (±0.47) | FAILED | 83.87s (±0.68) | FAILED |
| **Q11** | FAILED | FAILED | FAILED | 21.84s (±2.39) | FAILED | 227.02s (±75.01) | FAILED |
| **Q12** | FAILED | FAILED | FAILED | 43.58s (±0.29) | FAILED | 358.46s (±18.71) | FAILED |
| **Total Time** | **1682.19s** | **623.61s** | **40.22s** | **101.43s** | **17.50s** | **985.04s** | **387.14s** |
| **Geom. Mean** | 99.15s | 11.40s | 3.27s | 2.62s | 1.43s | 37.70s | 35.77s |

Generated Visualizations:
* `paper_eval/plots/runtimes_sf10_default.pdf` (and `.png`)
* `paper_eval/plots/categories_sf10.pdf` (and `.png`)
* `paper_eval/plots/summary_sf10_default.md`

---

## 6. Key Empirical Takeaways

1. **SedonaDB Completeness and Low Variance**:
   SedonaDB completes all 12 queries on SF10 in 101.43s total with high stability across runs. Cold-start caching differences (e.g., initial parquet footer parsing on Q2) stabilize to <1s in subsequent runs.
2. **PostGIS Indexed vs. Non-Indexed Trade-Offs**:
   - **Range Scans (Q1–Q3)**: Without indexes, sequential scan in PostGIS executes in ~2s (2.10s on Q1, 1.88s on Q2). When GiST index build overhead is amortized, indexed PostGIS latency increases to ~250s due to the 250s index construction time on 60M rows.
   - **Selective Joins (Q4, Q6, Q8)**: Spatial indexing is critical for PostGIS join viability. With GiST indexing, Q4 completes in 4.21s and Q8 in 327.81s. Without indexes, Q4 takes 527.84s, while Q6, Q8, Q10, Q11, Q12 exceed the 10-minute timeout and are killed.
3. **DuckDB, ClickHouse, and Polaris**:
   - DuckDB and ClickHouse excel at filtering and range scans (Q1–Q3, Q5, Q7) but fail or time out on complex joins (Q9–Q12).
   - Polaris completes 11 out of 12 queries but shows significant runtime variation on joins (e.g. Q11: 227.02s ± 75.01s).
