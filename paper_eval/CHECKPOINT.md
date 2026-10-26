# Checkpoint: Sedona SpatialBench Section 5 Reproduction

This checkpoint document provides complete context for Antigravity (`agy`) running on the workstation **DL190** to continue the empirical evaluation for **Section 5 (Benchmark Results)** of the paper *Sedona SpatialBench: A Modern Benchmark for Spatial Analytical Engines* (PVLDB 2027).

---

## 1. System and Environment Context

* **Host:** `cse-dnc196862d.coeit.osu.edu` (DL190 Workstation)
* **Processor:** 12th Gen Intel Core i9-12900 (16 physical cores, 24 hardware threads)
* **Memory:** 62 GiB RAM
* **Storage:** High-throughput NVMe SSD
* **Repository Path:** `/home/geng.161/sedona-spatialbench`
* **Git Branch:** `vldb2027-eval`
* **Isolated Conda Environment:**
  * Path: `/local/storage/liang/.anaconda3/envs/spatialbench`
  * Python Executable: `/local/storage/liang/.anaconda3/envs/spatialbench/bin/python` (Python 3.11.14)
  * Installed Packages: `duckdb` (1.4.4 with spatial extension), `sedonadb` (0.6.0), `geopandas` (1.1.2), `spatial_polars` (0.2.3), `polars` (1.37.1), `pyarrow` (23.0.0), `shapely` (2.1.2), `matplotlib` (3.11.2).

---

## 2. Benchmark Datasets on DL190

All datasets are standard Sedona SpatialBench parquet tables (`trip`, `building`, `customer`, `driver`, `vehicle`, `zone`):
* **Scale Factor 1 (SF1):** `/local/storage/liang/sedona-spatialbench/paper/benchmark-data-sf1`
* **Scale Factor 10 (SF10):** `/local/storage/liang/sf10` (5.6 GB, 60M fact records)

---

## 3. Reproduction Suite Structure (`paper_eval/`)

Located at `/home/geng.161/sedona-spatialbench/paper_eval`:

* `config.env`: Central environment variables, dataset paths, and default parameters.
* `setup_env.sh`: Automated script to verify Python environment, packages, and DuckDB spatial extension.
* `run_benchmarks.sh`: Experiment driver script supporting options:
  * `--sf 1|10`
  * `--engines sedonadb,duckdb,spatial_polars,geopandas`
  * `--queries q1,q2,...,q12`
  * `--runs 3`
  * `--timeout 300`
  * `--mode default|optimized`
* `summarize_and_plot.py`: Reads JSON results from `results/`, computes geometric means, outputs `summary_sf*.md`, and produces camera-ready plots (`runtimes_sf*.pdf`, `categories_sf*.pdf`).
* `results/`: Raw benchmark JSON outputs.
* `plots/`: Generated summary tables, PDFs, and PNGs.
* `logs/`: Execution logs with timestamps.

---

## 4. Initial Smoke Test Results (SF10, Mode: Default)

Verified pipeline on Query 1 (Center Proximity Search):
* **DuckDB:** 3.11s (100 rows)
* **SedonaDB:** 7.60s (100 rows)
* **Polaris (Spatial Polars):** 30.10s (100 rows)

Generated verification plots:
* `paper_eval/plots/runtimes_sf10_default.pdf`
* `paper_eval/plots/categories_sf10.pdf`
* `paper_eval/plots/summary_sf10_default.md`

---

## 5. Next Steps for Antigravity on DL190

When starting or resuming `agy` on DL190, proceed with the following incremental experiment plan:

### Step 5.1: Run Category 1 - Filtering & Scans (Q1, Q2, Q3)
```bash
cd /home/geng.161/sedona-spatialbench/paper_eval
./run_benchmarks.sh --sf 10 --engines sedonadb,duckdb,spatial_polars,geopandas --queries q1,q2,q3 --runs 3 --mode default
```

### Step 5.2: Run Category 2 - Selective Join Planning (Q4, Q6)
```bash
./run_benchmarks.sh --sf 10 --engines sedonadb,duckdb,spatial_polars,geopandas --queries q4,q6 --runs 3 --mode default
```

### Step 5.3: Run Category 3 - Geometric Computation (Q5, Q7, Q9)
```bash
./run_benchmarks.sh --sf 10 --engines sedonadb,duckdb,spatial_polars,geopandas --queries q5,q7,q9 --runs 3 --mode default
```

### Step 5.4: Run Category 4 - Heavy Spatial Joins (Q8, Q10, Q11, Q12)
```bash
./run_benchmarks.sh --sf 10 --engines sedonadb,duckdb,spatial_polars,geopandas --queries q8,q10,q11,q12 --runs 3 --timeout 600 --mode default
```
*(Note: For GeoPandas, Q8 and Q11 on SF10 may hit memory limits or timeouts. The runner records timeouts gracefully without halting).*

### Step 5.5: Run Optimized Mode
Execute with `--mode optimized` to evaluate tuned concurrency (24 threads) and memory configurations.

### Step 5.6: Scale Factor 1 Baseline Comparison
Run the same suite on SF1 (`--sf 1`) to analyze scaling characteristics between SF1 and SF10.

### Step 5.7: Generate Final Figures and Sync to Paper
After completing runs, execute:
```bash
python summarize_and_plot.py --results-dir ./results --output-dir ./plots --sf 10 --mode default
```
Copy the resulting `.pdf` figures to the paper repository at `/Users/liang/Documents/Projects/VLDB2027-Benchmark/figures/` for insertion into Section 5.
