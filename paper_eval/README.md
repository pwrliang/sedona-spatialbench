# Sedona SpatialBench: Section 5 Benchmark Reproduction Suite

This directory contains the automated evaluation scripts, documentation, and plotting utilities to reproduce the empirical results presented in **Section 5 (Benchmark Results)** of the paper *Sedona SpatialBench: A Modern Benchmark for Spatial Analytical Engines* (PVLDB 2027).

---

## 1. Experimental Platform Specification

* **Node / Host:** DL190 Workstation (`cse-dnc196862d.coeit.osu.edu`)
* **Processor:** 12th Gen Intel Core i9-12900 (16 physical cores, 24 hardware threads, up to 5.1 GHz)
* **Memory:** 62 GiB RAM
* **Storage:** High-throughput NVMe Solid-State Drive
* **Operating System:** Ubuntu 20.04.1 LTS (Linux Kernel 5.15)

---

## 2. Supported Single-Node Engines

| Engine | Version | Language / Core | Spatial Model |
| :--- | :---: | :--- | :--- |
| **SedonaDB** | 0.3.0 | Rust / Apache Arrow | Native columnar vectorization, Arrow buffers |
| **DuckDB** | 1.4.4 | C++ / Vectorized Morsel | Native `spatial` extension, OGC binary |
| **Polaris (Spatial Polars)** | 0.2.3 | Rust / Polars | Lock-free multi-threading, GeoArrow layouts |
| **GeoPandas** | 1.1.2 | Python / Shapely | Interpreted dataframe baseline, GEOS engine |

---

## 3. Environment Isolation & Setup

All experiments run in an isolated Conda environment ensuring reproducible dependencies without system contamination.

### Step 3.1: Activate or Verify Existing Environment
The environment is pre-configured on DL190 at `/local/storage/liang/.anaconda3/envs/spatialbench`. Run the automated verification script:

```bash
./setup_env.sh
```

### Step 3.2: Recreating the Environment from Scratch (Optional)
If setting up on a fresh machine:
```bash
conda create -n spatialbench python=3.11 -y
conda activate spatialbench
pip install duckdb==1.4.4 geopandas==1.1.2 spatial-polars==0.2.3 pyarrow shapely matplotlib pandas
# Install sedonadb Python bindings:
pip install "sedonadb[geopandas]" --pre --index-url https://repo.fury.io/sedona-nightlies/ --extra-index-url https://pypi.org/simple/
```

---

## 4. Benchmark Datasets

The evaluation uses standard Sedona SpatialBench parquet datasets:
* **Scale Factor 1 (SF1):** Located at `/local/storage/liang/sedona-spatialbench/paper/benchmark-data-sf1` (approx. 4 MB sample or full SF1).
* **Scale Factor 10 (SF10):** Located at `/local/storage/liang/sf10` (approx. 5.6 GB, 60M fact trip records).

Table schemas: `trip` (fact table), `building`, `customer`, `driver`, `vehicle`, and `zone`.

---

## 5. Running the Experiments

The unified driver script `run_benchmarks.sh` manages execution, timing, repetition, timeouts, and result logging.

### 5.1 Out-of-the-Box (Default Mode)
Runs each engine with standard factory defaults:
```bash
# Run all single-node engines on SF10 for queries Q1 through Q12:
./run_benchmarks.sh --sf 10 --engines sedonadb,duckdb,spatial_polars,geopandas --mode default

# Fast smoke test on Q1, Q2 with 1 repetition:
./run_benchmarks.sh --sf 10 --engines sedonadb,duckdb --queries q1,q2 --runs 1 --mode default
```

### 5.2 Optimized Mode
Tunes thread concurrency to saturate the 24 hardware threads and configures memory parameters according to product best practices:
```bash
./run_benchmarks.sh --sf 10 --engines sedonadb,duckdb,spatial_polars,geopandas --mode optimized
```

---

## 6. Summarizing Results & Generating Figures

The runner automatically invokes `summarize_and_plot.py` upon completion. You can also run it manually to regenerate tables and charts:

```bash
python summarize_and_plot.py \
    --results-dir ./results \
    --output-dir ./plots \
    --sf 10 \
    --mode default
```

### Generated Artifacts in `plots/`:
1. `summary_sf10_default.md`: Markdown summary table with execution times per query, total execution time, and geometric means.
2. `runtimes_sf10_default.pdf` and `.png`: Camera-ready grouped bar chart on a log-scale comparing runtimes for Q1–Q12.
3. `categories_sf10.pdf` and `.png`: Runtime breakdown across the four functional benchmark categories:
   * **Filtering & Scans (Q1–Q3)**
   * **Join Planning (Q4, Q6)**
   * **Geometric Compute (Q5, Q7, Q9)**
   * **Heavy Spatial Joins (Q8, Q10–Q12)**

---

## 7. Incorporating into the Paper

Figures generated in `plots/*.pdf` can be directly copied to the LaTeX project's `figures/` directory and referenced in Section 5 of `main.tex`:
```latex
\begin{figure}[t]
\centering
\includegraphics[width=\linewidth]{figures/runtimes_sf10_default.pdf}
\caption{Single-Node Query Runtimes on Workstation (SF10).}
\label{fig:sf10_runtimes}
\end{figure}
```
