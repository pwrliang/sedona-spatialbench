# 📊 SpatialBench Benchmark Results

| Parameter | Value |
|-----------|-------|
| **Scale Factor** | 1.0 |
| **Query Timeout** | 3600s |
| **Runs per Query** | 3 |
| **Timestamp** | 2026-01-30T03:21:18.756808+00:00 |
| **Queries** | 12 |

## 🔧 Software Versions

| Engine | Version |
|--------|---------|
| Apache_Sedona | `1.8.1` |
| 🦆 DuckDB | `1.4.4` |
| 🐼 GeoPandas | `1.1.2` |
| Pgstrom | `6.1.0` |
| 🌵 SedonaDB | `0.3.0a81` |
| 🐻‍❄️ Spatial Polars | `0.2.3` |

## 🏁 Results Comparison

| Query | Apache_Sedona | 🦆 DuckDB | 🐼 GeoPandas | Pgstrom | 🌵 SedonaDB | 🐻‍❄️ Spatial Polars |
|:------|:---:|:---:|:---:|:---:|:---:|:---:|
| **Q1** | 3.09s | **0.17s** | 12.87s | 0.18s | 0.50s | 3.25s |
| **Q2** | 113.18s | **0.34s** | 13.66s | 0.80s | 0.66s | 3.12s |
| **Q3** | 2.85s | 0.22s | 13.52s | **0.18s** | 0.57s | 3.12s |
| **Q4** | 6.68s | **0.59s** | 15.61s | 35.78s | 0.62s | 3.82s |
| **Q5** | 14.23s | **1.22s** | 44.17s | 13.12s | 2.38s | 7.94s |
| **Q6** | 9.25s | **0.77s** | 17.21s | 5.70s | 0.83s | 7.86s |
| **Q7** | 26.74s | 7.74s | 125.83s | 13.80s | **2.25s** | 6.57s |
| **Q8** | 3.40s | **0.76s** | 14.68s | 673.23s | 0.79s | 5.96s |
| **Q9** | 2.04s | 54.42s | 0.09s | 46.40s | 0.26s | **0.08s** |
| **Q10** | ⏱️ TIMEOUT | 204.45s | 35.73s | ⏱️ TIMEOUT | **5.31s** | 16.27s |
| **Q11** | ⏱️ TIMEOUT | ⏱️ TIMEOUT | 43.17s | ⏱️ TIMEOUT | **7.80s** | 27.81s |
| **Q12** | ❌ ERROR | ❌ ERROR | ⏱️ TIMEOUT | ⏱️ TIMEOUT | **28.21s** | 51.61s |

## 🥇 Performance Summary

| Engine | Wins |
|--------|:----:|
| 🦆 DuckDB | 6 |
| 🌵 SedonaDB | 4 |
| Pgstrom | 1 |
| 🐻‍❄️ Spatial Polars | 1 |
| Apache_Sedona | 0 |
| 🐼 GeoPandas | 0 |

## 📋 Detailed Results

<details>
<summary><b>Apache_Sedona</b> - Click to expand</summary>

| Query | Time | Status | Rows |
|:------|-----:|:------:|-----:|
| Q1 | 3.09s | ✅ | 94 |
| Q2 | 113.18s | ✅ | 1 |
| Q3 | 2.85s | ✅ | 22 |
| Q4 | 6.68s | ✅ | 258 |
| Q5 | 14.23s | ✅ | 316,691 |
| Q6 | 9.25s | ✅ | 3 |
| Q7 | 26.74s | ✅ | 6,000,000 |
| Q8 | 3.40s | ✅ | 369 |
| Q9 | 2.04s | ✅ | 37 |
| Q10 | 3600.00s | ⏱️ | — |
| Q11 | 3600.00s | ⏱️ | — |
| Q12 | N/A | ❌ | — |

</details>

<details>
<summary><b>🦆 DuckDB</b> - Click to expand</summary>

| Query | Time | Status | Rows |
|:------|-----:|:------:|-----:|
| Q1 | 0.17s | ✅ | 94 |
| Q2 | 0.34s | ✅ | 1 |
| Q3 | 0.22s | ✅ | 22 |
| Q4 | 0.59s | ✅ | 258 |
| Q5 | 1.22s | ✅ | 316,691 |
| Q6 | 0.77s | ✅ | 3 |
| Q7 | 7.74s | ✅ | 6,000,000 |
| Q8 | 0.76s | ✅ | 369 |
| Q9 | 54.42s | ✅ | 37 |
| Q10 | 204.45s | ✅ | 156,093 |
| Q11 | 3600.00s | ⏱️ | — |
| Q12 | N/A | ❌ | — |

</details>

<details>
<summary><b>🐼 GeoPandas</b> - Click to expand</summary>

| Query | Time | Status | Rows |
|:------|-----:|:------:|-----:|
| Q1 | 12.87s | ✅ | 94 |
| Q2 | 13.66s | ✅ | 1 |
| Q3 | 13.52s | ✅ | 22 |
| Q4 | 15.61s | ✅ | 258 |
| Q5 | 44.17s | ✅ | 316,691 |
| Q6 | 17.21s | ✅ | 3 |
| Q7 | 125.83s | ✅ | 6,000,000 |
| Q8 | 14.68s | ✅ | 369 |
| Q9 | 0.09s | ✅ | 37 |
| Q10 | 35.73s | ✅ | 156,095 |
| Q11 | 43.17s | ✅ | 1 |
| Q12 | 3600.00s | ⏱️ | — |

</details>

<details>
<summary><b>Pgstrom</b> - Click to expand</summary>

| Query | Time | Status | Rows |
|:------|-----:|:------:|-----:|
| Q1 | 0.18s | ✅ | 94 |
| Q2 | 0.80s | ✅ | 1 |
| Q3 | 0.18s | ✅ | 22 |
| Q4 | 35.78s | ✅ | 258 |
| Q5 | 13.12s | ✅ | 316,691 |
| Q6 | 5.70s | ✅ | 3 |
| Q7 | 13.80s | ✅ | 6,000,000 |
| Q8 | 673.23s | ✅ | 369 |
| Q9 | 46.40s | ✅ | 37 |
| Q10 | 3600.00s | ⏱️ | — |
| Q11 | 3600.00s | ⏱️ | — |
| Q12 | 3600.00s | ⏱️ | — |

</details>

<details>
<summary><b>🌵 SedonaDB</b> - Click to expand</summary>

| Query | Time | Status | Rows |
|:------|-----:|:------:|-----:|
| Q1 | 0.50s | ✅ | 94 |
| Q2 | 0.66s | ✅ | 1 |
| Q3 | 0.57s | ✅ | 22 |
| Q4 | 0.62s | ✅ | 258 |
| Q5 | 2.38s | ✅ | 316,691 |
| Q6 | 0.83s | ✅ | 3 |
| Q7 | 2.25s | ✅ | 6,000,000 |
| Q8 | 0.79s | ✅ | 369 |
| Q9 | 0.26s | ✅ | 37 |
| Q10 | 5.31s | ✅ | 156,095 |
| Q11 | 7.80s | ✅ | 1 |
| Q12 | 28.21s | ✅ | 30,000,000 |

</details>

<details>
<summary><b>🐻‍❄️ Spatial Polars</b> - Click to expand</summary>

| Query | Time | Status | Rows |
|:------|-----:|:------:|-----:|
| Q1 | 3.25s | ✅ | 94 |
| Q2 | 3.12s | ✅ | 1 |
| Q3 | 3.12s | ✅ | 22 |
| Q4 | 3.82s | ✅ | 258 |
| Q5 | 7.94s | ✅ | 316,691 |
| Q6 | 7.86s | ✅ | 3 |
| Q7 | 6.57s | ✅ | 6,000,000 |
| Q8 | 5.96s | ✅ | 369 |
| Q9 | 0.08s | ✅ | 37 |
| Q10 | 16.27s | ✅ | 156,095 |
| Q11 | 27.81s | ✅ | 1 |
| Q12 | 51.61s | ✅ | 30,000,000 |

</details>

## ⚠️ Errors and Timeouts

### Apache_Sedona

- **Q10**: `Query q10 timed out after 3600 seconds (process killed)`
- **Q11**: `Query q11 timed out after 3600 seconds (process killed)`
- **Q12**: `An error occurred while calling o69.collectToPython.
: org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 18 tasks (1071.4 MiB) is bigger than spark...`

### 🦆 DuckDB

- **Q11**: `Query q11 timed out after 3600 seconds (process killed)`
- **Q12**: `Out of Memory Error: failed to offload data block of size 256.0 KiB (115.1 GiB/115.1 GiB used).
This limit was set by the 'max_temp_directory_size' setting.
By default, this setting utilizes the avail...`

### 🐼 GeoPandas

- **Q12**: `Query q12 timed out after 3600 seconds (process killed)`

### Pgstrom

- **Q10**: `Query q10 timed out after 3600 seconds (process killed)`
- **Q11**: `Query q11 timed out after 3600 seconds (process killed)`
- **Q12**: `Query q12 timed out after 3600 seconds (process killed)`

---

| Legend | Meaning |
|--------|---------|
| **bold** | Fastest for this query |
| ⏱️ TIMEOUT | Query exceeded timeout |
| ❌ ERROR | Query failed |

*Generated by [SpatialBench](https://github.com/apache/sedona-spatialbench) on 2026-01-30 13:12:44 UTC*