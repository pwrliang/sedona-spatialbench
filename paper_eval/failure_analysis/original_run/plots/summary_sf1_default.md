# SpatialBench Performance Summary (SF1, Mode: Default)

| Query | ClickHouse | DuckDB | GeoPandas | PostGIS | SedonaDB | Polaris (Spatial Polars) |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|
| Q1 | 0.18s | 0.32s | 119.63s | 0.01s | 6.45s | 29.56s |
| Q2 | N/A | 0.69s | 213.05s | N/A | 6.18s | 27.88s |
| Q3 | N/A | 0.43s | FAILED | N/A | 2.45s | 28.10s |
| Q4 | N/A | 0.56s | 128.16s | N/A | 10.52s | 18.28s |
| Q5 | N/A | 4.33s | FAILED | N/A | 13.80s | 79.03s |
| Q6 | N/A | 1.13s | FAILED | N/A | 4.08s | 37.48s |
| Q7 | N/A | 0.97s | FAILED | N/A | 5.30s | 51.19s |
| Q8 | N/A | 2.23s | FAILED | N/A | 4.71s | 81.33s |
| Q9 | N/A | FAILED | 1.30s | N/A | 0.13s | 0.49s |
| Q10 | N/A | FAILED | FAILED | N/A | 16.21s | 277.25s |
| Q11 | N/A | FAILED | FAILED | N/A | 19.74s | FAILED |
| Q12 | N/A | FAILED | FAILED | N/A | 44.04s | 357.00s |
| **Total Time** | **0.18s** | **10.66s** | **462.14s** | **0.01s** | **133.61s** | **987.59s** |
| **Geom. Mean** | 0.18s | 0.93s | 45.39s | 0.01s | 6.10s | 38.21s |
