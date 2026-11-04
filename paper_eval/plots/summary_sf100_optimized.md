# SpatialBench Performance Summary (SF100, Mode: Optimized)

| Query | Apache Sedona (Cluster) | Trino (Cluster) | GeoMesa (Cluster) | ClickHouse (Cluster) |
|:---|:---:|:---:|:---:|:---:|
| Q1 | 5.71s (±2.55) | 5.18s | 12.93s | 13.50s |
| Q2 | FAILED | 83.15s | TIMEOUT | 11.81s |
| Q3 | 6.18s (±0.39) | 5.81s | 42.16s | 18.90s |
| Q4 | 24.14s (±2.10) | 21.75s | TIMEOUT | TIMEOUT |
| Q5 | 28.01s (±2.24) | 9.84s | 8.65s | 327.28s |
| Q6 | 250.55s (±13.96) | 79.35s | TIMEOUT | TIMEOUT |
| Q7 | 8.76s (±0.59) | 6.43s | 13.06s | 35.97s |
| Q8 | FAILED | 95.80s | TIMEOUT | TIMEOUT |
| Q9 | FAILED | 5.80s | TIMEOUT | TIMEOUT |
| Q10 | FAILED | FAILED | TIMEOUT | FAILED |
| Q11 | FAILED | FAILED | TIMEOUT | TIMEOUT |
| Q12 | FAILED | FAILED | 47.99s | FAILED |
| **Total Time** | **323.35s** | **313.10s** | **124.79s** | **407.45s** |
| **Geom. Mean** | 19.34s | 17.47s | 19.68s | 32.35s |
