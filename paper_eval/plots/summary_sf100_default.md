# SpatialBench Performance Summary (SF100, Mode: Default)

| Query | Apache Sedona (Cluster) | Trino (Cluster) | GeoMesa (Cluster) | ClickHouse (Cluster) |
|:---|:---:|:---:|:---:|:---:|
| Q1 | 5.40s (±2.17) | 4.44s | 14.10s | 15.24s |
| Q2 | FAILED | 79.51s | TIMEOUT | 13.31s |
| Q3 | 5.93s (±0.38) | 4.99s | 41.22s | 20.06s |
| Q4 | 26.47s (±0.66) | 22.09s | TIMEOUT | TIMEOUT |
| Q5 | FAILED | 9.53s | 12.42s | 352.87s |
| Q6 | 217.45s (±54.22) | 81.39s | TIMEOUT | TIMEOUT |
| Q7 | 8.51s (±0.46) | 6.24s | 9.92s | 21.76s |
| Q8 | FAILED | 91.97s | TIMEOUT | TIMEOUT |
| Q9 | 7.93s (±5.63) | 5.43s | TIMEOUT | TIMEOUT |
| Q10 | FAILED | FAILED | TIMEOUT | FAILED |
| Q11 | FAILED | FAILED | TIMEOUT | TIMEOUT |
| Q12 | FAILED | FAILED | 52.89s | FAILED |
| **Total Time** | **271.69s** | **305.59s** | **130.55s** | **423.25s** |
| **Geom. Mean** | 15.22s | 16.57s | 20.68s | 31.55s |
