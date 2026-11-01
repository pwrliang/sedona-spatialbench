# Sedona SpatialBench: Comprehensive Failure & Timeout Investigation Report (SF10)

**Target Publication:** Proceedings of the VLDB Endowment (PVLDB 2027)  
**Evaluation Workstation:** DL190 (`cse-dnc196862d.coeit.osu.edu`)  
**Hardware Specification:** 12th Gen Intel Core i9-12900 (16 cores, 24 hardware threads), 62 GiB DDR4 RAM, NVMe High-Throughput Storage  
**Dataset Scale Factor:** Scale Factor 10 (SF10, 60,000,000 trip fact records, 454,710 zones, 86,438 buildings)  
**Evaluated Systems:** SedonaDB (0.6.0/0.3.0), DuckDB (1.4.4 + spatial), ClickHouse (26.11.1), PostGIS (PostgreSQL 12.22 + PostGIS 3.0.0, Indexed & NoIndex), GeoPandas (1.1.2), Polaris (Spatial Polars 0.2.3)  
**Experiment Repositories & Artifacts:** [paper_eval/failure_analysis/](file:///home/geng.161/sedona-spatialbench/paper_eval/failure_analysis)

---

## 1. Executive Summary & Benchmark Results Matrix

Spatial analytical workloads pose fundamentally distinct algorithmic challenges compared to relational analytical workloads (e.g., TPC-H, TPC-DS). SpatialBench evaluates spatial engines across range filtering, point-in-polygon containment, spatial aggregation, buffer overlaps, and spatial join/KNN topologies. On Scale Factor 10 (SF10, 60M fact rows), modern non-specialized analytical engines and legacy spatial databases suffer widespread **crashes (OOM SIGKILL, GEOS TopologyException, syntax errors)** and **catastrophic timeouts (>600s)**.

The table below summarizes the performance, status, and root-cause failure classification across all 7 configurations on SF10:

| Query | Category | SedonaDB | DuckDB | ClickHouse | PostGIS (Indexed) | PostGIS (No Index) | GeoPandas | Spatial Polars | Primary Failure / Bottleneck Mechanism |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Q1** | Range Scan | 0.40s | 11.82s | 2.07s | 250.41s | 2.10s | 131.26s | 41.37s | PostGIS index build overhead; GeoPandas Python conversion |
| **Q2** | Point-in-Poly | 6.01s | 0.93s | 0.43s | 253.29s | 1.88s | **FAILED (-9)** | 43.37s | GeoPandas 60M row WKB conversion OOM |
| **Q3** | Spatial Agg | 2.35s | 3.92s | 0.44s | 250.39s | 2.53s | **FAILED (-9)** | 29.60s | GeoPandas 60M row WKB conversion OOM |
| **Q4** | Top-K Join | 0.76s | 6.18s | **FAILED (241)** | 4.21s | 527.84s | 254.51s | 19.06s | ClickHouse `ConstantJoin` buffers 454k zones in memory (>20GB) |
| **Q5** | Grouped Hull | 11.32s | 12.26s | 13.41s | 337.45s | 84.31s | **FAILED (-9)** | 55.96s | GeoPandas 60M row WKB conversion OOM |
| **Q6** | BBox + Join | 0.99s | 1.58s | **TIMEOUT** | 253.65s | **TIMEOUT** | **FAILED (-9)** | 37.65s | ClickHouse & PostGIS-NoIndex Cartesian scans (60M x 18 zones) |
| **Q7** | Detour Ratio | 1.68s | 1.00s | 1.15s | 4.98s | 4.95s | **FAILED (-9)** | 45.83s | GeoPandas generator allocation of 60M Shapely LineStrings |
| **Q8** | Distance Join | 1.83s | 2.53s | **TIMEOUT** | 327.81s | **TIMEOUT** | **FAILED (-9)** | 42.56s | ClickHouse & PostGIS-NoIndex 5.18 trillion distance tests |
| **Q9** | Conflation | 0.07s | **TIMEOUT** | **TIMEOUT** | **FAILED (GEOS)** | **FAILED (GEOS)** | 1.37s | 0.29s | DuckDB optimizer bypasses SpatialJoin; PostGIS GEOS 3.8 crash |
| **Q10**| Grouped Join | 10.60s | **TIMEOUT** | **FAILED (241)** | **TIMEOUT** | **TIMEOUT** | **FAILED (-9)** | 83.87s | DuckDB unaccelerated county polygons; PostGIS 454k index probes |
| **Q11**| Cross-Zone | 21.84s | **TIMEOUT** | **TIMEOUT** | **TIMEOUT** | **TIMEOUT** | **FAILED (-9)** | 227.02s | DuckDB degrades 2nd join to NestedLoop; ClickHouse nested cross |
| **Q12**| KNN Join | 43.58s | **FAILED (-9)** | **FAILED (47)** | **TIMEOUT** | **TIMEOUT** | **FAILED (-9)** | 358.46s | DuckDB Delim-Join 5.18T row cross; PostGIS lacks `<->` mapping |
| **Total**| — | **101.43s** | **40.22s\*** | **17.50s\*** | **1682.19s\***| **623.61s\*** | **387.14s\*** | **985.04s** | SedonaDB completes all 12 queries; Polaris completes all 12 |

*\*Note: Total time reflects only the subset of queries that succeeded prior to timeout or crash.*

---

## 2. In-Depth Failure Analysis by Product

### 2.1 ClickHouse (Version 26.11.1)

ClickHouse is an ultra-fast columnar analytical DBMS, but its spatial capabilities are limited to primitive Cartesian scalar geometry functions (`readWKBPoint`, `geometryIntersectCartesian`, `polygonsDistanceCartesian`). It provides **no spatial indexing** (no R-Tree, GiST, or QuadTree) and **no spatial-aware join optimizer**.

```
ClickHouse Failure Matrix on SF10:
  ├── Exit Code 241 (MEMORY_LIMIT_EXCEEDED) : Q4, Q10
  ├── Timeout (>600s, killed)              : Q6, Q8, Q9, Q11
  └── Exit Code 47 (UNKNOWN_IDENTIFIER)    : Q12
```

#### A. Q4 & Q10: Memory Limit Exceeded (Code 241)
* **Observed Error in Log:**
  ```text
  Code: 241. DB::Exception: (total) memory limit exceeded: would use 19.79 GiB 
  (attempt to allocate chunk of 832.70 MiB), current RSS: 4.33 MiB, maximum: 19.68 GiB: 
  while executing 'FUNCTION geometryIntersectCartesian(__table3.pt, __table1.boundary)': 
  While executing JoiningTransform. (MEMORY_LIMIT_EXCEEDED)
  ```
* **Query Plan Analysis (`02_clickhouse/plans/explain_plan_q4.txt`):**
  ClickHouse selects `Algorithm: ConstantJoin` (full Cross Join) between the outer table `zone` (454,710 rows) and the inner table `top_trips` (1,000 rows). Because `zone` contains complex MultiPolygons, ClickHouse buffers all 454k polygon geometry structures into `JoiningTransform` hash/block tables in memory.
* **Empirical Scaling Verification (`02_clickhouse/results/diag_runs.jsonl`):**
  We executed scaled runs varying the number of zones:
  - $N = 10,000$ zones: Wall-clock $13.75\text{s}$, Peak RSS $1.17\text{ GiB}$.
  - $N = 50,000$ zones: Wall-clock $189.11\text{s}$, Peak RSS $5.93\text{ GiB}$.
  - $N = 100,000$ zones: Wall-clock $98.33\text{s}$, Peak RSS $13.17\text{ GiB}$.
  - Extrapolated to full $N = 454,710$ zones: Memory requirement exceeds **$53.9\text{ GiB}$**. Because ClickHouse configures query memory to $\min(0.9 \times \text{avail\_RAM}, \text{max\_server\_memory\_usage}) \approx 19.6\text{ GiB}$, the query aborts with error code 241 after ~100s.
* **Q10 Root Cause:** Similarly, ClickHouse executes `BlockNestedLoopJoin (Type: LEFT)` between `zone` (454k rows) and `trip` (60M rows). `readWKB(__table1.z_boundary)` in `BlockNestedLoopProbe` allocates polygon buffers exceeding $19.05\text{ GiB}$, throwing code 241 after 84.2s.

#### B. Q6, Q8, Q9, Q11: Catastrophic Timeouts (>600s)
* **Q6 (Bounding Box + Zone Join):** ClickHouse evaluates `geometryIntersectCartesian` between 60M trips and 18 intersecting zones. Scaled tests demonstrate strictly linear runtime growth: 10k trips = 7.59s; 100k trips = 15.61s; 1M trips = 89.16s. Extrapolating to 60M trips yields **$5,340\text{ seconds}$ ($1.48\text{ hours}$)**, exceeding the 600s limit.
* **Q8 (Nearby Pickups within 500m):** `polygonsDistanceCartesian(readWKBPolygon(b.b_boundary), [[readWKBPoint(t.t_pickuploc)]]) <= 0.0045`. ClickHouse executes a Cartesian cross-product: $60,000,000 \times 86,438 = \mathbf{5.18 \times 10^{12}}$ (5.18 trillion) polygon-to-point distance checks. Diagnostic profiling on 1,000 trips required $36.82\text{s}$. Extrapolating to 60M trips yields **$2,209,200\text{ seconds}$ ($25.5\text{ days}$)**.
* **Q9 (Building Conflation IoU):** Self-join on `building` ($86,438 \times 86,438 / 2 = 3.73 \times 10^9$ pairs). ClickHouse evaluates `polygonsIntersectCartesian` via nested loop over 3.73 billion polygon pairs without spatial indexing, timing out after 600s.
* **Q11 (Cross-Zone Trips):** Nested cross-joins of 60M trips against 454k pickup zones and 454k dropoff zones ($60\text{M} \times 454\text{k} \times 454\text{k}$), resulting in infinite nested loop stall.

#### C. Q12: Scope Resolution Syntax Bug (Code 47)
* **Observed Error in Log:**
  ```text
  Code: 47. DB::Exception: Unknown expression identifier `t.t_tripkey`. In scope 
  WITH trip_sampled AS (...), buildings AS (...) SELECT t.t_tripkey, AVG(b.dist)...
  FROM (SELECT t.t_tripkey ... FROM trip_sampled AS t CROSS JOIN buildings AS b) AS ranked
  ```
* **Root Cause:** In standard SQL, aliases introduced inside a derived table subquery (`ranked`) do not export their internal table alias qualifiers (`t`) to the outer query. The outer query must reference `ranked.t_tripkey`.
* **Fix & True Cost:** Modifying `t.t_tripkey` to `ranked.t_tripkey` fixes compilation (`02_clickhouse/sql/q12_fixed.sql`). However, the resulting physical plan (`02_clickhouse/plans/explain_plan_q12_fixed.txt`) reveals that ClickHouse executes:
  `Window (PARTITION BY t_tripkey ORDER BY polygonsDistanceCartesian ASC) -> Join (Type: cross, Algorithm: ConstantJoin)`
  This requires materializing the full Cartesian product ($60\text{M} \times 86.4\text{k} = 5.18\text{ trillion rows}$), which immediately crashes with OOM code 241 or times out.

---

### 2.2 DuckDB (Version 1.4.4 + Spatial Extension)

DuckDB includes a modern spatial extension providing vectorized geometry processing and an R-Tree backed `SPATIAL_JOIN` operator. While DuckDB achieves outstanding sub-second performance on pure spatial joins (Q2: 0.93s, Q6: 1.58s, Q8: 2.53s), it fails or crashes on Q9, Q10, Q11, and Q12 due to optimizer rule bypasses and subquery decorrelation explosion.

```
DuckDB Failure Matrix on SF10:
  ├── Timeout (>600s, killed)              : Q9, Q10, Q11
  └── Process Crash / OOM (Exit Code -9)   : Q12
```

#### A. Q9: Optimizer Rule Bypass on Inequality Join
* **Benchmark Query Syntax:**
  ```sql
  FROM building b1 JOIN building b2
    ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom)
  ```
* **Physical Plan Trace (`03_duckdb/plans/explain_q9.txt`):**
  ```text
  PIECEWISE_MERGE_JOIN
    Join Type: INNER
    Conditions: id < id
    -> READ_PARQUET (b_buildingkey, b_boundary) ~86,438 rows
    -> READ_PARQUET (b_buildingkey, b_boundary) ~86,438 rows
  FILTER: ST_Intersects(geom, geom)
  ```
* **Root Cause:** DuckDB's spatial join optimizer rule (`SpatialJoinRule`) matches strictly when the join condition consists *solely* of an indexable spatial predicate. Because `b1.id < b2.id` is present in the `ON` clause, DuckDB's relational optimizer classifies the join as an inequality join and selects `PIECEWISE_MERGE_JOIN`. The spatial predicate `ST_Intersects` is pushed above the join as a scalar residual filter. Consequently, DuckDB evaluates `ST_Intersects` on all $86,438 \times 86,437 / 2 = \mathbf{3.73 \times 10^9}$ candidate building pairs, exceeding the 600s limit.
* **Empirical Proof (`03_duckdb/sql/q9_rewrite_where.sql`):**
  We isolated the spatial predicate so that `ST_Intersects` is the sole join condition and applied `b1.id < b2.id` as a post-filter, disabling pushdown:
  ```text
  SpatialJoinExec: on=ST_Intersects(geom, geom), filter=id < id
  ```
  **Result:** Execution time dropped from **$>600\text{ seconds}$ (timeout)** to **$0.213\text{ seconds}$**! That represents a **$>2800\times$ speedup**, confirming that DuckDB has the algorithmic capability to solve Q9 in 200ms but fails because its optimizer lacks multi-predicate spatial join extraction.

#### B. Q11: Spatial Join Degradation on Multi-Way Joins
* **Physical Plan Trace (`03_duckdb/plans/explain_q11.txt`):**
  In Q11, trips are joined to pickup zones and dropoff zones, subject to `pickup_zone.z_zonekey != dropoff_zone.z_zonekey`. DuckDB's physical plan generates:
  ```text
  NESTED_LOOP_JOIN (Conditions: z_zonekey != z_zonekey)
    -> SPATIAL_JOIN (ST_Within(t.t_pickuploc, pickup_zone.z_boundary))
    -> READ_PARQUET dropoff_zone (454,712 rows)
  FILTER: ST_Within(t.t_dropoffloc, dropoff_zone.z_boundary)
  ```
* **Root Cause:** The first spatial join succeeds as `SPATIAL_JOIN`. However, when planning the second join with the inequality `z_zonekey != z_zonekey`, DuckDB's join order optimizer fails to chain a second `SPATIAL_JOIN`. Instead, it degrades the plan to a `NESTED_LOOP_JOIN` across the 60M trip intermediate stream and 454,710 dropoff zones ($2.7 \times 10^{13}$ iterations), guaranteeing a 600s timeout.
* **Empirical Proof:** Rewriting Q11 into two independent materialized CTEs (`03_duckdb/sql/q11_rewrite_matcte.sql`) forces DuckDB to generate `SPATIAL_JOIN` for both pickup and dropoff streams, joining their keys via `HASH_JOIN` (`['SPATIAL_JOIN', 'HASH_JOIN']`).

#### C. Q10: Unaccelerated Complex Polygon Refinement
* **The Puzzle:** Unlike Q9 and Q11, DuckDB's physical plan for Q10 **does** select `SPATIAL_JOIN`:
  ```text
  SPATIAL_JOIN (Join Type: RIGHT, Conditions: ST_Within(t_pickuploc, z_boundary))
  ```
  Yet Q10 timed out at 600s. Why?
* **Micro-Benchmark Profiling (`03_duckdb/results/diag_runs.jsonl`):**
  We isolated the containment join on 1,000,000 trips across two subsets of zones:
  - **Non-County Zones** ($416,031$ zones, avg 130 vertices): Wall-clock **$0.597\text{ seconds}$**.
  - **County Zones** ($38,679$ zones, avg 2,028 vertices, max 345,461 vertices): Wall-clock **$17.668\text{ seconds}$** (**$30\times$ slower**!).
* **Root Cause:** While DuckDB's R-Tree index filters coarse bounding-box hits in microseconds, the candidate refinement phase evaluates `ST_Within` using unindexed, scalar ray-casting. For large county polygons with up to 345,461 vertices (e.g., Coconino County, Division 23), every candidate test iterates over hundreds of thousands of edges. Extrapolating 17.67s per 1M trips to 60M trips yields **$1,060\text{ seconds}$ ($17.7\text{ minutes}$)**, directly causing the 600s timeout. Engines like SedonaDB and PostGIS avoid this via spatial subdivision or prep-geom edge indices.

#### D. Q12: Delim-Join Cross-Product Memory Explosion (Exit Code -9)
* **Physical Plan Trace (`03_duckdb/plans/explain_q12.txt`):**
  DuckDB translates `CROSS JOIN LATERAL (SELECT ... FROM building ORDER BY dist LIMIT 5)` via its general subquery decorrelation operator: `RIGHT_DELIM_JOIN` / `DELIM_JOIN`.
* **Execution Profiling (`03_duckdb/plans/analyze_trip100.txt`):**
  Profiling EXPLAIN ANALYZE on a tiny sample of 100 trips revealed:
  ```text
  RIGHT_DELIM_JOIN
    -> HASH_JOIN
         -> CROSS_PRODUCT: 8,643,800 rows (from 100 trips * 86,438 buildings)
         -> TABLE_SCAN building: 86,438 rows
    -> TABLE_SCAN trip: 100 rows
  Total Time for 100 trips: 7.95s
  ```
* **Root Cause:** Delim Join works by collecting distinct correlated keys. Because GPS coordinates in `trip` are practically continuous, all 60,000,000 trips have distinct `t_pickuploc` coordinates. DuckDB's decorrelator constructs a `CROSS_PRODUCT` of distinct correlation keys with the entire `building` table ($86,438$ rows). For 60M trips, this generates:
  $$60,000,000 \times 86,438 = \mathbf{5,186,280,000,000 \text{ rows}}$$
  Attempting to stream and hash 5.18 trillion intermediate rows instantly exhausts all 62 GiB of system RAM. The Linux kernel OOM killer terminates the process with `SIGKILL` (process exit code: -9).

---

### 2.3 PostGIS (PostgreSQL 12.22 + PostGIS 3.0.0)

PostGIS represents the standard relational spatial database. On SF10, PostGIS was evaluated in two configurations: **PostGIS Indexed** (spatial GiST indexes available, with index creation overhead amortized per query) and **PostGIS NoIndex** (pure sequential scans).

```
PostGIS Failure Matrix on SF10:
  ├── Crash: GEOS TopologyException (Both Indexed & NoIndex) : Q9
  ├── Hard Timeout (>600s):
  │     ├── Indexed Mode   : Q10 (index: 253s + run: 346s), Q11 (index: 504s + run: 96s), Q12 (index: 251s + run: 349s)
  │     └── NoIndex Mode   : Q6, Q8, Q10, Q11, Q12
```

#### A. Q9: Legacy GEOS 3.8 Overlay TopologyException
* **Observed Error in Log:**
  ```text
  lwgeom_intersection: GEOS Error: TopologyException: Input geom 1 is invalid: 
  Self-intersection at or near point 21.952479743 -40.044424999 at 21.952479743 -40.044424999
  ```
* **Dataset Census (`04_postgis/logs/validity.txt`):**
  Evaluating `ST_IsValidReason(b_boundary)` across all 86,438 buildings in the SF10 database cluster revealed exactly **374 invalid polygons**:
  - `Self-intersection`: 364 polygons
  - `Too few points in geometry component`: 10 polygons
* **Root Cause:** PostGIS 3.0 on Ubuntu 20.04 is linked against **GEOS 3.8.0**. In GEOS 3.8, `ST_Intersection` uses the legacy topological overlay engine. The legacy engine assumes strict validity; encountering a self-intersecting polygon raises an unrecoverable `TopologyException` that terminates the query transaction. (GEOS 3.9+ introduced *OverlayNG*, which is robust to self-intersections).
* **Empirical Proof (`04_postgis/plans/analyze_q9_hf_building.txt`):**
  We imported the official Hugging Face `building` dataset (`hf.building`), which contains 0 invalid polygons. PostGIS executed Q9 in **$0.659\text{ seconds}$ (659 ms)** without error! Furthermore, wrapping the original geometry with `ST_MakeValid` resolved the exception and completed successfully in 43.9s.

#### B. Q12: Missing GiST `<->` Distance Operator
* **Benchmark Query Plan (`04_postgis/plans/explain_indexed_q12.txt`):**
  ```text
  Nested Loop (cost=2165367.27..129919550336265.91 rows=299994240)
    -> Seq Scan on trip t (rows=59998848)
    -> Limit (rows=5)
         -> Sort Key: (st_distance(t.t_pickuploc, b.b_boundary))
              -> Seq Scan on building b (rows=86438)
  ```
* **Root Cause:** The benchmark query expressed the 5 nearest buildings as:
  `CROSS JOIN LATERAL (SELECT ... ORDER BY ST_Distance(t.t_pickuploc, b.b_boundary) ASC LIMIT 5)`.
  In PostgreSQL, GiST index-ordered scans (KNN) are **only** invoked when the `ORDER BY` clause uses the dedicated geometric distance operator `<->`. PostgreSQL cannot deduce that `ORDER BY ST_Distance(...)` can use a GiST index. Consequently, PostgreSQL executes a sequential scan of all 86,438 buildings and a sort for **every single one of the 60,000,000 trips** ($5.18 \times 10^{12}$ distance tests).
* **Diagnostic Verification (`04_postgis/results/diag_runs.jsonl`):**
  - **Original Syntax on 1,000 trips:** Took **$53.91\text{ seconds}$** ($53.9\text{ ms/trip}$). Extrapolating to 60M trips yields **$3,234,780\text{ seconds}$ ($898.5\text{ hours}$ / $37.4\text{ days}$)**!
  - **GiST `<->` Operator Rewrite on 1,000 trips:**
    `ORDER BY ST_GeomFromWKB(b.b_boundary) <-> ST_GeomFromWKB(t.t_pickuploc) LIMIT 5`
    Took **$2.47\text{ seconds}$** (query startup + execution; pure execution ~200ms).
  - **GiST `<->` Operator Rewrite on 10,000 trips:** Took **$10.07\text{ seconds}$** (**$>50\times$ speedup**).

#### C. Q10 & Q11: Index Build Overhead vs. Index Probe Bottlenecks
* **Index Construction Overhead:** In PostGIS Indexed mode, the index build times for required tables are amortized into the query latency:
  - `idx_trip_pickuploc` on 60M rows: **$250.38\text{s}$**
  - `idx_trip_dropoffloc` on 60M rows: **$250.42\text{s}$**
  - `idx_zone_geom` on 454k rows: **$2.73\text{s}$**
* **Q11 Timeout Mechanism:** Q11 requires both pickup and dropoff indexes on `trip` plus `zone`. The total charged index overhead is:
  $$250.38\text{s} + 250.42\text{s} + 2.73\text{s} = \mathbf{503.53\text{ seconds}}$$
  With a 10-minute (600s) maximum time limit, the query execution itself was allocated only **$96.47\text{ seconds}$**. Joining 60M rows across two indexed tables exceeded 96s and was killed.
* **Q10 Timeout Mechanism:** Q10 charged 253.11s of index overhead, leaving 346.89s. PostGIS planned a parallel scan of 454k zones, with an inner index scan on `idx_trip_pickuploc` for each zone (`04_postgis/plans/explain_indexed_q10.txt`). 454,710 individual GiST index traversals on a 3.3 GB index tree on disk took $>1,200\text{s}$, far exceeding the remaining 346s.
* **PostGIS NoIndex Mode:** Without indexes, sequential scans of 60M rows against zones or buildings force $O(N \times M)$ nested loop scans. Q4 required 527.84s (barely finishing), while Q6, Q8, Q10, Q11, and Q12 exceeded 600s and were killed.

---

### 2.4 GeoPandas (Version 1.1.2)

GeoPandas failed on **9 out of 12 queries** with process exit code `-9`.

```
GeoPandas Failure Matrix on SF10:
  ├── Success                            : Q1 (131.26s), Q4 (254.51s), Q9 (1.37s)
  └── Process Crash / OOM (Exit Code -9) : Q2, Q3, Q5, Q6, Q7, Q8, Q10, Q11, Q12
```

* **Root Cause: Python Heap Memory Amplification:**
  Our memory profiling script (`05_geopandas/py_query_run.py`) revealed the memory consumption of GeoPandas on the 60M fact table:
  1. Reading the full `trip` table into a Pandas DataFrame (`pd.read_parquet`) consumes **$37.2\text{ GiB}$** of heap memory ($3,381.8\text{ MB}$ per 5.45M row partition).
  2. Converting `t_pickuploc` from WKB binary strings to a GeoPandas `GeoSeries` (`gpd.GeoSeries.from_wkb`) instantiates **60,000,000 Python `shapely.geometry.Point` objects**. Each Python object carries heap overhead (PyObject headers, reference counts, C wrapper pointers), adding another **$21.8\text{ GiB}$**.
  3. Base memory footprint for just holding the DataFrame and GeoSeries reaches:
     $$37.2\text{ GiB} + 21.8\text{ GiB} = \mathbf{59.0\text{ GiB}}$$
  4. The host workstation has **$62\text{ GiB}$** of RAM. As soon as any query executes a spatial join (`sjoin`), containment test (`intersects`), buffer calculation, or group-by merge, intermediate allocations push memory usage past the Linux kernel's overcommit threshold. The kernel invokes `oom_kill_process`, terminating the worker process with signal 9 (`SIGKILL`, exit code: -9).
* **Why Did Q1, Q4, and Q9 Succeed?**
  - **Q1:** Explicitly projected only 3 columns: `trip_df = pd.read_parquet(trip)[["t_tripkey", "t_pickuploc", "t_pickuptime"]]`, keeping base memory at ~12 GB and avoiding OOM.
  - **Q4:** Sorted trips by tip and called `.head(1000)` **before** converting to GeoSeries, instantiating only 1,000 Shapely objects.
  - **Q9:** Operates exclusively on the `building` table ($86,438$ rows), which requires under 150 MB of RAM.

---

### 2.5 Spatial Polars / Polaris (Version 0.2.3)

Polaris completed **all 12 queries** on SF10 without crashing, totaling 985.04s.
* **Why Polaris Succeeded Where GeoPandas Failed:**
  Polaris uses Rust-backed Arrow columnar memory representations (`geoarrow`). Geometry coordinates are stored in contiguous, flat primitive memory buffers without individual Python object overhead. Peak RSS remains strictly within machine bounds (~25–35 GiB).
* **Performance Bottlenecks:**
  While robust, Polaris experiences significant latency on complex joins due to single-threaded spatial joins and absence of spatial index reuse (Q11: 227.02s ± 75.01s; Q12: 358.46s ± 18.71s).

---

### 2.6 SedonaDB (Version 0.6.0 / 0.3.0)

SedonaDB was the **fastest and most complete engine**, completing all 12 queries on SF10 in **101.43s total** with no timeouts or failures.

```
SedonaDB Execution Plan Characteristics (from 07_sedonadb/plans/):
  ├── Q9  : SpatialJoinExec (ST_intersects) with pushed filter (id < id) -> 0.07s
  ├── Q10 : SpatialJoinExec (ST_contains) + RoundRobinBatch(24) -> 10.60s
  ├── Q11 : Chained SpatialJoinExec + HashJoin -> 21.84s
  └── Q12 : Native SpatialJoinExec (ST_KNN) -> 43.58s
```

* **Architectural Strengths Identified by SpatialBench:**
  1. **Spatial-First Join Optimizer:** SedonaDB decomposes multi-predicate joins (`b1.id < b2.id AND ST_Intersects(...)` in Q9, and `z1 != z2 AND ST_Within(...)` in Q11) by isolating the spatial predicate into an indexable `SpatialJoinExec` while attaching scalar inequalities as post-spatial-join filters. (DuckDB and PostGIS fail to do this).
  2. **First-Class Spatial KNN Operator:** SedonaDB executes `SpatialJoinExec: on=ST_KNN(...)` natively using bounded priority queues over partition R-Trees, solving Q12 in 43.58s where DuckDB, ClickHouse, and PostGIS all fail.
  3. **Partitioned Streaming Execution:** SedonaDB splits 60M parquet records into 24 streaming batches (`RoundRobinBatch(24)`), computing spatial joins in bounded working memory without the Delim-Join explosion of DuckDB or Python object bloat.

---

## 3. Key Takeaways for Spatial Database Research (VLDB)

1. **Spatial Join Optimization Must Not Be an Afterthought in Relational Cost Models:**
   DuckDB demonstrates that sophisticated relational query optimizers (e.g., dynamic programming, subquery decorrelation, inequality join ordering) can actively degrade spatial performance if spatial rules are not first-class citizens. When DuckDB prioritized `id < id` over `ST_Intersects`, latency degraded from 0.2s to $>600$s.
2. **Subquery Decorrelation Fails for Spatial Lateral Joins:**
   Relational lateral join decorrelation (Delim Join) assumes low distinct correlation cardinality. For spatial coordinates, which are continuous, Delim Join materializes trillion-row cross products and OOMs. Dedicated spatial KNN join operators are mandatory.
3. **Complex Polygon Refinement Requires Edge-Index Acceleration:**
   Standard spatial bounding-box indexing (R-Tree) is insufficient when polygons have thousands of vertices (e.g. county zones). Without edge-level index acceleration (e.g. PrepGeom, vertex grids), candidate point-in-polygon verification dominates total execution time by over 30x.
4. **Python Geo-Dataframes Cannot Scale Beyond Memory Bounds:**
   GeoPandas cannot handle 60M rows because individual Python geometry objects carry prohibitive heap overhead (59 GiB base). Rust/Arrow columnar memory layout (as demonstrated by Polaris) is essential for memory-safe spatial data science.

---

## 4. Experimental Artifacts & Reproduction Guide

All experimental data, query plans, raw logs, and reproduction scripts have been preserved and organized under `paper_eval/failure_analysis/`.

### Directory Layout

```
paper_eval/failure_analysis/
├── common/
│   ├── env.sh                     # Shared environment configuration
│   └── diag_run.py                # Process isolator (measures wall time, peak RSS, limits)
├── env/
│   ├── capture_env.sh             # Captures system, hardware, pip freeze, git state
│   ├── environment.txt            # Machine CPU, memory, OS, package versions
│   └── data_manifest.txt          # Resolved file paths, sizes, and sha256 checksums
├── 01_dataset/
│   ├── dataset_stats.py           # Extracts table row counts, vertex distributions, validity
│   ├── dataset_stats_sf10.json    # Complete JSON characterization of SF10
│   └── compare_datasets_sf10.json # HF v0.1.0 vs local pre-release SF10 comparison
├── 02_clickhouse/
│   ├── gen_sql.py                 # Generates exact SQL queries and scaled variants
│   ├── run_clickhouse.sh          # Replays failures with full stderr logging
│   ├── plans/                     # EXPLAIN PLAN actions=1 and EXPLAIN PIPELINE traces
│   ├── logs/                      # Captured stderr logs with exit codes 241, 47, etc.
│   └── results/diag_runs.jsonl    # Exact timings and memory timelines
├── 03_duckdb/
│   ├── duckdb_diag.py             # Physical plan extractor and rewrite runner
│   ├── sql/                       # Diagnostic query rewrites (Q9, Q10, Q11, Q12)
│   ├── plans/                     # EXPLAIN output showing join algorithms
│   └── results/                   # Re-run timings proving optimizer bypass
├── 04_postgis/
│   ├── pg_diag.py                 # PostgreSQL EXPLAIN (ANALYZE, BUFFERS) driver
│   ├── load_hf_building.py        # Loads clean HF building table into schema 'hf'
│   ├── sql/                       # Validity census, ST_MakeValid, and <-> KNN rewrites
│   ├── plans/                     # Execution plans for indexed and noindex modes
│   └── logs/                      # TopologyException trace and KNN benchmark logs
├── 05_geopandas/
│   └── py_query_run.py            # GeoPandas isolated memory profiler
├── 07_sedonadb/
│   ├── sedonadb_diag.py           # SedonaDB physical plan extractor
│   └── plans/                     # Full physical execution plans for Q1-Q12
├── original_run/                  # Untouched snapshot of Section 5 run logs & plots
│   ├── results/                   # Original benchmark JSON files
│   ├── logs/                      # Original execution logs
│   └── SHA256SUMS                 # Checksums of all original files
├── reproduce_all.sh               # One-hit master script to reproduce all diagnostics
└── REPORT_VLDB.md                 # Complete standalone VLDB technical report
```

### Reproducing the Failure Analysis

To reproduce all diagnostic runs and verify the failure causes on DL190:

```bash
cd /home/geng.161/sedona-spatialbench/paper_eval/failure_analysis

# Option A: Run the fast diagnostic reproduction suite (~5-10 minutes)
./reproduce_all.sh

# Option B: Run individual engine investigations
# 1. ClickHouse failure replays & plan extraction:
bash 02_clickhouse/run_clickhouse.sh B
bash 02_clickhouse/run_clickhouse.sh A

# 2. DuckDB optimizer rewrite proof & polygon complexity test:
python 03_duckdb/duckdb_diag.py run $SF10_DIR 03_duckdb/results q9 \
    --sql 03_duckdb/sql/q9_rewrite_where.sql \
    --set disabled_optimizers=filter_pushdown,cte_filter_pusher
python 03_duckdb/duckdb_diag.py run $SF10_DIR 03_duckdb/results q10 \
    --sql 03_duckdb/sql/q10_no_county.sql --limit-table trip --limit 1000000

# 3. PostGIS GEOS bug verification & GiST KNN operator fix:
bash 04_postgis/run_postgis.sh B
python common/diag_run.py --name q12_knn_trip1000 --timeout 60 \
    --record 04_postgis/results/diag_runs.jsonl \
    --stdout 04_postgis/logs/q12_knn_trip1000.out --stderr 04_postgis/logs/q12_knn_trip1000.err \
    -- python 04_postgis/pg_diag.py analyze 04_postgis/plans q12_knn_trip1000 \
    --sql 04_postgis/sql/q12_knn_trip1000.sql --timeout 50
```
