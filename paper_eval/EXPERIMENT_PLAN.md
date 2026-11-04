# Comprehensive Experiment Enhancement Plan for Sedona SpatialBench (VLDB 2027)

**Target Conference:** PVLDB / VLDB 2027  
**Location:** `/home/geng.161/sedona-spatialbench/paper_eval/EXPERIMENT_PLAN.md`  
**Date:** November 2026  

---

## 1. Executive Summary

Sedona SpatialBench already provides a strong empirical foundation with its 12 categorized analytical queries, single-node evaluation on Scale Factor 10 ($SF = 10$, 60M trips), in-depth diagnostic profiling of four major engine failure modes (DuckDB and PostGIS), distributed evaluation at Scale Factor 100 ($SF = 100$, 600M trips), and 1-to-16-node strong scalability testing across Apache Sedona, Trino, GeoMesa, and ClickHouse Cluster.

To elevate the manuscript into a definitive benchmark study that leaves no room for reviewer skepticism, this document details seven high-impact experimental thrusts designed to:
1. Cover missing architectural paradigms (e.g. distributed relational engines via Citus + PostGIS).
2. Deepen scalability rigor (Weak Scaling alongside Strong Scaling).
3. Systematically evaluate spatial-specific challenges (Spatial Data Skew, Polygon Vertex Complexity, Bounding Box Selectivity).
4. Address cloud-native lakehouse trends (GeoParquet metadata pushdown, Iceberg/Delta spatial Z-ordering).
5. Expose total cost of ownership (Index build time, storage footprint, and break-even analysis).
6. Measure multi-user concurrent throughput (QPS and latency percentiles under multi-tenant load).

---

## 2. Experimental Thrust Details

### Thrust 1: Weak Scalability (Constant Workload Per Node)
* **Motivation:** Strong scaling evaluates how fast a fixed problem ($SF = 100$) finishes as nodes increase. However, VLDB reviewers frequently request **weak scaling** (scaling data proportionally with compute) to verify whether communication complexity scales linearly or introduces hidden coordination overhead.
* **Research Question:** Does per-node throughput remain constant as cluster compute and spatial data volume scale proportionally?
* **Experimental Configuration:**
  - Workload per worker: Fixed at **37.5 million records (~3.5 GB)** per 8-core worker node (equivalent to $SF = 6.25$ per worker).
  - Node & Scale Matrix:
    - **1 Node:** $SF = 6.25$ (37.5M records)
    - **2 Nodes:** $SF = 12.5$ (75M records)
    - **4 Nodes:** $SF = 25$ (150M records)
    - **8 Nodes:** $SF = 50$ (300M records)
    - **16 Nodes:** $SF = 100$ (600M records)
  - Evaluated Engines: Apache Sedona, Trino Cluster, LocationTech GeoMesa, ClickHouse Cluster.
* **Metrics:**
  - Weak Scaling Efficiency: $E_{\text{weak}} = \frac{T_1}{T_N} \times 100\%$.
  - Execution runtime curve (ideal is a flat horizontal line).
  - Communication overhead ratio ($T_{\text{shuffle}} / T_{\text{total}}$).
* **Expected Architectural Insights:**
  - Trino will maintain high weak efficiency on broadcast joins but degrade when inter-stage shuffle buffers saturate.
  - GeoMesa will exhibit the flattest weak scaling curve because independent XZ-range scans require zero cross-node communication.
  - Sedona will show near-constant runtime on selective queries, with join queries displaying logarithmic shuffle increases ($O(N \log N)$).

---

### Thrust 2: Spatial Data Skew and Partitioning Robustness
* **Motivation:** Spatial data in the real world is inherently clustered (e.g. 90% of taxi trips concentrate in Manhattan, while rural zones are sparse). Conventional hash-partitioning and naive spatial grid partitioning fail catastrophically under extreme spatial skew.
* **Research Question:** How resilient are distributed spatial partitioning algorithms (Quadtree, KDB-tree, Uniform Grid, XZ-ordering, H3) against increasing spatial clustering?
* **Experimental Configuration:**
  - Generate synthetic trip datasets ($SF = 10$, 60M points) with controlled spatial distributions:
    1. **Uniform Random:** Points distributed uniformly across the bounding envelope (Zero Skew, baseline).
    2. **Gaussian Mixture / Clustered:** 5, 20, and 50 spatial hotspots with standard deviation $\sigma \in \{0.01, 0.05, 0.1\}$ degrees.
    3. **Zipfian Density:** Power-law spatial distribution with skew parameter $\alpha \in \{0.5, 1.0, 1.5, 2.0\}$.
    4. **Empirical Real-World:** Actual NYC TLC trip pickup coordinates.
  - Evaluated Partitioners in Sedona and GeoMesa:
    - Sedona: `QUADTREE`, `KDBTREE`, `EQUALGRID`, `VORONOI`.
    - GeoMesa: `XZ3` (XZ-ordering), `Z3` (Morton order).
    - Trino / ClickHouse: Default hash-sharded baseline.
* **Metrics:**
  - Partition balance: Coefficient of variation ($CV = \sigma_{\text{partition}} / \mu_{\text{partition}}$).
  - Straggler Ratio: $\text{Max Partition Rows} / \text{Average Partition Rows}$.
  - Execution runtime on Spatial Containment Join (Q6) and Proximity Join (Q8).
* **Expected Architectural Insights:**
  - Uniform grid partitioning collapses under high skew ($CV > 3.0$), causing single-worker memory exhaustion.
  - Adaptive KDB-tree and Quadtree maintain balanced partitions ($CV < 0.25$) by dynamically subdividing dense clusters.
  - Quantifies why standard analytical data warehouses (ClickHouse, Snowflake, BigQuery) without spatial partitioners suffer extreme skew degradation.

---

### Thrust 3: Storage Format & Metadata Pushdown (GeoParquet vs. Parquet vs. Iceberg)
* **Motivation:** Modern cloud lakehouses increasingly rely on format-level metadata to prune data I/O before reading actual column vectors. OGC GeoParquet 1.1 standardizes bounding-box metadata in Parquet file footers, while Iceberg/Delta support multidimensional Z-ordering.
* **Research Question:** How much storage I/O and query latency can be eliminated by spatial bounding box pushdown at the storage layer?
* **Experimental Configuration:**
  - Test the same $SF = 100$ workload across three storage layouts:
    1. **Standard Columnar Parquet:** Binary WKB geometry columns with standard min/max scalar metadata (no spatial bounding box stats).
    2. **OGC GeoParquet 1.1:** Embedded `bbox` bounding box metadata in file and row group metadata, enabling spatial predicate pushdown.
    3. **Spatially Clustered / Z-Ordered Parquet:** Parquet files sorted/clustered by Morton/Hilbert curve before writing, maximizing row group bounding box locality.
    4. **Apache Iceberg with Spatial Z-Ordering:** Iceberg tables with manifest-level bounding box pruning.
  - Workload: Selective range queries and spatial filters (Q1, Q2, Q3, Q4).
* **Metrics:**
  - I/O Pruning Ratio: $\text{Bytes Read} / \text{Total File Size}$.
  - Row Groups Skipped: $\text{Skipped Row Groups} / \text{Total Row Groups}$.
  - Cold Scan Latency (un-cached query execution time).
* **Expected Architectural Insights:**
  - Unordered Parquet requires scanning 100% of row groups because bounding envelopes of adjacent records span the entire region.
  - Z-ordered GeoParquet prunes >85% of row groups on selective queries (Q1 and Q2), cutting scan time by $4\times$ to $8\times$.

---

### Thrust 4: Spatial Indexing Trade-Offs (Build Cost, Storage, and Amortization)
* **Motivation:** Database practitioners rarely run queries in isolation; they must justify the upfront CPU and storage cost of constructing secondary spatial indexes against the downstream query speedup.
* **Research Question:** What is the total cost of ownership (build latency, memory footprint, storage expansion) of spatial indexes across single-node and distributed engines?
* **Experimental Configuration:**
  - Benchmark spatial indexing on the 60M fact table ($SF = 10$) and 600M table ($SF = 100$):
    - **PostGIS:** GiST (R-tree), SP-GiST (Space-partitioning tree), BRIN (Block Range Index).
    - **DuckDB:** Spatial R-tree extension (`CREATE INDEX ... USING RTREE`).
    - **ClickHouse:** H3 grid index and minmax skip indexes.
    - **GeoMesa:** XZ-ordering ingest pipeline on distributed storage.
    - **Apache Sedona:** Spatial partition index construction (`buildIndex("QUADTREE")`).
* **Metrics:**
  - Index Build Time (seconds) and Ingestion Throughput (records/sec).
  - Storage Footprint: Raw Table Size vs Indexed Table Size (Storage Amplification Factor).
  - Memory Peak during index construction.
  - **Break-Even Analysis:** Number of analytical queries required to offset index build time:
    $$N_{\text{breakeven}} = \frac{T_{\text{index\_build}}}{T_{\text{unindexed\_query}} - T_{\text{indexed\_query}}}$$
* **Expected Architectural Insights:**
  - PostGIS GiST index creation requires hours on large fact tables and increases storage by 1.8x, whereas BRIN builds in minutes with only 1% storage overhead, but provides coarser pruning.
  - DuckDB in-memory R-tree provides fast index scans but does not persist across restarts, forcing per-session build costs.

---

### Thrust 5: Distributed Relational OLAP (Citus Data 14.3 + PostGIS 3.6 Cluster)
* **Motivation:** The current distributed evaluation includes lakehouses (Sedona/Spark, Trino), NoSQL/spatial indexes (GeoMesa), and columnar shards (ClickHouse). Adding **Citus Cluster** fills a critical gap: distributed shared-nothing relational PostgreSQL.
* **Research Question:** How does distributed relational query execution (coordinator-worker model with distributed GiST indexes) compare against vectorized distributed lakehouse engines?
* **Experimental Configuration:**
  - Cluster: 17 nodes (1 Citus coordinator + 16 Citus workers), matching the exact AWS hardware used for Sedona, Trino, and ClickHouse.
  - Software: Citus Data 14.3 + PostGIS 3.6 on PostgreSQL 16.
  - Table Distribution Strategy:
    - `trip` table: Distributed by hash on `t_tripkey` across 64 shards.
    - `zone` and `building` tables: Configured as **Citus Reference Tables** (automatically replicated to all 16 workers, enabling local partition joins without coordinator pull).
    - Spatial indexes: GiST indexes built locally on every shard (`trip_t_pickuploc_gist`).
  - Script already drafted: `run_citus_cluster.py` under `paper_eval/`.
* **Metrics:**
  - Query latency across Q1–Q12 at $SF = 100$.
  - Completed query count, geometric mean, and failure mode analysis.
* **Expected Architectural Insights:**
  - Citus reference tables allow Q4 and Q6 to execute with high efficiency via local GiST index scans on workers.
  - Queries requiring cross-shard spatial shuffling (Q10, Q11) will expose Citus router limitations when spatial join conditions cannot be mapped to co-located distribution keys.

---

### Thrust 6: Multi-Tenant Concurrency and Throughput (QPS Stress Test)
* **Motivation:** Real-world enterprise spatial analytics serve multiple concurrent analysts, dashboard widgets, and background pipelines simultaneously. Single-query latency ignores thread contention, lock bottlenecks, and resource starvation.
* **Research Question:** How do spatial analytical engines sustain throughput and tail latency under increasing concurrent query loads?
* **Experimental Configuration:**
  - Client concurrency levels: $C \in \{1, 2, 4, 8, 16, 32\}$ concurrent analytical clients.
  - Workload Mix: A representative synthetic stream of SpatialBench queries:
    - 50% Filtering and Range Scans (Q1, Q2, Q3)
    - 30% Geometric Computation (Q5, Q7)
    - 20% Selective and Distance Joins (Q4, Q6, Q8)
  - Duration: 10 minutes steady-state execution per concurrency level.
  - Tested Systems:
    - Single Node ($SF = 10$): DuckDB, SedonaDB, ClickHouse, PostGIS.
    - Distributed Cluster ($SF = 100$): Trino Cluster, Apache Sedona Cluster.
* **Metrics:**
  - Throughput: Queries Per Second (QPS).
  - Latency Percentiles: P50, P90, P99 latency.
  - Scalability Factor: $\text{QPS}(C) / \text{QPS}(1)$.
* **Expected Architectural Insights:**
  - Embedded vectorized engines (DuckDB) maximize single-query thread parallelism, leading to CPU cache thrashing and lock contention when multiple clients compete for morsels.
  - MPP engines (Trino) and client-server DBMSs (ClickHouse, SedonaDB) excel at concurrent scheduling through dedicated connection pools and memory group isolation.

---

### Thrust 7: Algorithmic Polygon Complexity Stress Test
* **Motivation:** In Section 5.4, we showed that DuckDB suffers a 30x slowdown on Q10 due to high-vertex administrative polygons. Generalizing this into a controlled micro-benchmark will solidify SpatialBench's reputation as a diagnostic tool.
* **Research Question:** How does query execution scale as polygon vertex complexity increases from simple bounding boxes to high-precision coastal borders?
* **Experimental Configuration:**
  - Generate synthetic and simplified zone geometries with vertex counts $V \in \{4, 16, 64, 256, 1024, 4096, 16384, 65536\}$.
  - Execute spatial containment joins against 10M trip pickup points.
  - Compare engines with scalar ray-casting (DuckDB, unindexed ClickHouse) against engines with hierarchical edge indexing (PreparedGeometry in GEOS/PostGIS, interval trees in SedonaDB).
* **Metrics:**
  - Refinement phase latency as a function of $V$.
  - Memory consumption during polygon preparation.
* **Expected Architectural Insights:**
  - Unindexed ray-casting exhibits strict linear degradation: $O(V \cdot N_{\text{candidates}})$.
  - PreparedGeometry / edge-indexed kernels scale logarithmically: $O(\log V \cdot N_{\text{candidates}})$, demonstrating the critical necessity of edge indexing in production spatial engines.

---

## 3. Prioritized Implementation Roadmap

To maximize impact within available compute time, the experiments are ranked by priority:

| Priority | Thrust | Effort | Compute Resource | Value to VLDB Paper |
| :---: | :--- | :---: | :---: | :--- |
| **P0** | **Thrust 5: Citus Cluster ($SF = 100$)** | Low (script ready) | AWS 17-node cluster | Adds the 5th major distributed engine; completes the architectural taxonomy. |
| **P0** | **Thrust 1: Weak Scalability** | Medium | AWS cluster (1-16 nodes) | Essential counterpart to Strong Scalability; standard VLDB expectation. |
| **P1** | **Thrust 2: Spatial Data Skew** | Medium | Workstation / AWS | Demonstrates benchmark realism; explains the 8-node Sedona anomaly. |
| **P1** | **Thrust 3: GeoParquet & Storage Pushdown** | Medium | DL190 / Workstation | Connects the paper to cutting-edge cloud lakehouse trends. |
| **P2** | **Thrust 4: Index Build & Storage Trade-Offs** | Low | Workstation | High practical value for industry practitioners and database administrators. |
| **P2** | **Thrust 6: Multi-Tenant Concurrency** | Medium | Workstation / AWS | Demonstrates real-world enterprise serving capability. |
| **P3** | **Thrust 7: Vertex Complexity Micro-benchmark** | Low | Workstation | Extends Section 5.4 diagnostic profiling into an empirical law. |

---

## 4. Execution Commands on DL190

### 1. Launch Citus Cluster Evaluation ($SF = 100$)
```bash
cd /home/geng.161/sedona-spatialbench/paper_eval
# Verify coordinator connection
python3 run_citus_cluster.py --check-nodes
# Run full benchmark
python3 run_citus_cluster.py --scale-factor 100 --timeout 600 --mode default
```

### 2. Run Weak Scalability Sweep
```bash
cd /home/geng.161/sedona-spatialbench/paper_eval
python3 orchestrate_weak_scalability.py \
  --engines sedona,trino,geomesa,clickhouse \
  --base-sf 6.25 \
  --nodes 1,2,4,8,16
```

### 3. Generate Skew & Index Profiling Plots
```bash
cd /home/geng.161/sedona-spatialbench/paper_eval
python3 profile_spatial_skew.py --output plots/spatial_skew_analysis.pdf
python3 benchmark_index_build.py --sf 10 --output plots/index_build_tradeoffs.pdf
```
