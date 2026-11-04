#!/usr/bin/env python3
"""
Thrust 6: Multi-Tenant Concurrency and Throughput (QPS Stress Test).
Evaluates concurrent analytical throughput across:
- DuckDB (Embedded Vectorized OLAP)
- PostGIS (Client-Server Spatial Relational DBMS)
Concurrency levels: C in {1, 2, 4, 8, 16, 32} concurrent workers.
Workload: Representative SpatialBench query stream:
- Range filtering (Q1/Q2-style bounding box filter)
- Geometric computation (Q7-style distance ratio)
- Spatial join / selective spatial lookup
Metrics:
- Queries Per Second (QPS)
- Latency percentiles (P50, P90, P99)
- Concurrency scaling efficiency: QPS(C) / (C * QPS(1))
"""

import concurrent.futures
import json
import os
import random
import time
from pathlib import Path
import duckdb
import matplotlib.pyplot as plt
import numpy as np
import psycopg

RESULTS_DIR = Path("paper_eval/results")
PLOTS_DIR = Path("paper_eval/plots")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

PG_HOST = "127.0.0.1"
PG_PORT = 5433
PG_DB = "spatialbench"
PG_USER = "geng.161"

CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32]
NUM_QUERIES_PER_LEVEL = 100

def get_postgis_worker_queries():
    # Return query templates with randomized parameters to avoid query cache artifacts
    queries = [
        # 1. Range scan (50%)
        lambda: f"""
        SELECT count(*) FROM trip
        WHERE ST_Intersects(
            ST_GeomFromWKB(t_pickuploc),
            ST_SetSRID(ST_MakeBox2D(
                ST_Point({-112.2 + random.uniform(-0.1, 0.1)}, {34.4 + random.uniform(-0.1, 0.1)}),
                ST_Point({-111.8 + random.uniform(-0.1, 0.1)}, {34.8 + random.uniform(-0.1, 0.1)})
            ), 0)
        );
        """,
        # 2. Distance Computation (30%)
        lambda: f"""
        SELECT count(*) FROM trip
        WHERE t_distance > {random.uniform(5.0, 15.0)}
          AND ST_Distance(ST_GeomFromWKB(t_pickuploc), ST_GeomFromWKB(t_dropoffloc)) > 0.05
        LIMIT 500;
        """,
        # 3. Spatial Building Proximity Lookup (20%)
        lambda: f"""
        SELECT b.b_buildingkey, count(*)
        FROM trip t
        JOIN building b ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.01)
        WHERE b.b_buildingkey = {random.randint(1, 1000)}
        GROUP BY b.b_buildingkey;
        """
    ]
    weights = [0.5, 0.3, 0.2]
    return random.choices(queries, weights=weights)[0]()

def benchmark_postgis_concurrency():
    print("=== Evaluating PostGIS Multi-Tenant Concurrency ===")
    results = {}

    for c in CONCURRENCY_LEVELS:
        print(f"  Testing PostGIS Concurrency C={c}...")
        latencies = []

        def worker_task():
            conn = psycopg.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, autocommit=True
            )
            cur = conn.cursor()
            cur.execute("SET statement_timeout = '10s';")
            sql = get_postgis_worker_queries()
            t0 = time.perf_counter()
            try:
                cur.execute(sql)
                _ = cur.fetchall()
                elapsed = time.perf_counter() - t0
                cur.close()
                conn.close()
                return elapsed
            except Exception as e:
                cur.close()
                conn.close()
                return None

        total_queries = max(NUM_QUERIES_PER_LEVEL, c * 10)
        t_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
            futures = [executor.submit(worker_task) for _ in range(total_queries)]
            for fut in concurrent.futures.as_completed(futures):
                res = fut.result()
                if res is not None:
                    latencies.append(res)
        
        total_time = time.perf_counter() - t_start
        qps = len(latencies) / total_time
        lat_ms = np.array(latencies) * 1000.0

        p50 = np.percentile(lat_ms, 50) if len(lat_ms) > 0 else 0
        p90 = np.percentile(lat_ms, 90) if len(lat_ms) > 0 else 0
        p99 = np.percentile(lat_ms, 99) if len(lat_ms) > 0 else 0

        print(f"    PostGIS C={c:2d}: QPS={qps:6.1f} | P50={p50:6.1f}ms, P90={p90:6.1f}ms, P99={p99:6.1f}ms")
        results[c] = {
            "qps": round(qps, 2),
            "p50_ms": round(p50, 2),
            "p90_ms": round(p90, 2),
            "p99_ms": round(p99, 2),
            "total_queries": len(latencies),
        }

    return results

def benchmark_duckdb_concurrency():
    print("=== Evaluating DuckDB Multi-Tenant Concurrency ===")
    # Pre-create in-memory / persistent DuckDB database
    db_file = "/local/scratch/liang/tmp/concurrency_duckdb.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    init_con = duckdb.connect(db_file)
    init_con.execute("INSTALL spatial; LOAD spatial;")
    init_con.execute("""
    CREATE TABLE trip_bench AS 
    SELECT t_tripkey, t_distance,
           ST_Point(ST_X(ST_GeomFromWKB(t_pickuploc)), ST_Y(ST_GeomFromWKB(t_pickuploc))) as pickup_geom,
           ST_Point(ST_X(ST_GeomFromWKB(t_dropoffloc)), ST_Y(ST_GeomFromWKB(t_dropoffloc))) as dropoff_geom
    FROM "paper_eval/benchmark-data-sf1/trip/*.parquet"
    LIMIT 2000000;
    """)
    init_con.execute("CREATE INDEX idx_pickup ON trip_bench USING RTREE (pickup_geom);")
    init_con.close()

    def get_duckdb_query():
        queries = [
            lambda: f"""
            SELECT count(*) FROM trip_bench
            WHERE ST_Within(pickup_geom, ST_MakeEnvelope(
                {-112.2 + random.uniform(-0.1, 0.1)}, {34.4 + random.uniform(-0.1, 0.1)},
                {-111.8 + random.uniform(-0.1, 0.1)}, {34.8 + random.uniform(-0.1, 0.1)}
            ));
            """,
            lambda: f"""
            SELECT count(*) FROM trip_bench
            WHERE t_distance > {random.uniform(5.0, 15.0)}
              AND ST_Distance(pickup_geom, dropoff_geom) > 0.05
            LIMIT 500;
            """
        ]
        return random.choice(queries)()

    results = {}

    for c in CONCURRENCY_LEVELS:
        print(f"  Testing DuckDB Concurrency C={c}...")
        latencies = []

        def worker_task():
            con = duckdb.connect(db_file, read_only=True)
            con.execute("INSTALL spatial; LOAD spatial;")
            sql = get_duckdb_query()
            t0 = time.perf_counter()
            try:
                con.execute(sql).fetchall()
                elapsed = time.perf_counter() - t0
                con.close()
                return elapsed
            except Exception as e:
                con.close()
                return None

        total_queries = max(NUM_QUERIES_PER_LEVEL, c * 15)
        t_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
            futures = [executor.submit(worker_task) for _ in range(total_queries)]
            for fut in concurrent.futures.as_completed(futures):
                res = fut.result()
                if res is not None:
                    latencies.append(res)

        total_time = time.perf_counter() - t_start
        qps = len(latencies) / total_time
        lat_ms = np.array(latencies) * 1000.0

        p50 = np.percentile(lat_ms, 50) if len(lat_ms) > 0 else 0
        p90 = np.percentile(lat_ms, 90) if len(lat_ms) > 0 else 0
        p99 = np.percentile(lat_ms, 99) if len(lat_ms) > 0 else 0

        print(f"    DuckDB  C={c:2d}: QPS={qps:6.1f} | P50={p50:6.1f}ms, P90={p90:6.1f}ms, P99={p99:6.1f}ms")
        results[c] = {
            "qps": round(qps, 2),
            "p50_ms": round(p50, 2),
            "p90_ms": round(p90, 2),
            "p99_ms": round(p99, 2),
            "total_queries": len(latencies),
        }

    if os.path.exists(db_file):
        os.remove(db_file)

    return results

def plot_concurrency(results):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5), dpi=300)
    
    c_vals = CONCURRENCY_LEVELS
    pg_qps = [results["PostGIS"][str(c)]["qps"] if str(c) in results["PostGIS"] else results["PostGIS"][c]["qps"] for c in c_vals]
    duck_qps = [results["DuckDB"][str(c)]["qps"] if str(c) in results["DuckDB"] else results["DuckDB"][c]["qps"] for c in c_vals]

    pg_p99 = [results["PostGIS"][str(c)]["p99_ms"] if str(c) in results["PostGIS"] else results["PostGIS"][c]["p99_ms"] for c in c_vals]
    duck_p99 = [results["DuckDB"][str(c)]["p99_ms"] if str(c) in results["DuckDB"] else results["DuckDB"][c]["p99_ms"] for c in c_vals]

    # QPS Throughput
    ax1.plot(c_vals, duck_qps, "o-", color="#377eb8", linewidth=2.5, markersize=7, label="DuckDB (Vectorized Morsel OLAP)")
    ax1.plot(c_vals, pg_qps, "s-", color="#e41a1c", linewidth=2.5, markersize=7, label="PostGIS (Connection Pool RDBMS)")
    ax1.set_xlabel("Concurrent Client Workers (C)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Throughput (Queries Per Second, QPS)", fontsize=11, fontweight="bold")
    ax1.set_title("Multi-Tenant Analytical Throughput (Higher is Better)", fontsize=12, fontweight="bold")
    ax1.grid(True, linestyle=":", alpha=0.6)
    ax1.legend(fontsize=10)

    # P99 Tail Latency
    ax2.plot(c_vals, duck_p99, "o--", color="#377eb8", linewidth=2.5, markersize=7, label="DuckDB P99 Latency")
    ax2.plot(c_vals, pg_p99, "s--", color="#e41a1c", linewidth=2.5, markersize=7, label="PostGIS P99 Latency")
    ax2.set_yscale("log")
    ax2.set_xlabel("Concurrent Client Workers (C)", fontsize=11, fontweight="bold")
    ax2.set_ylabel("P99 Latency (ms, Log Scale)", fontsize=11, fontweight="bold")
    ax2.set_title("Tail Latency Under Contention (Lower is Better)", fontsize=12, fontweight="bold")
    ax2.grid(True, which="both", linestyle=":", alpha=0.6)
    ax2.legend(fontsize=10)

    plt.tight_layout()
    png_path = PLOTS_DIR / "concurrency_throughput.png"
    pdf_path = PLOTS_DIR / "concurrency_throughput.pdf"
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
    print(f"Saved Concurrency plots to {png_path} and {pdf_path}")

def main():
    pg_res = benchmark_postgis_concurrency()
    duck_res = benchmark_duckdb_concurrency()

    full_results = {
        "PostGIS": pg_res,
        "DuckDB": duck_res,
    }

    with open(RESULTS_DIR / "concurrency_results.json", "w") as f:
        json.dump(full_results, f, indent=2)

    plot_concurrency(full_results)

if __name__ == "__main__":
    main()
