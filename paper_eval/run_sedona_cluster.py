#!/usr/bin/env python3
"""
Apache Sedona Distributed Cluster Benchmark Runner for SpatialBench.
Executes queries across the 16-node Spark/Sedona cluster on AWS EC2.
"""

import argparse
import json
import signal
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


class QueryTimeoutException(Exception):
    pass


def _timeout_handler(signum, frame):
    raise QueryTimeoutException("Query execution timed out")

QUERIES = {
    "q1": """
SELECT
   t.t_tripkey, ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon, ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat, t.t_pickuptime,
   ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('POINT (-111.7610 34.8697)')) AS distance_to_center
FROM trip t
WHERE ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('POINT (-111.7610 34.8697)'), 0.45)
ORDER BY distance_to_center ASC, t.t_tripkey ASC
LIMIT 100
    """,
    "q2": """
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(ST_GeomFromWKB(t.t_pickuploc), (SELECT ST_GeomFromWKB(z.z_boundary) FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1))
    """,
    "q3": """
SELECT
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month, COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_distance) AS avg_distance, AVG(UNIX_TIMESTAMP(t.t_dropofftime) - UNIX_TIMESTAMP(t.t_pickuptime)) AS avg_duration,
   AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_DWithin(
             ST_GeomFromWKB(t.t_pickuploc),
             ST_GeomFromText('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))'),
             0.045
     )
GROUP BY pickup_month
ORDER BY pickup_month
    """,
    "q4": """
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM
   zone z
       JOIN (
       SELECT t.t_pickuploc
       FROM trip t
       ORDER BY t.t_tip DESC, t.t_tripkey ASC
       LIMIT 1000
   ) top_trips ON ST_Within(ST_GeomFromWKB(top_trips.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
    """,
    "q5": """
SELECT
   c.c_custkey, c.c_name AS customer_name,
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
   ST_Area(ST_ConvexHull(ST_Collect_Agg(ST_GeomFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area,
   COUNT(*) as dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC, pickup_month ASC
LIMIT 100
    """,
    "q6": """
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups, AVG(t.t_distance) AS avg_distance,
   AVG(UNIX_TIMESTAMP(t.t_dropofftime) - UNIX_TIMESTAMP(t.t_pickuptime)) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'), ST_GeomFromWKB(z.z_boundary))
 AND ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
    """,
    "q7": """
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       t.t_distance AS reported_distance_m,
       ST_Length(
               ST_MakeLine(
                       ST_GeomFromWKB(t.t_pickuploc),
                       ST_GeomFromWKB(t.t_dropoffloc)
               )
       ) / 0.000009 AS line_distance_m
   FROM trip t
)
SELECT
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
LIMIT 100
    """,
    "q8": """
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t JOIN building b ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045)
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
LIMIT 100
    """,
    "q9": """
WITH b1 AS (
   SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom
   FROM building
),
    b2 AS (
        SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom
        FROM building
    ),
    pairs AS (
        SELECT
            b1.id AS building_1,
            b2.id AS building_2,
            ST_Area(b1.geom) AS area1,
            ST_Area(b2.geom) AS area2,
            ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
        FROM b1
                 JOIN b2
                      ON b1.id < b2.id
                          AND ST_Intersects(b1.geom, b2.geom)
    )
SELECT
   building_1,
   building_2,
   area1,
   area2,
   overlap_area,
   CASE
       WHEN overlap_area = 0 THEN 0.0
       WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
       ELSE overlap_area / (area1 + area2 - overlap_area)
       END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC
LIMIT 100
    """,
    "q10": """
SELECT
   z.z_zonekey, z.z_name AS pickup_zone,
   AVG(UNIX_TIMESTAMP(t.t_dropofftime) - UNIX_TIMESTAMP(t.t_pickuptime)) AS avg_duration,
   AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM zone z LEFT JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
LIMIT 100
    """,
    "q11": """
SELECT COUNT(*) AS cross_zone_trip_count
FROM
   trip t
       JOIN zone pickup_zone ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
       JOIN zone dropoff_zone ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
    """,
    "q12": """
WITH trip_with_geom AS (
   SELECT t_tripkey, ST_GeomFromWKB(t_pickuploc) as pickup_geom
   FROM trip
),
    building_with_geom AS (
        SELECT ST_GeomFromWKB(b_boundary) as boundary_geom
        FROM building
    ),
    knn AS (
        SELECT
            t.t_tripkey,
            ST_Distance(t.pickup_geom, b.boundary_geom) AS distance_to_building
        FROM trip_with_geom t JOIN building_with_geom b
                                  ON ST_KNN(t.pickup_geom, b.boundary_geom, 5, FALSE)
    )
SELECT
   t_tripkey,
   AVG(distance_to_building) AS avg_distance_to_5_nearest
FROM knn
GROUP BY t_tripkey
ORDER BY avg_distance_to_5_nearest DESC, t_tripkey ASC
LIMIT 100
    """,
}


def run_benchmark():
    parser = argparse.ArgumentParser(description="Run SpatialBench on Sedona Spark Cluster")
    parser.add_argument("--data-dir", default="/home/ubuntu/data/sf1", help="Path to data directory on cluster nodes")
    parser.add_argument("--sf", type=str, default="1", help="Scale factor (1 or 10)")
    parser.add_argument("--runs", type=int, default=3, help="Number of repetitions per query")
    parser.add_argument("--timeout", type=int, default=600, help="Per-run timeout in seconds (default 600)")
    parser.add_argument("--queries", default="all", help="Comma-separated query names or 'all'")
    parser.add_argument("--output", default="/home/ubuntu/sedona_cluster_results.json", help="Output JSON path")
    args = parser.parse_args()

    print("=" * 70)
    print(" Sedona Spark Cluster Benchmark (AWS EC2 16 Workers, 128 Cores)")
    print(f" Data Directory : {args.data_dir}")
    print(f" Scale Factor   : SF{args.sf}")
    print(f" Repetitions    : {args.runs}")
    print(f" Timeout        : {args.timeout}s")
    print(f" Queries        : {args.queries}")
    print("=" * 70)

    # Initialize Sedona SparkSession targeting the standalone cluster
    config = (
        SedonaContext.builder()
        .master("spark://master:7077")
        .appName(f"SedonaSpatialBench_SF{args.sf}")
        .config("spark.executor.cores", "8")
        .config("spark.executor.memory", "26g")
        .config("spark.driver.memory", "16g")
        .config("spark.driver.host", "master")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "128")
        .getOrCreate()
    )
    spark = SedonaContext.create(config)
    sc = spark.sparkContext
    print(f"Connected to Master: {sc.master}, Cores: {sc.defaultParallelism}")

    # Register all tables as temporary views
    data_path = Path(args.data_dir)
    tables = ["building", "customer", "driver", "trip", "vehicle", "zone"]
    print("Registering Parquet tables as Spark views...")
    t_load = time.time()
    for tbl in tables:
        tbl_path = str(data_path / tbl)
        df = spark.read.parquet(tbl_path)
        df.createOrReplaceTempView(tbl)
    print(f"All tables registered in {time.time() - t_load:.2f}s")

    # Select queries
    if args.queries == "all":
        query_keys = list(QUERIES.keys())
    else:
        query_keys = [q.strip().lower() for q in args.queries.split(",") if q.strip()]

    results = []
    for q_name in query_keys:
        if q_name not in QUERIES:
            print(f"Skipping unknown query {q_name}")
            continue

        q_sql = QUERIES[q_name]
        print(f"\n--- Running {q_name.upper()} ({args.runs} runs) ---")
        run_times = []
        last_row_count = None
        error_msg = None
        status = "success"

        for run_idx in range(args.runs):
            job_group_id = f"spatialbench_{q_name}_run_{run_idx}"
            sc.setJobGroup(job_group_id, f"SpatialBench {q_name} (Run {run_idx + 1}/{args.runs})")
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(args.timeout)
            try:
                t0 = time.perf_counter()
                df_res = spark.sql(q_sql)
                # Materialize results by collecting to driver (bounded to at most 100 rows by query LIMIT)
                collected = df_res.collect()
                elapsed = time.perf_counter() - t0
                signal.alarm(0)
                run_times.append(round(elapsed, 4))
                last_row_count = len(collected)
                print(f"  Run {run_idx + 1}/{args.runs}: {elapsed:.3f}s ({last_row_count} rows)")
            except QueryTimeoutException:
                signal.alarm(0)
                sc.cancelJobGroup(job_group_id)
                print(f"  Run {run_idx + 1}/{args.runs}: TIMEOUT (> {args.timeout}s)")
                status = "timeout"
                error_msg = f"Timed out after {args.timeout} seconds"
                break
            except Exception as e:
                signal.alarm(0)
                sc.cancelJobGroup(job_group_id)
                print(f"  Run {run_idx + 1}/{args.runs}: FAILED - {e}")
                error_msg = str(e)
                status = "failed"
                break

        if status == "success" and run_times:
            mean_time = round(statistics.mean(run_times), 3)
            std_dev = round(statistics.stdev(run_times), 3) if len(run_times) > 1 else 0.0
            min_time = min(run_times)
            max_time = max(run_times)
        else:
            mean_time = None
            std_dev = None
            min_time = None
            max_time = None

        res_entry = {
            "query": q_name,
            "engine": "sedona_cluster",
            "time_seconds": mean_time,
            "row_count": last_row_count,
            "status": status,
            "error_message": error_msg,
            "run_times": run_times,
            "std_dev": std_dev,
            "min_time": min_time,
            "max_time": max_time,
        }
        results.append(res_entry)
        if mean_time is not None:
            print(f"  >> {q_name.upper()} Summary: Mean={mean_time:.3f}s (±{std_dev:.3f}s)")

    # Write results JSON
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suite_data = {
        "benchmark": "SpatialBench",
        "scale_factor": args.sf,
        "engine": "sedona_cluster",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cluster": {
            "master": sc.master,
            "workers": 16,
            "total_cores": 128,
            "spark_version": spark.version,
        },
        "results": results,
    }
    with open(output_path, "w") as f:
        json.dump(suite_data, f, indent=2)
    print(f"\n[SUCCESS] Benchmark results saved to {output_path}")

    spark.stop()


if __name__ == "__main__":
    run_benchmark()
