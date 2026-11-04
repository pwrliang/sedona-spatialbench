#!/usr/bin/env python3
"""SpatialBench Runner for GeoMesa 5.4.0 (Spark FileSystem / SQL) on HDFS.

Evaluates LocationTech GeoMesa 5.4.0 on Apache Spark 3.5.9 cluster
(17 nodes: 1 master, 16 workers, 128 cores, 477 GB RAM) on SpatialBench SF100.
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


class QueryTimeoutException(Exception):
    pass


def _timeout_handler(signum, frame):
    raise QueryTimeoutException("Query execution timed out")

# GeoMesa Queries for SpatialBench SF100
QUERIES = {
    "q1": """
SELECT
   t.t_tripkey,
   st_x(st_geomFromWKB(t.t_pickuploc)) AS pickup_longitude,
   st_y(st_geomFromWKB(t.t_pickuploc)) AS pickup_latitude,
   t.t_totalamount,
   t.t_distance
FROM trip t
WHERE st_distance(st_point(-111.7635, 34.8697), st_geomFromWKB(t.t_pickuploc)) <= 0.45
ORDER BY st_distance(st_point(-111.7635, 34.8697), st_geomFromWKB(t.t_pickuploc)) ASC
LIMIT 100
""",
    "q2": """
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
JOIN (SELECT st_geomFromWKB(z_boundary) AS boundary FROM zone WHERE z_name = 'Coconino County' LIMIT 1) z
  ON st_intersects(st_geomFromWKB(t.t_pickuploc), z.boundary)
""",
    "q3": """
SELECT
   date_trunc('month', t.t_pickuptime) AS pickup_month,
   COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_totalamount) AS avg_amount,
   AVG(t.t_distance) AS avg_distance
FROM trip t
WHERE st_intersects(
    st_bufferPoint(st_point(-111.775, 34.875), 0.1),
    st_geomFromWKB(t.t_pickuploc)
)
GROUP BY date_trunc('month', t.t_pickuptime)
ORDER BY pickup_month
""",
    "q4": """
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM zone z
JOIN (
    SELECT t_pickuploc
    FROM trip
    WHERE t_pickuptime >= TIMESTAMP '1995-01-01 00:00:00'
      AND t_pickuptime < TIMESTAMP '1995-02-01 00:00:00'
    ORDER BY t_tip DESC
    LIMIT 1000
) t ON st_intersects(st_geomFromWKB(z.z_boundary), st_geomFromWKB(t.t_pickuploc))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
LIMIT 100
""",
    "q5": """
SELECT
   c.c_custkey, c.c_name AS customer_name,
   date_trunc('month', t.t_pickuptime) AS trip_month,
   COUNT(t.t_tripkey) AS trip_count
FROM trip t
JOIN customer c ON t.t_custkey = c.c_custkey
WHERE t.t_pickuptime >= TIMESTAMP '1995-01-01 00:00:00'
  AND t.t_pickuptime < TIMESTAMP '1996-01-01 00:00:00'
GROUP BY c.c_custkey, c.c_name, date_trunc('month', t.t_pickuptime)
HAVING COUNT(t.t_tripkey) >= 10
ORDER BY trip_count DESC, c.c_custkey ASC, trip_month ASC
LIMIT 100
""",
    "q6": """
WITH filtered_zones AS (
    SELECT z_zonekey, z_name, st_geomFromWKB(z_boundary) AS boundary
    FROM zone
    WHERE st_intersects(
        st_polygonFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'),
        st_geomFromWKB(z_boundary)
    )
)
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups,
   AVG(t.t_distance) AS avg_distance,
   AVG(CAST(unix_timestamp(t.t_dropofftime) - unix_timestamp(t.t_pickuptime) AS DOUBLE)) AS avg_duration
FROM trip t
JOIN filtered_zones z ON st_intersects(st_geomFromWKB(t.t_pickuploc), z.boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
LIMIT 100
""",
    "q7": """
WITH trip_lengths AS (
    SELECT
        t.t_tripkey,
        t.t_distance,
        st_distance(st_geomFromWKB(t.t_pickuploc), st_geomFromWKB(t.t_dropoffloc)) AS straight_line_distance
    FROM trip t
    WHERE t.t_distance > 0
)
SELECT
    COUNT(*) AS total_trips,
    COUNT(CASE WHEN straight_line_distance > 0 AND (t_distance / straight_line_distance) > 2.0 THEN 1 END) AS detour_trips,
    AVG(CASE WHEN straight_line_distance > 0 THEN t_distance / straight_line_distance END) AS avg_detour_ratio
FROM trip_lengths
""",
    "q8": """
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t
JOIN building b
  ON st_distance(st_geomFromWKB(t.t_pickuploc), st_geomFromWKB(b.b_boundary)) <= 0.005
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC
LIMIT 100
""",
    "q9": """
WITH b1 AS (
    SELECT b_buildingkey AS id, st_geomFromWKB(b_boundary) AS geom
    FROM building
),
b2 AS (
    SELECT b_buildingkey AS id, st_geomFromWKB(b_boundary) AS geom
    FROM building
)
SELECT
    b1.id AS b1_id,
    b2.id AS b2_id,
    st_area(st_intersection(b1.geom, b2.geom)) /
        (st_area(b1.geom) + st_area(b2.geom) - st_area(st_intersection(b1.geom, b2.geom))) AS iou
FROM b1
JOIN b2 ON b1.id < b2.id AND st_intersects(b1.geom, b2.geom)
WHERE (st_area(b1.geom) + st_area(b2.geom) - st_area(st_intersection(b1.geom, b2.geom))) > 0
  AND st_area(st_intersection(b1.geom, b2.geom)) /
      (st_area(b1.geom) + st_area(b2.geom) - st_area(st_intersection(b1.geom, b2.geom))) >= 0.5
ORDER BY iou DESC, b1_id ASC, b2_id ASC
LIMIT 100
""",
    "q10": """
SELECT
   z.z_zonekey, z.z_name AS pickup_zone,
   AVG(CAST(unix_timestamp(t.t_dropofftime) - unix_timestamp(t.t_pickuptime) AS DOUBLE)) AS avg_trip_duration,
   COUNT(t.t_tripkey) AS trip_count
FROM trip t
JOIN zone z ON st_contains(st_geomFromWKB(z.z_boundary), st_geomFromWKB(t.t_pickuploc))
GROUP BY z.z_zonekey, z.z_name
HAVING COUNT(t.t_tripkey) >= 1000
ORDER BY avg_trip_duration DESC
LIMIT 100
""",
    "q11": """
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON st_within(st_geomFromWKB(t.t_pickuploc), st_geomFromWKB(pickup_zone.z_boundary))
JOIN zone dropoff_zone ON st_within(st_geomFromWKB(t.t_dropoffloc), st_geomFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey <> dropoff_zone.z_zonekey
""",
    "q12": """
WITH top_trips AS (
    SELECT t_tripkey, st_geomFromWKB(t_pickuploc) AS pickup_geom
    FROM trip
    ORDER BY t_totalamount DESC
    LIMIT 100
),
ranked_neighbors AS (
    SELECT
        tt.t_tripkey,
        b.b_buildingkey,
        st_distance(tt.pickup_geom, st_geomFromWKB(b.b_boundary)) AS distance,
        ROW_NUMBER() OVER (
            PARTITION BY tt.t_tripkey
            ORDER BY st_distance(tt.pickup_geom, st_geomFromWKB(b.b_boundary)) ASC
        ) AS rank_val
    FROM top_trips tt
    CROSS JOIN building b
)
SELECT
    t_tripkey,
    AVG(distance) AS avg_top5_dist
FROM ranked_neighbors
WHERE rank_val <= 5
GROUP BY t_tripkey
ORDER BY avg_top5_dist DESC
LIMIT 100
""",
}


def run_geomesa_query(spark, sql, timeout=600, job_group="geomesa_query"):
    """Executes a query via Spark DataFrame with active timeout monitoring."""
    sc = spark.sparkContext
    sc.setJobGroup(job_group, job_group)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout)
    start = time.perf_counter()
    try:
        df = spark.sql(sql)
        # Force evaluation by taking rows
        rows = df.take(100)
        signal.alarm(0)
        duration = time.perf_counter() - start
        return {
            "success": True,
            "duration": duration,
            "error": None,
            "timeout": False,
            "count": len(rows),
        }
    except QueryTimeoutException:
        signal.alarm(0)
        try:
            sc.cancelJobGroup(job_group)
        except Exception:
            pass
        return {
            "success": False,
            "duration": None,
            "error": f"Query timed out after {timeout} seconds",
            "timeout": True,
        }
    except Exception as e:
        signal.alarm(0)
        try:
            sc.cancelJobGroup(job_group)
        except Exception:
            pass
        duration = time.perf_counter() - start
        err_str = str(e)
        is_timeout = "timeout" in err_str.lower() or duration >= timeout
        return {
            "success": False,
            "duration": None if is_timeout else duration,
            "error": err_str[:300],
            "timeout": is_timeout,
        }


def main():
    parser = argparse.ArgumentParser(description="SpatialBench Runner for GeoMesa on Spark")
    parser.add_argument("--scale-factor", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--setting", choices=["default", "optimized"], default="default")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(args.log_dir, f"geomesa_cluster_sf{args.scale_factor}_{args.setting}_{timestamp}.log")
    result_file = os.path.join(args.output_dir, f"geomesa_cluster_sf{args.scale_factor}_{args.setting}.json")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info(f"Starting GeoMesa 5.4.0 Spark Benchmark: SF={args.scale_factor}, Setting={args.setting}")

    # Initialize Spark
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName(f"GeoMesaSpatialBench_SF{args.scale_factor}_{args.setting}") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator") \
        .config("spark.sql.shuffle.partitions", "128" if args.setting == "optimized" else "200") \
        .config("spark.default.parallelism", "128")

    spark = builder.getOrCreate()
    spark._jvm.org.locationtech.geomesa.spark.sql.SQLTypes.init(spark._jsparkSession.sqlContext())
    logging.info("GeoMesa SQL functions registered successfully.")

    # Register HDFS Parquet tables
    tables = ["building", "customer", "driver", "trip", "vehicle", "zone"]
    for t in tables:
        path = f"hdfs://master:9000/data/sf{args.scale_factor}/{t}"
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(t)
        logging.info(f"Registered table {t} from {path}")

    queries = dict(QUERIES)
    if args.setting == "optimized":
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
        queries["q4"] = queries["q4"].replace("JOIN (", "JOIN /*+ BROADCAST */ (")
        queries["q6"] = queries["q6"].replace("JOIN filtered_zones z", "JOIN /*+ BROADCAST(z) */ filtered_zones z")
        queries["q8"] = queries["q8"].replace("JOIN building b", "JOIN /*+ BROADCAST(b) */ building b")

    results = []

    for q_name in [f"q{i}" for i in range(1, 13)]:
        sql = queries[q_name]
        logging.info(f"--- Running {q_name.upper()} ({args.setting}) ---")

        run_times = []
        status = "SUCCESS"
        err_msg = None

        for iteration in range(1, args.iterations + 1):
            logging.info(f"  Iteration {iteration}/{args.iterations}...")
            job_group = f"geomesa_{q_name}_iter_{iteration}"
            res = run_geomesa_query(spark, sql, timeout=args.timeout, job_group=job_group)

            if res["success"]:
                logging.info(f"    Completed in {res['duration']:.3f}s (Rows: {res.get('count')})")
                run_times.append(res["duration"])
            elif res["timeout"]:
                logging.warning(f"    TIMEOUT after {args.timeout}s")
                status = "TIMEOUT"
                err_msg = res["error"]
                break
            else:
                logging.error(f"    FAILED: {res['error']}")
                status = "FAILED"
                err_msg = res["error"]
                break

        q_entry = {
            "query": q_name,
            "status": status,
            "iterations": args.iterations,
        }

        if status == "SUCCESS" and run_times:
            for idx, t in enumerate(run_times, 1):
                q_entry[f"run_{idx}"] = round(t, 4)
            q_entry["mean"] = round(sum(run_times) / len(run_times), 4)
            q_entry["min"] = round(min(run_times), 4)
            q_entry["max"] = round(max(run_times), 4)
        else:
            q_entry["mean"] = None
            q_entry["error"] = err_msg

        results.append(q_entry)
        logging.info(f"  Result: {status} (Mean: {q_entry.get('mean')}s)")

    spark.stop()

    with open(result_file, "w") as f:
        json.dump(results, f, indent=2)
    logging.info(f"Benchmark finished. Results saved to {result_file}")


if __name__ == "__main__":
    main()
