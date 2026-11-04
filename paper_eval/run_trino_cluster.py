#!/usr/bin/env python3
"""SpatialBench Runner for Trino Cluster (Distributed) on HDFS.

Evaluates Trino 484 Cluster (17 nodes: 1 coordinator, 16 workers, 128 cores, 477 GB RAM)
on SpatialBench SF100 under both Default and Optimized settings.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Trino Queries for SpatialBench SF100
QUERIES = {
    "q1": """
SELECT
   t.t_tripkey,
   ST_X(ST_GeomFromBinary(t.t_pickuploc)) AS pickup_longitude,
   ST_Y(ST_GeomFromBinary(t.t_pickuploc)) AS pickup_latitude,
   t.t_totalamount,
   t.t_distance
FROM hive.sf100.trip t
WHERE ST_Distance(ST_Point(-111.7635, 34.8697), ST_GeomFromBinary(t.t_pickuploc)) <= 0.45
ORDER BY ST_Distance(ST_Point(-111.7635, 34.8697), ST_GeomFromBinary(t.t_pickuploc)) ASC
LIMIT 100
""",
    "q2": """
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM hive.sf100.trip t
JOIN (SELECT ST_GeomFromBinary(z_boundary) AS boundary FROM hive.sf100.zone WHERE z_name = 'Coconino County' LIMIT 1) z
  ON ST_Intersects(ST_GeomFromBinary(t.t_pickuploc), z.boundary)
""",
    "q3": """
SELECT
   date_trunc('month', t.t_pickuptime) AS pickup_month,
   COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_totalamount) AS avg_amount,
   AVG(t.t_distance) AS avg_distance
FROM hive.sf100.trip t
WHERE ST_Intersects(
    ST_Buffer(ST_GeometryFromText('POLYGON((-111.85 34.80, -111.70 34.80, -111.70 34.95, -111.85 34.95, -111.85 34.80))'), 0.05),
    ST_GeomFromBinary(t.t_pickuploc)
)
GROUP BY date_trunc('month', t.t_pickuptime)
ORDER BY pickup_month
""",
    "q4": """
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM hive.sf100.zone z
JOIN (
    SELECT t_pickuploc
    FROM hive.sf100.trip
    WHERE t_pickuptime >= TIMESTAMP '1995-01-01 00:00:00'
      AND t_pickuptime < TIMESTAMP '1995-02-01 00:00:00'
    ORDER BY t_tip DESC
    LIMIT 1000
) t ON ST_Intersects(ST_GeomFromBinary(z.z_boundary), ST_GeomFromBinary(t.t_pickuploc))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
LIMIT 100
""",
    "q5": """
SELECT
   c.c_custkey, c.c_name AS customer_name,
   date_trunc('month', t.t_pickuptime) AS trip_month,
   COUNT(t.t_tripkey) AS trip_count,
   convex_hull_agg(ST_GeomFromBinary(t.t_dropoffloc)) AS dropoff_convex_hull
FROM hive.sf100.trip t
JOIN hive.sf100.customer c ON t.t_custkey = c.c_custkey
WHERE t.t_pickuptime >= TIMESTAMP '1995-01-01 00:00:00'
  AND t.t_pickuptime < TIMESTAMP '1996-01-01 00:00:00'
GROUP BY c.c_custkey, c.c_name, date_trunc('month', t.t_pickuptime)
HAVING COUNT(t.t_tripkey) >= 10
ORDER BY trip_count DESC, c.c_custkey ASC, trip_month ASC
LIMIT 100
""",
    "q6": """
WITH filtered_zones AS (
    SELECT z_zonekey, z_name, ST_GeomFromBinary(z_boundary) AS boundary
    FROM hive.sf100.zone
    WHERE ST_Intersects(
        ST_GeometryFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'),
        ST_GeomFromBinary(z_boundary)
    )
)
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups,
   AVG(t.t_distance) AS avg_distance,
   AVG(date_diff('second', t.t_pickuptime, t.t_dropofftime)) AS avg_duration
FROM hive.sf100.trip t
JOIN filtered_zones z ON ST_Intersects(ST_GeomFromBinary(t.t_pickuploc), z.boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
LIMIT 100
""",
    "q7": """
WITH trip_lengths AS (
    SELECT
        t.t_tripkey,
        t.t_distance,
        ST_Distance(ST_GeomFromBinary(t.t_pickuploc), ST_GeomFromBinary(t.t_dropoffloc)) AS straight_line_distance
    FROM hive.sf100.trip t
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
FROM hive.sf100.trip t
JOIN hive.sf100.building b
  ON ST_Distance(ST_GeomFromBinary(t.t_pickuploc), ST_GeomFromBinary(b.b_boundary)) <= 0.005
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC
LIMIT 100
""",
    "q9": """
WITH b1 AS (
    SELECT b_buildingkey AS id, ST_GeomFromBinary(b_boundary) AS geom
    FROM hive.sf100.building
),
b2 AS (
    SELECT b_buildingkey AS id, ST_GeomFromBinary(b_boundary) AS geom
    FROM hive.sf100.building
)
SELECT
    b1.id AS b1_id,
    b2.id AS b2_id,
    ST_Area(ST_Intersection(b1.geom, b2.geom)) /
        (ST_Area(b1.geom) + ST_Area(b2.geom) - ST_Area(ST_Intersection(b1.geom, b2.geom))) AS iou
FROM b1
JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom)
WHERE (ST_Area(b1.geom) + ST_Area(b2.geom) - ST_Area(ST_Intersection(b1.geom, b2.geom))) > 0
  AND ST_Area(ST_Intersection(b1.geom, b2.geom)) /
      (ST_Area(b1.geom) + ST_Area(b2.geom) - ST_Area(ST_Intersection(b1.geom, b2.geom))) >= 0.5
ORDER BY iou DESC, b1_id ASC, b2_id ASC
LIMIT 100
""",
    "q10": """
SELECT
   z.z_zonekey, z.z_name AS pickup_zone,
   AVG(date_diff('second', t.t_pickuptime, t.t_dropofftime)) AS avg_trip_duration,
   COUNT(t.t_tripkey) AS trip_count
FROM hive.sf100.trip t
JOIN hive.sf100.zone z ON ST_Contains(ST_GeomFromBinary(z.z_boundary), ST_GeomFromBinary(t.t_pickuploc))
GROUP BY z.z_zonekey, z.z_name
HAVING COUNT(t.t_tripkey) >= 1000
ORDER BY avg_trip_duration DESC
LIMIT 100
""",
    "q11": """
SELECT COUNT(*) AS cross_zone_trip_count
FROM hive.sf100.trip t
JOIN hive.sf100.zone pickup_zone ON ST_Within(ST_GeomFromBinary(t.t_pickuploc), ST_GeomFromBinary(pickup_zone.z_boundary))
JOIN hive.sf100.zone dropoff_zone ON ST_Within(ST_GeomFromBinary(t.t_dropoffloc), ST_GeomFromBinary(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey <> dropoff_zone.z_zonekey
""",
    "q12": """
WITH top_trips AS (
    SELECT t_tripkey, ST_GeomFromBinary(t_pickuploc) AS pickup_geom
    FROM hive.sf100.trip
    ORDER BY t_totalamount DESC
    LIMIT 100
),
ranked_neighbors AS (
    SELECT
        tt.t_tripkey,
        b.b_buildingkey,
        ST_Distance(tt.pickup_geom, ST_GeomFromBinary(b.b_boundary)) AS distance,
        ROW_NUMBER() OVER (
            PARTITION BY tt.t_tripkey
            ORDER BY ST_Distance(tt.pickup_geom, ST_GeomFromBinary(b.b_boundary)) ASC
        ) AS rank_val
    FROM top_trips tt
    CROSS JOIN hive.sf100.building b
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


def run_trino_query(sql, timeout=600, host="127.0.0.1", port=8085, session_properties=None):
    """Executes a query against Trino cluster with timeout enforcement."""
    session_prefix = ""
    if session_properties:
        for k, v in session_properties.items():
            session_prefix += f"SET SESSION {k} = {v};\n"

    full_sql = session_prefix + sql.strip().rstrip(";") + ";"

    cmd = [
        "/home/ubuntu/trino/trino",
        "--server",
        f"http://{host}:{port}",
        "--catalog",
        "hive",
        "--schema",
        "sf100",
        "--execute",
        full_sql,
    ]

    start = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        duration = time.perf_counter() - start

        if proc.returncode == 0:
            return {
                "success": True,
                "duration": duration,
                "output": proc.stdout[:200],
                "error": None,
                "timeout": False,
            }
        else:
            return {
                "success": False,
                "duration": duration,
                "output": None,
                "error": proc.stderr.strip() or proc.stdout.strip(),
                "timeout": False,
            }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "duration": None,
            "output": None,
            "error": f"Query timed out after {timeout} seconds",
            "timeout": True,
        }
    except Exception as e:
        return {
            "success": False,
            "duration": None,
            "output": None,
            "error": str(e),
            "timeout": False,
        }


def main():
    parser = argparse.ArgumentParser(description="SpatialBench Runner for Trino Cluster")
    parser.add_argument("--scale-factor", type=int, default=100, help="SpatialBench scale factor")
    parser.add_argument("--iterations", type=int, default=1, help="Benchmark iterations per query")
    parser.add_argument("--setting", choices=["default", "optimized"], default="default", help="Benchmark setting")
    parser.add_argument("--timeout", type=int, default=600, help="Query timeout in seconds")
    parser.add_argument("--host", default="127.0.0.1", help="Trino coordinator host")
    parser.add_argument("--port", type=int, default=8085, help="Trino coordinator port")
    parser.add_argument("--output-dir", default="results", help="Directory for JSON results")
    parser.add_argument("--log-dir", default="logs", help="Directory for log files")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(args.log_dir, f"trino_cluster_sf{args.scale_factor}_{args.setting}_{timestamp}.log")
    result_file = os.path.join(args.output_dir, f"trino_cluster_sf{args.scale_factor}_{args.setting}.json")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info(f"Starting Trino Cluster Benchmark: SF={args.scale_factor}, Setting={args.setting}")

    # Session properties for optimization
    session_properties = {}
    if args.setting == "optimized":
        session_properties = {
            "join_distribution_type": "'AUTOMATIC'",
            "max_hash_partition_count": "128",
            "min_hash_partition_count": "16",
            "task_concurrency": "8",
        }

    results = []

    for q_name in [f"q{i}" for i in range(1, 13)]:
        sql = QUERIES[q_name]
        logging.info(f"--- Running {q_name.upper()} ({args.setting}) ---")

        run_times = []
        status = "SUCCESS"
        err_msg = None

        for iteration in range(1, args.iterations + 1):
            logging.info(f"  Iteration {iteration}/{args.iterations}...")
            res = run_trino_query(
                sql,
                timeout=args.timeout,
                host=args.host,
                port=args.port,
                session_properties=session_properties,
            )

            if res["success"]:
                logging.info(f"    Completed in {res['duration']:.3f}s")
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

    with open(result_file, "w") as f:
        json.dump(results, f, indent=2)
    logging.info(f"Benchmark finished. Results saved to {result_file}")


if __name__ == "__main__":
    main()
