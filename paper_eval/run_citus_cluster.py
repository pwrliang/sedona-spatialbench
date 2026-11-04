#!/usr/bin/env python3
"""SpatialBench Runner for Citus Data 14.3 + PostGIS 3.6 Cluster.

Evaluates Citus 14.3 distributed PostgreSQL cluster (17 nodes: 1 coordinator, 16 workers)
on SpatialBench SF100.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
import psycopg2

QUERIES = {
    "q1": """
SELECT
   t.t_tripkey, ST_X(t.t_pickuploc) AS pickup_longitude, ST_Y(t.t_pickuploc) AS pickup_latitude,
   t.t_totalamount, t.t_distance
FROM trip t
WHERE ST_Distance(ST_SetSRID(ST_Point(-111.7635, 34.8697), 4326), t.t_pickuploc) <= 0.45
ORDER BY ST_Distance(ST_SetSRID(ST_Point(-111.7635, 34.8697), 4326), t.t_pickuploc) ASC
LIMIT 100;
""",
    "q2": """
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(t.t_pickuploc, (SELECT z_boundary FROM zone WHERE z_name = 'Coconino County' LIMIT 1));
""",
    "q3": """
SELECT
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
   COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_totalamount) AS avg_amount,
   AVG(t.t_distance) AS avg_distance
FROM trip t
WHERE ST_Intersects(
    ST_Buffer(ST_SetSRID(ST_GeomFromText('POLYGON((-111.85 34.80, -111.70 34.80, -111.70 34.95, -111.85 34.95, -111.85 34.80))'), 4326), 0.05),
    t.t_pickuploc
)
GROUP BY DATE_TRUNC('month', t.t_pickuptime)
ORDER BY pickup_month;
""",
    "q4": """
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM
   zone z
   JOIN (
       SELECT t_pickuploc
       FROM trip
       WHERE t_pickuptime >= TIMESTAMP '1995-01-01 00:00:00'
         AND t_pickuptime < TIMESTAMP '1995-02-01 00:00:00'
       ORDER BY t_tip DESC
       LIMIT 1000
   ) t ON ST_Intersects(z.z_boundary, t.t_pickuploc)
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC;
""",
    "q5": """
SELECT
   c.c_custkey, c.c_name AS customer_name,
   DATE_TRUNC('month', t.t_pickuptime) AS trip_month,
   COUNT(t.t_tripkey) AS trip_count,
   ST_ConvexHull(ST_Collect(t.t_dropoffloc)) AS dropoff_convex_hull
FROM trip t
JOIN customer c ON t.t_custkey = c.c_custkey
WHERE t.t_pickuptime >= TIMESTAMP '1995-01-01 00:00:00'
  AND t.t_pickuptime < TIMESTAMP '1996-01-01 00:00:00'
GROUP BY c.c_custkey, c.c_name, DATE_TRUNC('month', t.t_pickuptime)
HAVING COUNT(t.t_tripkey) >= 10
ORDER BY trip_count DESC, c.c_custkey ASC, trip_month ASC
LIMIT 100;
""",
    "q6": """
WITH filtered_zones AS (
    SELECT z_zonekey, z_name, z_boundary
    FROM zone
    WHERE ST_Intersects(
        ST_SetSRID(ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'), 4326),
        z_boundary
    )
)
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups,
   AVG(t.t_distance) AS avg_distance,
   AVG(EXTRACT(EPOCH FROM (t.t_dropofftime - t.t_pickuptime))) AS avg_duration
FROM trip t
JOIN filtered_zones z ON ST_Intersects(t.t_pickuploc, z.z_boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC;
""",
    "q7": """
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       t.t_distance,
       ST_Distance(t.t_pickuploc, t.t_dropoffloc) AS straight_line_distance
   FROM trip t
   WHERE t.t_distance > 0
)
SELECT
   COUNT(*) AS total_trips,
   COUNT(CASE WHEN straight_line_distance > 0 AND (t_distance / straight_line_distance) > 2.0 THEN 1 END) AS detour_trips,
   AVG(CASE WHEN straight_line_distance > 0 THEN t_distance / straight_line_distance END) AS avg_detour_ratio
FROM trip_lengths;
""",
    "q8": """
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t
JOIN building b ON ST_DWithin(t.t_pickuploc, b.b_boundary, 0.005)
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC
LIMIT 100;
""",
    "q9": """
WITH b1 AS (
   SELECT b_buildingkey AS id, b_boundary AS geom
   FROM building
),
b2 AS (
   SELECT b_buildingkey AS id, b_boundary AS geom
   FROM building
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
LIMIT 100;
""",
    "q10": """
SELECT
   z.z_zonekey, z.z_name AS pickup_zone,
   AVG(EXTRACT(EPOCH FROM (t.t_dropofftime - t.t_pickuptime))) AS avg_trip_duration,
   COUNT(t.t_tripkey) AS trip_count
FROM trip t
JOIN zone z ON ST_Contains(z.z_boundary, t.t_pickuploc)
GROUP BY z.z_zonekey, z.z_name
HAVING COUNT(t.t_tripkey) >= 1000
ORDER BY avg_trip_duration DESC
LIMIT 100;
""",
    "q11": """
SELECT COUNT(*) AS cross_zone_trip_count
FROM
   trip t
   JOIN zone pickup_zone ON ST_Within(t.t_pickuploc, pickup_zone.z_boundary)
   JOIN zone dropoff_zone ON ST_Within(t.t_dropoffloc, dropoff_zone.z_boundary)
WHERE pickup_zone.z_zonekey <> dropoff_zone.z_zonekey;
""",
    "q12": """
SELECT
   t.t_tripkey,
   AVG(nb.distance_to_building) AS avg_distance_to_top5_buildings
FROM (
   SELECT t_tripkey, t_pickuploc
   FROM trip
   ORDER BY t_totalamount DESC
   LIMIT 100
) t
CROSS JOIN LATERAL (
   SELECT
       b.b_buildingkey,
       ST_Distance(t.t_pickuploc, b.b_boundary) AS distance_to_building
   FROM building b
   ORDER BY t.t_pickuploc <-> b.b_boundary
   LIMIT 5
) nb
GROUP BY t.t_tripkey
ORDER BY avg_distance_to_top5_buildings DESC
LIMIT 100;
""",
}


def run_citus_query(conn, sql, timeout=600):
    start = time.perf_counter()
    try:
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout = '{timeout}s';")
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        duration = time.perf_counter() - start
        return {
            "success": True,
            "duration": duration,
            "error": None,
            "timeout": False,
            "count": len(rows),
        }
    except psycopg2.extensions.QueryCanceledError as e:
        conn.rollback()
        return {
            "success": False,
            "duration": None,
            "error": f"Query timed out after {timeout} seconds",
            "timeout": True,
        }
    except Exception as e:
        conn.rollback()
        err_str = str(e)
        is_timeout = "timeout" in err_str.lower()
        return {
            "success": False,
            "duration": None if is_timeout else time.perf_counter() - start,
            "error": err_str[:300],
            "timeout": is_timeout,
        }


def main():
    parser = argparse.ArgumentParser(description="SpatialBench Runner for Citus Data Cluster")
    parser.add_argument("--scale-factor", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--setting", choices=["default", "optimized"], default="default")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--dbname", default="spatialbench")
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(args.log_dir, f"citus_cluster_sf{args.scale_factor}_{args.setting}_{timestamp}.log")
    result_file = os.path.join(args.output_dir, f"citus_cluster_sf{args.scale_factor}_{args.setting}.json")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info(f"Starting Citus Data Cluster Benchmark: SF={args.scale_factor}, Setting={args.setting}")

    conn = psycopg2.connect(
        dbname=args.dbname,
        user="postgres",
        host="127.0.0.1",
        port=5432,
    )
    conn.autocommit = True

    # Configure session settings
    cur = conn.cursor()
    if args.setting == "optimized":
        cur.execute("SET max_parallel_workers_per_gather = 8;")
        cur.execute("SET work_mem = '512MB';")
        cur.execute("SET citus.enable_repartition_joins = on;")
    cur.close()

    results = []

    for q_name in [f"q{i}" for i in range(1, 13)]:
        sql = QUERIES[q_name]
        logging.info(f"--- Running {q_name.upper()} ({args.setting}) ---")

        run_times = []
        status = "SUCCESS"
        err_msg = None

        for iteration in range(1, args.iterations + 1):
            logging.info(f"  Iteration {iteration}/{args.iterations}...")
            res = run_citus_query(conn, sql, timeout=args.timeout)

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

    conn.close()

    with open(result_file, "w") as f:
        json.dump(results, f, indent=2)
    logging.info(f"Benchmark finished. Results saved to {result_file}")


if __name__ == "__main__":
    main()
