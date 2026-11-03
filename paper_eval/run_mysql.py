#!/usr/bin/env python3
"""
SpatialBench Benchmark Runner for MySQL 8.0.
Evaluates Q1-Q12 on SF1 with strict timeouts, thread cancellation, and statistics.
"""

import argparse
import datetime
import json
import os
import signal
import statistics
import sys
import time
from datetime import timezone
from pathlib import Path
import pymysql

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

QUERIES = {
    "q1": """
SELECT
   t.t_tripkey,
   ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon,
   ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat,
   t.t_pickuptime,
   ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_PointFromText('POINT(-111.7610 34.8697)')) AS distance_to_center
FROM trip t
WHERE ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_PointFromText('POINT(-111.7610 34.8697)')) <= 0.45
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
   DATE_FORMAT(t.t_pickuptime, '%Y-%m-01') AS pickup_month,
   COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_distance) AS avg_distance,
   AVG(TIMESTAMPDIFF(SECOND, t.t_pickuptime, t.t_dropofftime)) AS avg_duration,
   AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_Distance(
   ST_GeomFromWKB(t.t_pickuploc),
   ST_GeomFromText('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))')
) <= 0.045
GROUP BY pickup_month
ORDER BY pickup_month
    """,
    "q4": """
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM zone z
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
   c.c_custkey,
   c.c_name AS customer_name,
   DATE_FORMAT(t.t_pickuptime, '%Y-%m-01') AS pickup_month,
   ST_Area(ST_ConvexHull(ST_GeomFromText(CONCAT('MULTIPOINT(', GROUP_CONCAT(CONCAT(ST_X(ST_GeomFromWKB(t.t_dropoffloc)), ' ', ST_Y(ST_GeomFromWKB(t.t_dropoffloc))) SEPARATOR ','), ')')))) AS monthly_travel_hull_area,
   COUNT(*) AS dropoff_count
FROM trip t
JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC, pickup_month ASC
LIMIT 100
    """,
    "q6": """
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups,
   AVG(t.t_distance) AS avg_distance,
   AVG(TIMESTAMPDIFF(SECOND, t.t_pickuptime, t.t_dropofftime)) AS avg_duration
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
       CAST(t.t_distance AS DOUBLE) AS reported_distance_m,
       ST_Length(LineString(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(t.t_dropoffloc))) / 0.000009 AS line_distance_m
   FROM trip t
)
SELECT
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC, reported_distance_m DESC, t_tripkey ASC
LIMIT 100
    """,
    "q8": """
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t
JOIN building b ON ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) <= 0.0045
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
LIMIT 100
    """,
    "q9": """
WITH pairs AS (
   SELECT
       b1.b_buildingkey AS building_1,
       b2.b_buildingkey AS building_2,
       ST_Area(ST_GeomFromWKB(b1.b_boundary)) AS area1,
       ST_Area(ST_GeomFromWKB(b2.b_boundary)) AS area2,
       ST_Area(ST_Intersection(ST_GeomFromWKB(b1.b_boundary), ST_GeomFromWKB(b2.b_boundary))) AS overlap_area
   FROM building b1
   JOIN building b2 ON b1.b_buildingkey < b2.b_buildingkey
                   AND ST_Intersects(ST_GeomFromWKB(b1.b_boundary), ST_GeomFromWKB(b2.b_boundary))
)
SELECT
   building_1, building_2, area1, area2, overlap_area,
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
   AVG(TIMESTAMPDIFF(SECOND, t.t_pickuptime, t.t_dropofftime)) AS avg_duration,
   AVG(t.t_distance) AS avg_distance,
   COUNT(t.t_tripkey) AS num_trips
FROM zone z
LEFT JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC, z.z_zonekey ASC
LIMIT 100
    """,
    "q11": """
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
JOIN zone dropoff_zone ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
    """,
    "q12": """
SELECT
   t.t_tripkey,
   AVG(nb.distance_to_building) AS avg_distance_to_5_nearest
FROM trip t
CROSS JOIN LATERAL (
   SELECT ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) AS distance_to_building
   FROM building b
   ORDER BY distance_to_building ASC
   LIMIT 5
) AS nb
GROUP BY t.t_tripkey
ORDER BY avg_distance_to_5_nearest DESC, t.t_tripkey ASC
LIMIT 100
    """,
}


class QueryTimeoutException(Exception):
    pass


def main():
    parser = argparse.ArgumentParser(description="Run SpatialBench on MySQL 8.0")
    parser.add_argument("--socket", default="/local/scratch/liang/mysql_db/mysql.sock", help="Unix socket path")
    parser.add_argument("--db", default="spatialbench", help="Database name")
    parser.add_argument("--sf", type=str, default="1", help="Scale factor")
    parser.add_argument("--runs", type=int, default=3, help="Repetitions per query")
    parser.add_argument("--timeout", type=int, default=600, help="Per-run timeout in seconds")
    parser.add_argument("--queries", default="all", help="Comma-separated query names or 'all'")
    parser.add_argument("--output", default=None, help="Output JSON path")
    args = parser.parse_args()

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = REPO_ROOT / "paper_eval" / "results" / f"mysql_sf{args.sf}_default.json"

    print("=" * 70)
    print(" MySQL 8.0 Benchmark Runner")
    print(f" Database     : {args.db}")
    print(f" Socket       : {args.socket}")
    print(f" Scale Factor : SF{args.sf}")
    print(f" Repetitions  : {args.runs}")
    print(f" Timeout      : {args.timeout}s")
    print(f" Queries      : {args.queries}")
    print(f" Output Path  : {output_path}")
    print("=" * 70)

    con = pymysql.connect(unix_socket=args.socket, user="root", db=args.db)

    if args.queries == "all":
        selected_queries = list(QUERIES.keys())
    else:
        selected_queries = [q.strip().lower() for q in args.queries.split(",") if q.strip()]

    results = []
    total_start = time.perf_counter()

    for q_name in selected_queries:
        if q_name not in QUERIES:
            print(f"Skipping unknown query: {q_name}")
            continue

        q_sql = QUERIES[q_name]
        print(f"\n--- Running {q_name.upper()} ({args.runs} runs) ---")
        run_times = []
        last_row_count = None
        error_msg = None
        status = "success"

        for run_idx in range(args.runs):
            # Handler to interrupt MySQL query via KILL QUERY
            def interrupt_cb(signum, frame):
                try:
                    admin_con = pymysql.connect(unix_socket=args.socket, user="root", db=args.db)
                    with admin_con.cursor() as cur:
                        cur.execute(f"KILL QUERY {con.thread_id()}")
                    admin_con.close()
                except Exception:
                    pass
                raise QueryTimeoutException("Query execution timed out")

            signal.signal(signal.SIGALRM, interrupt_cb)
            signal.alarm(args.timeout)

            try:
                t0 = time.perf_counter()
                with con.cursor() as cur:
                    cur.execute(q_sql)
                    rows = cur.fetchall()
                elapsed = time.perf_counter() - t0
                signal.alarm(0)
                run_times.append(round(elapsed, 4))
                last_row_count = len(rows)
                print(f"  Run {run_idx + 1}/{args.runs}: {elapsed:.3f}s ({last_row_count} rows)")
            except (QueryTimeoutException, pymysql.err.OperationalError) as e:
                signal.alarm(0)
                if "query execution was interrupted" in str(e).lower() or isinstance(e, QueryTimeoutException):
                    print(f"  Run {run_idx + 1}/{args.runs}: TIMEOUT (> {args.timeout}s)")
                    status = "timeout"
                    error_msg = f"Query timed out after {args.timeout} seconds"
                else:
                    print(f"  Run {run_idx + 1}/{args.runs}: FAILED - {e}")
                    status = "failed"
                    error_msg = str(e)
                # Reconnect in case connection was affected
                try:
                    con.close()
                except Exception:
                    pass
                con = pymysql.connect(unix_socket=args.socket, user="root", db=args.db)
                break
            except Exception as e:
                signal.alarm(0)
                print(f"  Run {run_idx + 1}/{args.runs}: FAILED - {e}")
                status = "failed"
                error_msg = str(e)
                try:
                    con.close()
                except Exception:
                    pass
                con = pymysql.connect(unix_socket=args.socket, user="root", db=args.db)
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
            "engine": "mysql",
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

    con.close()
    total_time = round(time.perf_counter() - total_start, 2)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suite_data = {
        "benchmark": "spatialbench",
        "version": "0.1.0",
        "generated_at": datetime.datetime.now(timezone.utc).isoformat(),
        "engine": "mysql",
        "scale_factor": float(args.sf),
        "total_time": total_time,
        "results": results,
    }

    with open(output_path, "w") as f:
        json.dump(suite_data, f, indent=2)

    print(f"\n[SUCCESS] MySQL benchmark results saved to {output_path}")


if __name__ == "__main__":
    main()
