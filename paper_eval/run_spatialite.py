#!/usr/bin/env python3
"""
SpatialBench Benchmark Runner for SpatiaLite.
Evaluates Q1-Q12 with per-query timeouts (600s), SQLite interrupt handling,
and structured JSON output.
"""

import argparse
import json
import os
import signal
import sqlite3
import statistics
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

QUERIES = {
    "q1": """
SELECT
   t.t_tripkey,
   ST_X(GeomFromWKB(t.t_pickuploc, 4326)) AS pickup_lon,
   ST_Y(GeomFromWKB(t.t_pickuploc, 4326)) AS pickup_lat,
   t.t_pickuptime,
   ST_Distance(GeomFromWKB(t.t_pickuploc, 4326), MakePoint(-111.7610, 34.8697, 4326)) AS distance_to_center
FROM trip t
WHERE ST_Distance(GeomFromWKB(t.t_pickuploc, 4326), MakePoint(-111.7610, 34.8697, 4326)) <= 0.45
ORDER BY distance_to_center ASC, t.t_tripkey ASC
LIMIT 100
    """,
    "q2": """
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(GeomFromWKB(t.t_pickuploc, 4326), (SELECT GeomFromWKB(z.z_boundary, 4326) FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1))
    """,
    "q3": """
SELECT
   strftime('%Y-%m-01', t.t_pickuptime) AS pickup_month,
   COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_distance) AS avg_distance,
   AVG(strftime('%s', t.t_dropofftime) - strftime('%s', t.t_pickuptime)) AS avg_duration,
   AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_Distance(
   GeomFromWKB(t.t_pickuploc, 4326),
   GeomFromText('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))', 4326)
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
) top_trips ON ST_Within(GeomFromWKB(top_trips.t_pickuploc, 4326), GeomFromWKB(z.z_boundary, 4326))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
    """,
    "q5": """
SELECT
   c.c_custkey,
   c.c_name AS customer_name,
   strftime('%Y-%m-01', t.t_pickuptime) AS pickup_month,
   ST_Area(ST_ConvexHull(Collect(GeomFromWKB(t.t_dropoffloc, 4326)))) AS monthly_travel_hull_area,
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
   AVG(strftime('%s', t.t_dropofftime) - strftime('%s', t.t_pickuptime)) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))', 4326), GeomFromWKB(z.z_boundary, 4326))
  AND ST_Within(GeomFromWKB(t.t_pickuploc, 4326), GeomFromWKB(z.z_boundary, 4326))
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
    """,
    "q7": """
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       CAST(t.t_distance AS REAL) AS reported_distance_m,
       ST_Length(MakeLine(GeomFromWKB(t.t_pickuploc, 4326), GeomFromWKB(t.t_dropoffloc, 4326))) / 0.000009 AS line_distance_m
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
JOIN building b ON ST_Distance(GeomFromWKB(t.t_pickuploc, 4326), GeomFromWKB(b.b_boundary, 4326)) <= 0.0045
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
LIMIT 100
    """,
    "q9": """
WITH pairs AS (
   SELECT
       b1.b_buildingkey AS building_1,
       b2.b_buildingkey AS building_2,
       ST_Area(GeomFromWKB(b1.b_boundary, 4326)) AS area1,
       ST_Area(GeomFromWKB(b2.b_boundary, 4326)) AS area2,
       ST_Area(ST_Intersection(GeomFromWKB(b1.b_boundary, 4326), GeomFromWKB(b2.b_boundary, 4326))) AS overlap_area
   FROM building b1
   JOIN building b2 ON b1.b_buildingkey < b2.b_buildingkey
                   AND ST_Intersects(GeomFromWKB(b1.b_boundary, 4326), GeomFromWKB(b2.b_boundary, 4326))
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
   AVG(strftime('%s', t.t_dropofftime) - strftime('%s', t.t_pickuptime)) AS avg_duration,
   AVG(t.t_distance) AS avg_distance,
   COUNT(t.t_tripkey) AS num_trips
FROM zone z
LEFT JOIN trip t ON ST_Within(GeomFromWKB(t.t_pickuploc, 4326), GeomFromWKB(z.z_boundary, 4326))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC, z.z_zonekey ASC
LIMIT 100
    """,
    "q11": """
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON ST_Within(GeomFromWKB(t.t_pickuploc, 4326), GeomFromWKB(pickup_zone.z_boundary, 4326))
JOIN zone dropoff_zone ON ST_Within(GeomFromWKB(t.t_dropoffloc, 4326), GeomFromWKB(dropoff_zone.z_boundary, 4326))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
    """,
    "q12": """
SELECT
   t.t_tripkey,
   (
       SELECT AVG(nb.distance_to_building)
       FROM (
           SELECT ST_Distance(GeomFromWKB(t.t_pickuploc, 4326), GeomFromWKB(b.b_boundary, 4326)) AS distance_to_building
           FROM building b
           ORDER BY distance_to_building ASC
           LIMIT 5
       ) nb
   ) AS avg_distance_to_5_nearest
FROM trip t
ORDER BY avg_distance_to_5_nearest DESC, t.t_tripkey ASC
LIMIT 100
    """,
}


class QueryTimeoutException(Exception):
    pass


def _timeout_handler(signum, frame):
    raise QueryTimeoutException("Query execution timed out")


def main():
    parser = argparse.ArgumentParser(description="Run SpatialBench on SpatiaLite")
    parser.add_argument("--db-path", default="/local/scratch/liang/spatialite_sf1.db", help="Path to SQLite/SpatiaLite DB")
    parser.add_argument("--sf", type=str, default="1", help="Scale factor (1)")
    parser.add_argument("--runs", type=int, default=3, help="Repetitions per query")
    parser.add_argument("--timeout", type=int, default=600, help="Per-run timeout in seconds")
    parser.add_argument("--queries", default="all", help="Comma-separated query names or 'all'")
    parser.add_argument("--output", default=None, help="Output JSON path")
    args = parser.parse_args()

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = REPO_ROOT / "paper_eval" / "results" / f"spatialite_sf{args.sf}_default.json"

    print("=" * 70)
    print(" SpatiaLite Benchmark Runner")
    print(f" Database     : {args.db_path}")
    print(f" Scale Factor : SF{args.sf}")
    print(f" Repetitions  : {args.runs}")
    print(f" Timeout      : {args.timeout}s")
    print(f" Queries      : {args.queries}")
    print(f" Output Path  : {output_path}")
    print("=" * 70)

    # Initialize SpatiaLite connection
    os.environ["TMPDIR"] = "/local/scratch/liang/tmp"
    con = sqlite3.connect(args.db_path)
    con.enable_load_extension(True)
    con.load_extension("mod_spatialite")
    con.execute("PRAGMA temp_store_directory = '/local/scratch/liang/tmp'")
    con.execute("PRAGMA cache_size = -4000000")  # 4 GB cache
    con.execute("PRAGMA mmap_size = 30000000000") # 30 GB mmap

    if args.queries == "all":
        selected_queries = list(QUERIES.keys())
    else:
        selected_queries = [q.strip().lower() for q in args.queries.split(",") if q.strip()]

    results = []
    if output_path.exists():
        try:
            with open(output_path, "r") as f:
                existing_data = json.load(f)
                results = existing_data.get("results", [])
        except Exception:
            pass

    existing_query_names = {r.get("query") for r in results if r.get("status") == "success"}

    total_start = time.perf_counter()

    for q_name in selected_queries:
        if q_name not in QUERIES:
            print(f"Skipping unknown query: {q_name}")
            continue
        if q_name in existing_query_names:
            print(f"Skipping already completed query: {q_name.upper()}")
            continue

        q_sql = QUERIES[q_name]
        print(f"\n--- Running {q_name.upper()} ({args.runs} runs) ---")
        run_times = []
        last_row_count = None
        error_msg = None
        status = "success"

        for run_idx in range(args.runs):
            timer = threading.Timer(args.timeout, con.interrupt)
            timer.daemon = True
            timer.start()

            try:
                t0 = time.perf_counter()
                cur = con.execute(q_sql)
                rows = cur.fetchall()
                elapsed = time.perf_counter() - t0
                timer.cancel()
                run_times.append(round(elapsed, 4))
                last_row_count = len(rows)
                print(f"  Run {run_idx + 1}/{args.runs}: {elapsed:.3f}s ({last_row_count} rows)")
            except sqlite3.OperationalError as e:
                timer.cancel()
                if "interrupted" in str(e).lower() or isinstance(e, QueryTimeoutException):
                    print(f"  Run {run_idx + 1}/{args.runs}: TIMEOUT (> {args.timeout}s)")
                    status = "timeout"
                    error_msg = f"Query timed out after {args.timeout} seconds"
                else:
                    print(f"  Run {run_idx + 1}/{args.runs}: FAILED - {e}")
                    status = "failed"
                    error_msg = str(e)
                break
            except Exception as e:
                timer.cancel()
                print(f"  Run {run_idx + 1}/{args.runs}: FAILED - {e}")
                status = "failed"
                error_msg = str(e)
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
            "engine": "spatialite",
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

        # Save incremental results
        output_path.parent.mkdir(parents=True, exist_ok=True)
        suite_data = {
            "benchmark": "spatialbench",
            "version": "0.1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "engine": "spatialite",
            "scale_factor": float(args.sf),
            "total_time": round(time.perf_counter() - total_start, 2),
            "results": results,
        }
        with open(output_path, "w") as f:
            json.dump(suite_data, f, indent=2)

    con.close()
    print(f"\n[SUCCESS] SpatiaLite benchmark results saved to {output_path}")


if __name__ == "__main__":
    main()
