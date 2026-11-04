#!/usr/bin/env python3
"""
Thrust 1: Weak Scalability Sweep on AWS 17-Node Cluster.
Constant Workload per Worker: ~37.5M records per 8-core worker (SF 6.25/worker).
- 1 Node:  SF =  6.25 ( 10 files,   8 cores)
- 2 Nodes: SF = 12.5  ( 19 files,  16 cores)
- 4 Nodes: SF = 25.0  ( 38 files,  32 cores)
- 8 Nodes: SF = 50.0  ( 77 files,  64 cores)
- 16 Nodes: SF = 100.0 (154 files, 128 cores)
Evaluated Engines:
- Apache Sedona
- Trino Cluster
- GeoMesa
- ClickHouse Cluster
Metrics:
- Query Wall-Clock Latency T_N
- Weak Scaling Efficiency: E_weak = (T_1 / T_N) * 100%
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

RESULTS_DIR = Path("paper_eval/results")
PLOTS_DIR = Path("paper_eval/plots")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def generate_weak_scalability_script():
    """Generates the remote orchestration script to be executed on sedona-master."""
    script_content = '''#!/usr/bin/env python3
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

LOG_FILE = "/home/ubuntu/logs/weak_scalability.log"
os.makedirs("/home/ubuntu/logs", exist_ok=True)
os.makedirs("/home/ubuntu/results", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])

NODES = [1, 2, 4, 8, 16]
# Base files per node (154 total files / 16 nodes ~= 9.6 files/node)
FILES_PER_NODE = {
    1: 10,
    2: 19,
    4: 38,
    8: 77,
    16: 154
}

def run_sedona_weak():
    logging.info("=== Starting Sedona Weak Scalability Sweep ===")
    from pyspark.sql import SparkSession
    from sedona.spark import SedonaContext

    # We evaluate selective and compute queries that succeed on 16 nodes
    queries = {
        "q1": """
        SELECT t.t_tripkey, ST_X(ST_Point(t.x, t.y)) AS pickup_lon, ST_Y(ST_Point(t.x, t.y)) AS pickup_lat, t.t_totalamount, t.t_distance
        FROM trip t
        WHERE ST_Distance(ST_Point(-111.7635, 34.8697), ST_Point(t.x, t.y)) <= 0.45
        ORDER BY ST_Distance(ST_Point(-111.7635, 34.8697), ST_Point(t.x, t.y)) ASC
        LIMIT 100
        """,
        "q3": """
        SELECT DATE_TRUNC('month', t.t_pickuptime) AS pickup_month, COUNT(t.t_tripkey) AS total_trips, AVG(t.t_totalamount) AS avg_amount, AVG(t.t_distance) AS avg_distance
        FROM trip t
        WHERE ST_Intersects(ST_Buffer(ST_GeomFromText('POLYGON((-111.85 34.80, -111.70 34.80, -111.70 34.95, -111.85 34.95, -111.85 34.80))'), 0.05), ST_Point(t.x, t.y))
        GROUP BY DATE_TRUNC('month', t.t_pickuptime)
        ORDER BY pickup_month
        """,
        "q5": """
        SELECT c.c_custkey, c.c_name, DATE_TRUNC('month', t.t_pickuptime) AS trip_month, COUNT(t.t_tripkey) AS trip_count,
               ST_ConvexHull(ST_Collect(ST_Point(t.dropoff_x, t.dropoff_y))) AS dropoff_convex_hull
        FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
        WHERE t.t_pickuptime >= TIMESTAMP '1995-01-01 00:00:00' AND t.t_pickuptime < TIMESTAMP '1996-01-01 00:00:00'
        GROUP BY c.c_custkey, c.c_name, DATE_TRUNC('month', t.t_pickuptime)
        HAVING COUNT(t.t_tripkey) >= 10
        ORDER BY trip_count DESC, c.c_custkey ASC, trip_month ASC
        LIMIT 100
        """,
        "q7": """
        SELECT COUNT(*) AS total_trips,
               COUNT(CASE WHEN straight_dist > 0 AND (t_distance / straight_dist) > 2.0 THEN 1 END) AS detour_trips,
               AVG(CASE WHEN straight_dist > 0 THEN t_distance / straight_dist END) AS avg_detour_ratio
        FROM (
            SELECT t.t_tripkey, t.t_distance, ST_Distance(ST_Point(t.x, t.y), ST_Point(t.dropoff_x, t.dropoff_y)) AS straight_dist
            FROM trip t WHERE t.t_distance > 0
        )
        """
    }

    # Get list of HDFS trip parquet files
    cmd = "/opt/hadoop/bin/hdfs dfs -ls /data/sf100/trip | grep '\\.parquet' | awk '{print $8}'"
    all_files = subprocess.check_output(cmd, shell=True).decode().strip().split()
    all_files.sort()

    results = {}

    for n in NODES:
        n_files = FILES_PER_NODE[n]
        selected_files = all_files[:n_files]
        logging.info(f">> Running Sedona on {n} nodes ({n_files} files, {n*8} cores)...")

        config = (
            SedonaContext.builder()
            .master("spark://master:7077")
            .appName(f"SedonaWeakScaling_{n}Nodes")
            .config("spark.jars", "/opt/spark-3.5.9-bin-hadoop3/sedona-jars/sedona-spark-shaded-3.5_2.12-1.9.1.jar,/opt/spark-3.5.9-bin-hadoop3/sedona-jars/geotools-wrapper-1.9.1-33.5.jar")
            .config("spark.cores.max", str(n * 8))
            .config("spark.executor.cores", "8")
            .config("spark.executor.memory", "26g")
            .config("spark.driver.memory", "16g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", str(max(16, n * 16)))
            .getOrCreate()
        )
        spark = SedonaContext.create(config)

        # Register dimension tables
        for tbl in ["building", "customer", "zone"]:
            spark.read.parquet(f"hdfs://master:9000/data/sf100/{tbl}").createOrReplaceTempView(tbl)

        # Register trip with weak-scaled file list
        df_trip = spark.read.parquet(*selected_files)
        df_trip.createOrReplaceTempView("trip")

        results[n] = {}
        for q_name, sql in queries.items():
            t0 = time.perf_counter()
            try:
                rows = spark.sql(sql).collect()
                elapsed = time.perf_counter() - t0
                logging.info(f"   Sedona {q_name} on {n} nodes: {elapsed:.3f}s ({len(rows)} rows)")
                results[n][q_name] = round(elapsed, 3)
            except Exception as e:
                logging.error(f"   Sedona {q_name} on {n} nodes failed: {e}")
                results[n][q_name] = None

        spark.stop()

    with open("/home/ubuntu/results/weak_scalability_sedona.json", "w") as f:
        json.dump(results, f, indent=2)
    logging.info("Sedona weak scalability complete.")

if __name__ == "__main__":
    run_sedona_weak()
'''
    return script_content

if __name__ == "__main__":
    Path("paper_eval/remote_weak_scaling.py").write_text(generate_weak_scalability_script())
    print("Generated remote weak scalability script.")
