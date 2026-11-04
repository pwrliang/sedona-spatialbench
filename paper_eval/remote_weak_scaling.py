#!/usr/bin/env python3
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext
from run_sedona_cluster_optimized import QUERIES

LOG_FILE = "/home/ubuntu/logs/weak_scalability.log"
os.makedirs("/home/ubuntu/logs", exist_ok=True)
os.makedirs("/home/ubuntu/results", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])

NODES = [1, 2, 4, 8, 16]
FILES_PER_NODE = {
    1: 10,
    2: 19,
    4: 38,
    8: 77,
    16: 154
}

TARGET_QUERIES = ["q1", "q3", "q4", "q5", "q6", "q7"]

def run_sedona_weak():
    logging.info("=== Starting Sedona Weak Scalability Sweep ===")

    cmd = "/opt/hadoop/bin/hdfs dfs -ls /data/sf100/trip | grep '\\.parquet' | awk '{print $8}'"
    all_raw = subprocess.check_output(cmd, shell=True).decode().strip().split()
    all_files = [f"hdfs://master:9000{f}" for f in sorted(all_raw)]

    results = {}

    for n in NODES:
        n_files = FILES_PER_NODE[n]
        selected_files = all_files[:n_files]
        logging.info(f">> Running Sedona Weak Scalability on {n} nodes ({n_files} files, {n*8} cores)...")

        num_partitions = str(max(16, n * 16))
        config = (
            SedonaContext.builder()
            .master("spark://master:7077")
            .appName(f"SedonaWeakScaling_{n}Nodes")
            .config("spark.jars", "/opt/spark-3.5.9-bin-hadoop3/sedona-jars/sedona-spark-shaded-3.5_2.12-1.9.1.jar,/opt/spark-3.5.9-bin-hadoop3/sedona-jars/geotools-wrapper-1.9.1-33.5.jar")
            .config("spark.cores.max", str(n * 8))
            .config("spark.executor.cores", "8")
            .config("spark.executor.memory", "26g")
            .config("spark.driver.memory", "16g")
            .config("spark.driver.maxResultSize", "8g")
            .config("spark.driver.host", "master")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", num_partitions)
            .config("spark.sedona.join.gridtype", "kdbtree")
            .config("spark.sedona.join.index", "true")
            .config("spark.sedona.join.numpartition", num_partitions)
            .config("spark.sedona.join.spatrans", "true")
            .getOrCreate()
        )
        spark = SedonaContext.create(config)

        # Register dimension tables
        for tbl in ["building", "customer", "driver", "vehicle", "zone"]:
            spark.read.parquet(f"hdfs://master:9000/data/sf100/{tbl}").createOrReplaceTempView(tbl)

        # Register trip with weak-scaled file list
        df_trip = spark.read.parquet(*selected_files)
        df_trip.createOrReplaceTempView("trip")

        results[n] = {}
        for q_name in TARGET_QUERIES:
            sql = QUERIES[q_name]
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
