#!/usr/bin/env python3
import argparse
import json
import logging
import os
import signal
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

from run_sedona_cluster_optimized import QUERIES, QueryTimeoutException, _timeout_handler

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', type=int, required=True, choices=[1, 2, 4, 8, 16])
    parser.add_argument('--queries', default='q1,q3,q4,q5,q6,q7')
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--output-dir', default='/home/ubuntu/results')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs('/home/ubuntu/logs', exist_ok=True)
    log_file = os.path.join('/home/ubuntu/logs', f'sedona_scalability_nodes_{args.nodes}.log')
    result_file = os.path.join(args.output_dir, f'sedona_cluster_sf100_nodes_{args.nodes}.json')

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])
    logging.info(f'Starting Sedona Scalability: Nodes={args.nodes}')

    num_partitions = str(max(16, args.nodes * 16))
    config = (
        SedonaContext.builder()
        .master("spark://master:7077")
        .appName(f"SedonaScalability_Nodes{args.nodes}")
        .config("spark.jars", "/opt/spark-3.5.9-bin-hadoop3/sedona-jars/sedona-spark-shaded-3.5_2.12-1.9.1.jar,/opt/spark-3.5.9-bin-hadoop3/sedona-jars/geotools-wrapper-1.9.1-33.5.jar")
        .config("spark.cores.max", str(args.nodes * 8))
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
    sc = spark.sparkContext
    logging.info(f'Connected to Spark Master: {sc.master}, Total Cores: {sc.defaultParallelism}')

    tables = ["building", "customer", "driver", "trip", "vehicle", "zone"]
    for tbl in tables:
        tbl_path = f"hdfs://master:9000/data/sf100/{tbl}"
        df = spark.read.parquet(tbl_path)
        df.createOrReplaceTempView(tbl)
    logging.info("All HDFS tables registered successfully.")

    target_queries = [q.strip().lower() for q in args.queries.split(',') if q.strip()]
    results = []

    for q_name in target_queries:
        if q_name not in QUERIES:
            continue
        q_sql = QUERIES[q_name]
        logging.info(f'--- Running {q_name.upper()} on {args.nodes} nodes ---')
        job_group_id = f'sedona_{q_name}_nodes_{args.nodes}'
        sc.setJobGroup(job_group_id, f'Sedona {q_name} ({args.nodes} nodes)')
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(args.timeout)

        entry = {'query': q_name, 'nodes': args.nodes}
        try:
            t0 = time.perf_counter()
            df_res = spark.sql(q_sql)
            collected = df_res.collect()
            elapsed = time.perf_counter() - t0
            signal.alarm(0)
            logging.info(f'  SUCCESS: {elapsed:.3f}s ({len(collected)} rows)')
            entry['status'] = 'SUCCESS'
            entry['mean'] = round(elapsed, 4)
            entry['run_1'] = round(elapsed, 4)
        except QueryTimeoutException:
            signal.alarm(0)
            sc.cancelJobGroup(job_group_id)
            logging.warning(f'  TIMEOUT after {args.timeout}s')
            entry['status'] = 'TIMEOUT'
            entry['mean'] = None
            entry['error'] = f'Timed out after {args.timeout}s'
        except Exception as e:
            signal.alarm(0)
            sc.cancelJobGroup(job_group_id)
            logging.error(f'  FAILED: {e}')
            entry['status'] = 'FAILED'
            entry['mean'] = None
            entry['error'] = str(e)

        results.append(entry)

    spark.stop()

    with open(result_file, 'w') as f:
        json.dump(results, f, indent=2)
    logging.info(f'Sedona {args.nodes}-node run complete: {result_file}')

if __name__ == '__main__':
    main()
