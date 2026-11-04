#!/usr/bin/env python3
import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession

from run_geomesa_cluster_opt import QUERIES, QueryTimeoutException, _timeout_handler, run_geomesa_query

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', type=int, required=True, choices=[1, 2, 4, 8, 16])
    parser.add_argument('--queries', default='q1,q3,q5,q7,q12')
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--output-dir', default='/home/ubuntu/results')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs('/home/ubuntu/logs', exist_ok=True)
    log_file = os.path.join('/home/ubuntu/logs', f'geomesa_scalability_nodes_{args.nodes}.log')
    result_file = os.path.join(args.output_dir, f'geomesa_cluster_sf100_nodes_{args.nodes}.json')

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])
    logging.info(f'Starting GeoMesa Scalability: Nodes={args.nodes}')

    num_partitions = str(max(16, args.nodes * 8))
    builder = SparkSession.builder \
        .appName(f"GeoMesaScalability_Nodes{args.nodes}") \
        .config("spark.cores.max", str(args.nodes * 8)) \
        .config("spark.executor.cores", "8") \
        .config("spark.executor.memory", "20g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator") \
        .config("spark.sql.shuffle.partitions", num_partitions) \
        .config("spark.default.parallelism", num_partitions) \
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")

    spark = builder.getOrCreate()
    spark._jvm.org.locationtech.geomesa.spark.sql.SQLTypes.init(spark._jsparkSession.sqlContext())
    logging.info("GeoMesa SQL functions registered successfully.")

    tables = ["building", "customer", "driver", "trip", "vehicle", "zone"]
    for t in tables:
        path = f"hdfs://master:9000/data/sf100/{t}"
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(t)
    logging.info("All HDFS tables registered successfully.")

    queries = dict(QUERIES)
    target_queries = [q.strip().lower() for q in args.queries.split(',') if q.strip()]
    results = []

    for q_name in target_queries:
        if q_name not in queries:
            continue
        sql = queries[q_name]
        logging.info(f'--- Running {q_name.upper()} on {args.nodes} nodes ---')
        job_group = f'geomesa_{q_name}_nodes_{args.nodes}'
        res = run_geomesa_query(spark, sql, timeout=args.timeout, job_group=job_group)
        entry = {'query': q_name, 'nodes': args.nodes}
        if res['success']:
            logging.info(f'  SUCCESS: {res["duration"]:.3f}s (Rows: {res.get("count")})')
            entry['status'] = 'SUCCESS'
            entry['mean'] = round(res['duration'], 4)
            entry['run_1'] = round(res['duration'], 4)
        elif res['timeout']:
            logging.warning(f'  TIMEOUT after {args.timeout}s')
            entry['status'] = 'TIMEOUT'
            entry['mean'] = None
            entry['error'] = res['error']
        else:
            logging.error(f'  FAILED: {res["error"]}')
            entry['status'] = 'FAILED'
            entry['mean'] = None
            entry['error'] = res['error']
        results.append(entry)

    spark.stop()

    with open(result_file, 'w') as f:
        json.dump(results, f, indent=2)
    logging.info(f'GeoMesa {args.nodes}-node run complete: {result_file}')

if __name__ == '__main__':
    main()
