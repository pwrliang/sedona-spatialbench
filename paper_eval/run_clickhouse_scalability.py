#!/usr/bin/env python3
import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Add spatialbench queries to path
for p in [Path('/home/ubuntu/spatialbench-queries'), Path(__file__).resolve().parent / 'spatialbench-queries']:
    if p.exists():
        sys.path.insert(0, str(p))
        break
from print_queries import ClickHouseSpatialBenchBenchmark

TABLES = ['building', 'customer', 'driver', 'trip', 'vehicle', 'zone']

def get_cluster_views(cluster_name, sf='sf100'):
    views = []
    for t in TABLES:
        src = f"SELECT * FROM hdfsCluster('{cluster_name}', 'hdfs://master:9000/data/{sf}/{t}/{t}.*.parquet', 'Parquet')"
        views.append(f"CREATE OR REPLACE VIEW {t} AS {src};")
    return "\n".join(views)

def run_clickhouse_query(sql, timeout=600, port=9010, extra_settings=None):
    settings_prefix = ""
    if extra_settings:
        for k, v in extra_settings.items():
            settings_prefix += f"SET {k} = {v};\n"
    full_sql = settings_prefix + sql
    cmd = ['clickhouse-client', '--port', str(port), '--multiquery', '--time', '--format', 'Null']
    start_time = time.perf_counter()
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate(input=full_sql, timeout=timeout)
        duration = time.perf_counter() - start_time
        if proc.returncode != 0:
            return {'success': False, 'duration': duration, 'error': stderr.strip() or stdout.strip(), 'timeout': False}
        time_val = duration
        for line in stderr.splitlines():
            line = line.strip()
            try:
                val = float(line)
                if val > 0:
                    time_val = val
            except ValueError:
                pass
        return {'success': True, 'duration': time_val, 'error': None, 'timeout': False}
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        return {'success': False, 'duration': timeout, 'error': f'Query timed out after {timeout} seconds', 'timeout': True}
    except Exception as e:
        return {'success': False, 'duration': time.perf_counter() - start_time, 'error': str(e), 'timeout': False}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', type=int, required=True, choices=[1, 2, 4, 8, 16])
    parser.add_argument('--queries', default='q1,q2,q3,q5,q7')
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--output-dir', default='/home/ubuntu/results')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    cluster_name = f'spatial_cluster_{args.nodes}'
    result_file = os.path.join(args.output_dir, f'clickhouse_cluster_sf100_nodes_{args.nodes}.json')
    log_file = os.path.join('/home/ubuntu/logs', f'clickhouse_scalability_nodes_{args.nodes}.log')
    os.makedirs('/home/ubuntu/logs', exist_ok=True)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])
    logging.info(f'Starting ClickHouse Scalability: Nodes={args.nodes}, Cluster={cluster_name}')

    init_sql = get_cluster_views(cluster_name)
    init_res = run_clickhouse_query(init_sql, timeout=60)
    if not init_res['success']:
        logging.error(f'Failed to init views: {init_res["error"]}')
        sys.exit(1)

    extra_settings = {
        'max_threads': 16,
        'max_execution_time': args.timeout,
        'join_algorithm': "'grace_hash,hash'",
        'max_bytes_before_external_join': 10000000000,
        'max_bytes_before_external_group_by': 10000000000,
        'distributed_product_mode': "'local'",
    }

    benchmark = ClickHouseSpatialBenchBenchmark()
    all_queries = benchmark.queries()
    target_queries = [q.strip().lower() for q in args.queries.split(',') if q.strip()]

    results = []
    for q_name in target_queries:
        sql = all_queries[q_name].strip().rstrip(';') + ';'
        logging.info(f'--- Running {q_name.upper()} on {args.nodes} nodes ---')
        res = run_clickhouse_query(sql, timeout=args.timeout, extra_settings=extra_settings)
        entry = {'query': q_name, 'nodes': args.nodes}
        if res['success']:
            logging.info(f'  SUCCESS: {res["duration"]:.3f}s')
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

    with open(result_file, 'w') as f:
        json.dump(results, f, indent=2)
    logging.info(f'ClickHouse {args.nodes}-node run complete: {result_file}')

if __name__ == '__main__':
    main()
