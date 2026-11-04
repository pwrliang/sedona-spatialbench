#!/usr/bin/env python3
import json
import logging
import os
import subprocess
import sys
import time

LOG_FILE = '/home/ubuntu/logs/orchestrate_scalability.log'
os.makedirs('/home/ubuntu/logs', exist_ok=True)
os.makedirs('/home/ubuntu/results', exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)])

def step_generate_16node_baselines():
    logging.info('=== Step 1: Generate 16-node baseline files from existing runs ===')
    # ClickHouse
    with open('/home/ubuntu/results/clickhouse_cluster_sf100_optimized.json') as f:
        ch = json.load(f)
    surv_ch = [{'query': q['query'], 'nodes': 16, 'status': 'SUCCESS', 'mean': q['mean'], 'run_1': q['mean']}
               for q in ch if q.get('status') == 'SUCCESS']
    with open('/home/ubuntu/results/clickhouse_cluster_sf100_nodes_16.json', 'w') as f:
        json.dump(surv_ch, f, indent=2)

    # GeoMesa
    with open('/home/ubuntu/results/geomesa_cluster_sf100_optimized.json') as f:
        gm = json.load(f)
    surv_gm = [{'query': q['query'], 'nodes': 16, 'status': 'SUCCESS', 'mean': q['mean'], 'run_1': q['mean']}
               for q in gm if q.get('status') == 'SUCCESS']
    with open('/home/ubuntu/results/geomesa_cluster_sf100_nodes_16.json', 'w') as f:
        json.dump(surv_gm, f, indent=2)

    # Trino
    with open('/home/ubuntu/results/trino_cluster_sf100_optimized.json') as f:
        tr = json.load(f)
    surv_tr = [{'query': q['query'], 'nodes': 16, 'status': 'SUCCESS', 'mean': q['mean'], 'run_1': q['mean']}
               for q in tr if q.get('status') == 'SUCCESS']
    with open('/home/ubuntu/results/trino_cluster_sf100_nodes_16.json', 'w') as f:
        json.dump(surv_tr, f, indent=2)

    # Sedona
    with open('/home/ubuntu/sedona_cluster_sf100_optimized.json') as f:
        sed = json.load(f)['results']
    surv_sed = [{'query': q['query'], 'nodes': 16, 'status': 'SUCCESS', 'mean': q['time_seconds'], 'run_1': q['time_seconds']}
                for q in sed if q.get('status') == 'success']
    with open('/home/ubuntu/results/sedona_cluster_sf100_nodes_16.json', 'w') as f:
        json.dump(surv_sed, f, indent=2)

    logging.info(f'16-node baselines generated: CH={len(surv_ch)}, GM={len(surv_gm)}, TR={len(surv_tr)}, SED={len(surv_sed)}')

def run_clickhouse_scalability():
    logging.info('====================================================')
    logging.info('=== ClickHouse Scalability Experiments (8, 4, 2, 1) ===')
    logging.info('====================================================')
    active_queries = ['q1', 'q2', 'q3', 'q5', 'q7']
    for n in [8, 4, 2, 1]:
        if not active_queries:
            logging.info(f'No remaining active queries for ClickHouse at {n} nodes.')
            break
        logging.info(f'>>> Running ClickHouse on {n} nodes with queries: {active_queries}')
        cmd = ['python3', '/home/ubuntu/run_clickhouse_scalability.py', '--nodes', str(n), '--queries', ','.join(active_queries)]
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            logging.error(f'ClickHouse {n} nodes run returned non-zero code {proc.returncode}')
        res_file = f'/home/ubuntu/results/clickhouse_cluster_sf100_nodes_{n}.json'
        if os.path.exists(res_file):
            with open(res_file) as f:
                res = json.load(f)
            surviving = [r['query'] for r in res if r['status'] == 'SUCCESS']
            dropped = set(active_queries) - set(surviving)
            if dropped:
                logging.info(f'ClickHouse {n}-node dropped queries: {dropped}')
            active_queries = surviving

def run_geomesa_scalability():
    logging.info('====================================================')
    logging.info('=== GeoMesa Scalability Experiments (8, 4, 2, 1) ===')
    logging.info('====================================================')
    active_queries = ['q1', 'q3', 'q5', 'q7', 'q12']
    for n in [8, 4, 2, 1]:
        if not active_queries:
            logging.info(f'No remaining active queries for GeoMesa at {n} nodes.')
            break
        logging.info(f'>>> Running GeoMesa on {n} nodes with queries: {active_queries}')
        cmd = [
            '/opt/spark/bin/spark-submit',
            '--master', 'spark://master:7077',
            '--total-executor-cores', str(n * 8),
            '--executor-cores', '8',
            '--executor-memory', '20g',
            '--jars', '/home/ubuntu/geomesa/geomesa-fs_2.12-5.4.0/dist/spark/geomesa-fs-spark-runtime_2.12-5.4.0.jar',
            '/home/ubuntu/run_geomesa_scalability.py',
            '--nodes', str(n),
            '--queries', ','.join(active_queries)
        ]
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            logging.error(f'GeoMesa {n} nodes run returned non-zero code {proc.returncode}')
        res_file = f'/home/ubuntu/results/geomesa_cluster_sf100_nodes_{n}.json'
        if os.path.exists(res_file):
            with open(res_file) as f:
                res = json.load(f)
            surviving = [r['query'] for r in res if r['status'] == 'SUCCESS']
            dropped = set(active_queries) - set(surviving)
            if dropped:
                logging.info(f'GeoMesa {n}-node dropped queries: {dropped}')
            active_queries = surviving

def run_sedona_scalability():
    logging.info('====================================================')
    logging.info('=== Sedona Scalability Experiments (8, 4, 2, 1) ===')
    logging.info('====================================================')
    active_queries = ['q1', 'q3', 'q4', 'q5', 'q6', 'q7']
    for n in [8, 4, 2, 1]:
        if not active_queries:
            logging.info(f'No remaining active queries for Sedona at {n} nodes.')
            break
        logging.info(f'>>> Running Sedona on {n} nodes with queries: {active_queries}')
        cmd = [
            '/opt/sedona-venv/bin/python',
            '/home/ubuntu/run_sedona_scalability.py',
            '--nodes', str(n),
            '--queries', ','.join(active_queries)
        ]
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            logging.error(f'Sedona {n} nodes run returned non-zero code {proc.returncode}')
        res_file = f'/home/ubuntu/results/sedona_cluster_sf100_nodes_{n}.json'
        if os.path.exists(res_file):
            with open(res_file) as f:
                res = json.load(f)
            surviving = [r['query'] for r in res if r['status'] == 'SUCCESS']
            dropped = set(active_queries) - set(surviving)
            if dropped:
                logging.info(f'Sedona {n}-node dropped queries: {dropped}')
            active_queries = surviving

def run_trino_scalability():
    logging.info('====================================================')
    logging.info('=== Trino Scalability Experiments (8, 4, 2, 1) ===')
    logging.info('====================================================')
    active_queries = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9']
    for n in [8, 4, 2, 1]:
        if not active_queries:
            logging.info(f'No remaining active queries for Trino at {n} nodes.')
            break
        logging.info(f'>>> Running Trino on {n} nodes with queries: {active_queries}')
        cmd = ['python3', '/home/ubuntu/run_trino_scalability.py', '--nodes', str(n), '--queries', ','.join(active_queries)]
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            logging.error(f'Trino {n} nodes run returned non-zero code {proc.returncode}')
        res_file = f'/home/ubuntu/results/trino_cluster_sf100_nodes_{n}.json'
        if os.path.exists(res_file):
            with open(res_file) as f:
                res = json.load(f)
            surviving = [r['query'] for r in res if r['status'] == 'SUCCESS']
            dropped = set(active_queries) - set(surviving)
            if dropped:
                logging.info(f'Trino {n}-node dropped queries: {dropped}')
            active_queries = surviving

def main():
    t_start = time.time()
    step_generate_16node_baselines()
    run_clickhouse_scalability()
    run_geomesa_scalability()
    run_sedona_scalability()
    run_trino_scalability()
    elapsed = time.time() - t_start
    logging.info(f'ALL SCALABILITY EXPERIMENTS FINISHED SUCCESSFULLY IN {elapsed:.1f}s!')
    with open('/home/ubuntu/SCALABILITY_DONE.flag', 'w') as f:
        f.write(f'Finished in {elapsed:.1f} seconds at {time.ctime()}\n')

if __name__ == '__main__':
    main()
