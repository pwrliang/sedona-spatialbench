#!/usr/bin/env python3
import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

# Trino Queries
from run_trino_cluster import QUERIES

def set_trino_cluster_size(num_nodes, timeout=90):
    logging.info(f'Configuring Trino cluster to {num_nodes} worker nodes...')
    # 1. Stop all workers
    stop_cmd = "for w in $(cat /opt/spark/conf/workers); do ssh -o StrictHostKeyChecking=no $w 'JAVA_HOME=/home/ubuntu/trino/jdk-25.0.5+8 /home/ubuntu/trino/trino-server-484/bin/launcher stop 2>/dev/null || true' & done; wait"
    subprocess.run(stop_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)

    # 2. Start target workers
    start_cmd = f"for w in $(head -n {num_nodes} /opt/spark/conf/workers); do ssh -o StrictHostKeyChecking=no $w 'JAVA_HOME=/home/ubuntu/trino/jdk-25.0.5+8 /home/ubuntu/trino/trino-server-484/bin/launcher start' & done; wait"
    subprocess.run(start_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # 3. Poll Trino coordinator until num_nodes are active
    start_t = time.time()
    while time.time() - start_t < timeout:
        check_cmd = ['/home/ubuntu/trino/trino', '--server', 'http://127.0.0.1:8085', '--output-format', 'CSV', '--execute', "SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false AND state = 'active'"]
        res = subprocess.run(check_cmd, capture_output=True, text=True)
        if res.returncode == 0:
            out = res.stdout.strip().strip('"')
            try:
                cnt = int(out)
                logging.info(f'Trino active workers: {cnt}/{num_nodes}')
                if cnt == num_nodes:
                    logging.info(f'Trino cluster successfully scaled to {num_nodes} workers!')
                    return True
            except ValueError:
                pass
        time.sleep(3)
    logging.error(f'Timeout waiting for Trino to reach {num_nodes} workers!')
    return False

def run_trino_query(sql, timeout=600, host="127.0.0.1", port=8085):
    session_properties = {
        "join_distribution_type": "AUTOMATIC",
        "max_hash_partition_count": "128",
        "min_hash_partition_count": "16",
        "task_concurrency": "8",
    }
    cmd = [
        "/home/ubuntu/trino/trino",
        "--server", f"http://{host}:{port}",
        "--output-format", "CSV",
        "--progress",
    ]
    for prop, val in session_properties.items():
        cmd.extend(["--session", f"{prop}={val}"])
    cmd.extend(["--execute", sql])

    start_time = time.perf_counter()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        duration = time.perf_counter() - start_time
        if proc.returncode == 0:
            return {"success": True, "duration": duration, "output": proc.stdout[:200], "error": None, "timeout": False}
        else:
            return {"success": False, "duration": duration, "output": None, "error": proc.stderr.strip() or proc.stdout.strip(), "timeout": False}
    except subprocess.TimeoutExpired:
        return {"success": False, "duration": None, "output": None, "error": f"Query timed out after {timeout} seconds", "timeout": True}
    except Exception as e:
        return {"success": False, "duration": None, "output": None, "error": str(e), "timeout": False}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', type=int, required=True, choices=[1, 2, 4, 8, 16])
    parser.add_argument('--queries', default='q1,q2,q3,q4,q5,q6,q7,q8,q9')
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--output-dir', default='/home/ubuntu/results')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs('/home/ubuntu/logs', exist_ok=True)
    log_file = os.path.join('/home/ubuntu/logs', f'trino_scalability_nodes_{args.nodes}.log')
    result_file = os.path.join(args.output_dir, f'trino_cluster_sf100_nodes_{args.nodes}.json')

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])
    logging.info(f'Starting Trino Scalability: Nodes={args.nodes}')

    if not set_trino_cluster_size(args.nodes):
        logging.error(f'Failed to scale Trino cluster to {args.nodes} nodes.')
        sys.exit(1)

    target_queries = [q.strip().lower() for q in args.queries.split(',') if q.strip()]
    results = []

    for q_name in target_queries:
        sql = QUERIES[q_name]
        logging.info(f'--- Running {q_name.upper()} on {args.nodes} nodes ---')
        res = run_trino_query(sql, timeout=args.timeout)
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
    logging.info(f'Trino {args.nodes}-node run complete: {result_file}')

if __name__ == '__main__':
    main()
