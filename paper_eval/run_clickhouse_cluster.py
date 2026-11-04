#!/usr/bin/env python3
"""SpatialBench Runner for ClickHouse Cluster (Distributed) on HDFS.

Evaluates ClickHouse Cluster (17 nodes: 1 master, 16 workers) on SpatialBench SF100
under both Default and Optimized settings.
"""

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
for p in [
    Path(__file__).resolve().parent / "spatialbench-queries",
    Path(__file__).resolve().parent.parent / "spatialbench-queries",
    Path("/home/ubuntu/spatialbench-queries"),
]:
    if p.exists():
        sys.path.insert(0, str(p))
        break
from print_queries import ClickHouseSpatialBenchBenchmark

TABLES = ["building", "customer", "driver", "trip", "vehicle", "zone"]


def get_cluster_views(hdfs_host="master", hdfs_port=9000, sf="sf100"):
    views = []
    for t in TABLES:
        src = f"SELECT * FROM hdfsCluster('spatial_cluster', 'hdfs://{hdfs_host}:{hdfs_port}/data/{sf}/{t}/{t}.*.parquet', 'Parquet')"
        views.append(f"CREATE OR REPLACE VIEW {t} AS {src};")
    return "\n".join(views)


def run_clickhouse_query(sql, timeout=600, port=9010, extra_settings=None):
    """Executes a query against ClickHouse cluster via clickhouse-client."""
    settings_prefix = ""
    if extra_settings:
        for k, v in extra_settings.items():
            settings_prefix += f"SET {k} = {v};\n"

    full_sql = settings_prefix + sql
    cmd = [
        "clickhouse-client",
        "--port",
        str(port),
        "--multiquery",
        "--time",
        "--format",
        "Null",
    ]

    start_time = time.perf_counter()
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = proc.communicate(input=full_sql, timeout=timeout)
        duration = time.perf_counter() - start_time

        if proc.returncode != 0:
            return {
                "success": False,
                "duration": duration,
                "error": stderr.strip() or stdout.strip(),
                "timeout": False,
            }

        # Extract timing from clickhouse-client output if available
        # clickhouse-client --time prints elapsed seconds to stderr
        time_val = duration
        for line in stderr.splitlines():
            line = line.strip()
            try:
                val = float(line)
                if val > 0:
                    time_val = val
            except ValueError:
                pass

        return {
            "success": True,
            "duration": time_val,
            "error": None,
            "timeout": False,
        }

    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        return {
            "success": False,
            "duration": timeout,
            "error": f"Query timed out after {timeout} seconds",
            "timeout": True,
        }
    except Exception as e:
        return {
            "success": False,
            "duration": time.perf_counter() - start_time,
            "error": str(e),
            "timeout": False,
        }


def main():
    parser = argparse.ArgumentParser(description="ClickHouse Cluster SpatialBench Runner")
    parser.add_argument("--scale-factor", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--setting", choices=["default", "optimized"], default="default")
    parser.add_argument("--port", type=int, default=9010)
    parser.add_argument("--output-dir", type=str, default="paper_eval/results")
    parser.add_argument("--log-dir", type=str, default="paper_eval/logs")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(
        args.log_dir,
        f"clickhouse_cluster_sf{args.scale_factor}_{args.setting}_{timestamp}.log",
    )
    result_file = os.path.join(
        args.output_dir,
        f"clickhouse_cluster_sf{args.scale_factor}_{args.setting}.json",
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    logging.info(
        f"Starting ClickHouse Cluster Benchmark: SF={args.scale_factor}, Setting={args.setting}"
    )

    # 1. Initialize views
    logging.info("Initializing distributed views pointing to HDFS...")
    init_views_sql = get_cluster_views(sf=f"sf{args.scale_factor}")
    init_res = run_clickhouse_query(init_views_sql, timeout=60, port=args.port)
    if not init_res["success"]:
        logging.error(f"Failed to create views: {init_res['error']}")
        sys.exit(1)
    logging.info("Distributed views initialized successfully.")

    # 2. Configure query settings
    extra_settings = {}
    if args.setting == "optimized":
        extra_settings = {
            "max_threads": 16,
            "max_execution_time": args.timeout,
            "join_algorithm": "'grace_hash,hash'",
            "max_bytes_before_external_join": 10000000000,
            "max_bytes_before_external_group_by": 10000000000,
            "distributed_product_mode": "'local'",
        }

    # 3. Load queries
    benchmark = ClickHouseSpatialBenchBenchmark()
    queries = benchmark.queries()

    # Apply dialect fix for Q12 if running in optimized mode
    if args.setting == "optimized":
        queries["q12"] = queries["q12"].replace(
            "SELECT\n   t.t_tripkey,\n   AVG(b.dist)",
            "SELECT\n   ranked.t_tripkey,\n   AVG(ranked.dist)",
        )

    results = []

    for q_name in [f"q{i}" for i in range(1, 13)]:
        sql = queries[q_name].strip().rstrip(";") + ";"
        logging.info(f"--- Running {q_name.upper()} ({args.setting}) ---")

        run_times = []
        status = "SUCCESS"
        err_msg = None

        for iteration in range(1, args.iterations + 1):
            logging.info(f"  Iteration {iteration}/{args.iterations}...")
            res = run_clickhouse_query(
                sql, timeout=args.timeout, port=args.port, extra_settings=extra_settings
            )

            if res["success"]:
                logging.info(f"    Completed in {res['duration']:.3f}s")
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

    # 4. Save results
    with open(result_file, "w") as f:
        json.dump(results, f, indent=2)
    logging.info(f"Benchmark finished. Results saved to {result_file}")


if __name__ == "__main__":
    main()
