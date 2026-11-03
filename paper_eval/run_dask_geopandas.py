#!/usr/bin/env python3
"""
SpatialBench Benchmark Runner for Dask-GeoPandas.
Evaluates Q1-Q12 on SF1 or SF10 with strict timeout and statistics.
"""

import argparse
import json
import os
import signal
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root and queries directory to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(REPO_ROOT / "spatialbench-queries"))
sys.path.insert(0, str(REPO_ROOT))

import dask_geopandas_queries as dg


class QueryTimeoutException(Exception):
    pass


def _timeout_handler(signum, frame):
    raise QueryTimeoutException("Query execution timed out")


def main():
    parser = argparse.ArgumentParser(description="Run SpatialBench on Dask-GeoPandas")
    parser.add_argument("--sf", type=str, default="1", help="Scale factor (1 or 10)")
    parser.add_argument("--data-dir", default=None, help="Directory containing parquet tables")
    parser.add_argument("--runs", type=int, default=3, help="Number of repetitions per query")
    parser.add_argument("--timeout", type=int, default=600, help="Per-run timeout in seconds")
    parser.add_argument("--queries", default="all", help="Comma-separated query names (e.g. q1,q2) or 'all'")
    parser.add_argument("--output", default=None, help="Output JSON path")
    args = parser.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir)
    elif args.sf == "1":
        data_dir = REPO_ROOT / "paper_eval" / "benchmark-data-sf1"
        if not data_dir.exists():
            data_dir = Path("/home/geng.161/.cache/huggingface/hub/datasets--apache-sedona--spatialbench/snapshots/v0.1.0/sf1")
    elif args.sf == "10":
        data_dir = Path("/local/storage/liang/sf10")
        if not data_dir.exists():
            data_dir = REPO_ROOT / "paper_eval" / "benchmark-data-sf10"
    else:
        data_dir = REPO_ROOT / f"paper_eval/benchmark-data-sf{args.sf}"

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = REPO_ROOT / "paper_eval" / "results" / f"dask_geopandas_sf{args.sf}_default.json"

    print("=" * 70)
    print(" Dask-GeoPandas Benchmark Runner")
    print(f" Scale Factor : SF{args.sf}")
    print(f" Data Dir     : {data_dir}")
    print(f" Repetitions  : {args.runs}")
    print(f" Timeout      : {args.timeout}s")
    print(f" Queries      : {args.queries}")
    print(f" Output Path  : {output_path}")
    print("=" * 70)

    tables = ["building", "customer", "driver", "trip", "vehicle", "zone"]
    data_paths = {t: str(data_dir / t) for t in tables}

    query_map = {
        f"q{i}": getattr(dg, f"q{i}") for i in range(1, 13)
    }

    if args.queries == "all":
        selected_queries = list(query_map.keys())
    else:
        selected_queries = [q.strip().lower() for q in args.queries.split(",") if q.strip()]

    results = []
    total_start = time.perf_counter()

    for q_name in selected_queries:
        if q_name not in query_map:
            print(f"Skipping unknown query: {q_name}")
            continue

        q_fn = query_map[q_name]
        print(f"\n--- Running {q_name.upper()} ({args.runs} runs) ---")
        run_times = []
        last_row_count = None
        error_msg = None
        status = "success"

        for run_idx in range(args.runs):
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(args.timeout)
            try:
                t0 = time.perf_counter()
                res_df = q_fn(data_paths)
                elapsed = time.perf_counter() - t0
                signal.alarm(0)
                run_times.append(round(elapsed, 4))
                last_row_count = len(res_df) if res_df is not None else 0
                print(f"  Run {run_idx + 1}/{args.runs}: {elapsed:.3f}s ({last_row_count} rows)")
            except QueryTimeoutException:
                signal.alarm(0)
                print(f"  Run {run_idx + 1}/{args.runs}: TIMEOUT (> {args.timeout}s)")
                status = "timeout"
                error_msg = f"Query timed out after {args.timeout} seconds"
                break
            except Exception as e:
                signal.alarm(0)
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
            "engine": "dask_geopandas",
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

    total_time = round(time.perf_counter() - total_start, 2)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suite_data = {
        "benchmark": "spatialbench",
        "version": "0.1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "dask_geopandas",
        "scale_factor": float(args.sf),
        "total_time": total_time,
        "results": results,
    }

    with open(output_path, "w") as f:
        json.dump(suite_data, f, indent=2)

    print(f"\n[SUCCESS] Dask-GeoPandas benchmark results saved to {output_path}")
    import os
    os._exit(0)


if __name__ == "__main__":
    main()
