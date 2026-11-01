#!/usr/bin/env python3
"""E3 - DuckDB failure analysis helper.

Modes
  explain  <data_dir> <out_dir>
      EXPLAIN (physical plan) of all 12 benchmark queries -> which join operator
      DuckDB picks (SPATIAL_JOIN vs. NESTED_LOOP / BLOCKWISE_NL / CROSS_PRODUCT).
  analyze  <data_dir> <out_dir> <query> [--limit-table T --limit N] [--sql FILE]
      EXPLAIN ANALYZE of one query (optionally with table T truncated to N rows,
      optionally with an alternative SQL text) -> per-operator time & cardinality.
  run      <data_dir> <out_dir> <query> [--limit-table T --limit N] [--sql FILE]
      Execute and fetch the result (used under diag_run.py for timing + peak RSS).

The views are created exactly like benchmark/run_benchmark.py::DuckDBBenchmark.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "spatialbench-queries"))
from print_queries import DuckDBSpatialBenchBenchmark  # noqa: E402

TABLES = ["building", "customer", "driver", "trip", "vehicle", "zone"]


def connect(data_dir, limit_table=None, limit=None, sets=None):
    con = duckdb.connect()
    con.execute("LOAD spatial;")
    for kv in sets or []:
        k, v = kv.split("=", 1)
        con.execute(f"SET {k} = '{v}';")
    con.execute("SET enable_external_file_cache = false;")
    if os.environ.get("DUCKDB_THREADS"):
        con.execute(f"SET threads = {os.environ['DUCKDB_THREADS']};")
    if os.environ.get("DUCKDB_MEMORY_LIMIT"):
        con.execute(f"SET memory_limit = '{os.environ['DUCKDB_MEMORY_LIMIT']}';")
    for t in TABLES:
        src = f"SELECT * FROM read_parquet('{data_dir}/{t}/*.parquet')"
        if t == limit_table:
            # materialise the prefix so the measured work is the join, not the scan
            con.execute(f"CREATE TABLE {t} AS {src} LIMIT {limit}")
        else:
            con.execute(f"CREATE VIEW {t} AS {src}")
    return con


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["explain", "analyze", "run"])
    ap.add_argument("data_dir")
    ap.add_argument("out_dir")
    ap.add_argument("query", nargs="?")
    ap.add_argument("--limit-table")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--sql", help="file with an alternative SQL text for the query")
    ap.add_argument("--tag", default="")
    ap.add_argument("--set", action="append", help="DuckDB setting k=v, e.g. disabled_optimizers=filter_pushdown")
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    queries = DuckDBSpatialBenchBenchmark().queries()
    con = connect(a.data_dir, a.limit_table, a.limit, a.set)

    if a.mode == "explain":
        items = queries.items()
        if a.sql:
            items = [(a.tag or Path(a.sql).stem, Path(a.sql).read_text())]
        for name, sql in items:
            plan = con.execute("EXPLAIN " + sql).fetchall()
            (out / f"explain_{name}.txt").write_text("\n".join(r[1] for r in plan))
            ops = [op for op in ["SPATIAL_JOIN", "NESTED_LOOP_JOIN", "BLOCKWISE_NL_JOIN",
                                 "CROSS_PRODUCT", "PIECEWISE_MERGE_JOIN", "HASH_JOIN",
                                 "DELIM_JOIN", "LEFT_DELIM_JOIN", "RIGHT_DELIM_JOIN"]
                   if op in "\n".join(r[1] for r in plan)]
            print(f"{name}: {ops}")
        return

    sql = Path(a.sql).read_text() if a.sql else queries[a.query]
    tag = a.tag or (f"{a.query}" + (f"_{a.limit_table}{a.limit}" if a.limit_table else ""))
    if a.mode == "analyze":
        con.execute("SET explain_output = 'all';")
        t0 = time.perf_counter()
        rows = con.execute("EXPLAIN ANALYZE " + sql).fetchall()
        dt = time.perf_counter() - t0
        (out / f"analyze_{tag}.txt").write_text(f"-- wall {dt:.2f}s\n" + "\n".join(r[1] for r in rows))
        print(f"{tag}: EXPLAIN ANALYZE {dt:.2f}s")
    else:
        t0 = time.perf_counter()
        res = con.execute(sql).fetchall()
        dt = time.perf_counter() - t0
        print(json.dumps({"tag": tag, "seconds": round(dt, 3), "rows": len(res)}))


if __name__ == "__main__":
    main()
