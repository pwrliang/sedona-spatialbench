#!/usr/bin/env python3
"""E4 - PostGIS failure analysis helper.

Modes
  explain  <out_dir>
      EXPLAIN (costs) of all 12 benchmark queries, for both benchmark configurations:
        indexed   : default planner settings (GiST indexes available)
        noindex   : SET enable_indexscan=off; SET enable_bitmapscan=off
                    (exactly what PostGISNoIndexBenchmark does)
  analyze  <out_dir> <tag> (--query qN | --sql FILE) [--config indexed|noindex]
           [--timeout SEC] [--set k=v ...]
      EXPLAIN (ANALYZE, BUFFERS, VERBOSE) with a server-side statement_timeout.
      If the statement times out the plain EXPLAIN is stored instead and the
      timeout is recorded.  Always appends a JSON record to <out_dir>/../results/analyze.jsonl
  run      <out_dir> <tag> (--query qN | --sql FILE) [...same options]
      Executes the query and fetches all rows (timing only).

Connection parameters come from POSTGIS_* environment variables (config.env).
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "spatialbench-queries"))
from print_queries import PostGISSpatialBenchBenchmark  # noqa: E402


def connect():
    cs = (f"host={os.environ.get('POSTGIS_HOST','127.0.0.1')} port={os.environ.get('POSTGIS_PORT','5433')} "
          f"dbname={os.environ.get('POSTGIS_DB','spatialbench')} user={os.environ.get('POSTGIS_USER', os.environ.get('USER'))}")
    return psycopg.connect(cs, autocommit=True)


def configure(conn, config, sets):
    if config == "noindex":
        conn.execute("SET enable_indexscan = off")
        conn.execute("SET enable_bitmapscan = off")
    for kv in sets or []:
        k, v = kv.split("=", 1)
        conn.execute(f"SET {k} = {v}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["explain", "analyze", "run"])
    ap.add_argument("out_dir")
    ap.add_argument("tag", nargs="?")
    ap.add_argument("--query")
    ap.add_argument("--sql")
    ap.add_argument("--config", default="indexed", choices=["indexed", "noindex"])
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--set", action="append")
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    queries = PostGISSpatialBenchBenchmark().queries()

    if a.mode == "explain":
        for config in ["indexed", "noindex"]:
            for name, sql in queries.items():
                with connect() as conn:
                    configure(conn, config, a.set)
                    try:
                        rows = conn.execute("EXPLAIN (VERBOSE) " + sql).fetchall()
                        txt = "\n".join(r[0] for r in rows)
                    except Exception as e:
                        txt = f"ERROR: {e}"
                (out / f"explain_{config}_{name}.txt").write_text(txt)
                print(f"{config} {name}: {txt.splitlines()[0][:110]}")
        return

    sql = Path(a.sql).read_text() if a.sql else queries[a.query]
    rec = {"tag": a.tag, "query": a.query, "sql_file": a.sql, "config": a.config, "sets": a.set,
           "timeout": a.timeout, "started_utc": datetime.now(timezone.utc).isoformat()}
    with connect() as conn:
        configure(conn, a.config, a.set)
        conn.execute(f"SET statement_timeout = '{a.timeout}s'")
        t0 = time.perf_counter()
        try:
            if a.mode == "analyze":
                rows = conn.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql).fetchall()
                txt = "\n".join(r[0] for r in rows)
                rec["status"] = "ok"
            else:
                rows = conn.execute(sql).fetchall()
                txt = f"{len(rows)} rows"
                rec["status"] = "ok"; rec["rows"] = len(rows)
        except psycopg.errors.QueryCanceled as e:
            rec["status"] = "timeout"; rec["error"] = str(e).strip()
            conn.execute("SET statement_timeout = 0")
            txt = "-- TIMEOUT after %ss; plain EXPLAIN follows\n" % a.timeout + "\n".join(
                r[0] for r in conn.execute("EXPLAIN (VERBOSE) " + sql).fetchall())
        except Exception as e:
            rec["status"] = "error"; rec["error"] = str(e).strip()
            txt = f"ERROR: {e}"
        rec["wall_seconds"] = round(time.perf_counter() - t0, 3)
    (out / f"{a.mode}_{a.tag}.txt").write_text(f"-- {json.dumps(rec)}\n{txt}\n")
    res = out.parent / "results"
    res.mkdir(exist_ok=True)
    with open(res / f"{a.mode}.jsonl", "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"[pg] {a.tag}: {rec['status']} {rec['wall_seconds']}s {rec.get('error','')[:200]}", flush=True)


if __name__ == "__main__":
    main()
