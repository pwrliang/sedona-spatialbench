#!/usr/bin/env python3
"""Generate the exact SQL scripts that the benchmark driver feeds to `clickhouse local`.

The benchmark (benchmark/run_benchmark.py::ClickHouseBenchmark) prepends one
`CREATE VIEW <table> AS SELECT * FROM file('<dir>/*.parquet')` per table and then
the dialect query from spatialbench-queries/print_queries.py.  We reproduce this
byte-for-byte so failures can be replayed outside the harness (which discarded
ClickHouse's stderr and kept only the exit code).

Outputs (in 02_clickhouse/sql/):
  qN.sql              - exact benchmark input
  explain_plan_qN.sql - EXPLAIN PLAN actions=1 (logical plan incl. join algorithm)
  explain_pipe_qN.sql - EXPLAIN PIPELINE (physical operators / parallelism)
  scaled/qN_<table>_<n>.sql - same query with <table> view truncated to n rows
                              (used to measure throughput & extrapolate timeouts)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "spatialbench-queries"))
from print_queries import ClickHouseSpatialBenchBenchmark  # noqa: E402

data_dir = Path(sys.argv[1]).resolve()
out = Path(__file__).resolve().parent / "sql"
(out / "scaled").mkdir(parents=True, exist_ok=True)
TABLES = ["building", "customer", "driver", "trip", "vehicle", "zone"]


def views(limit_table=None, limit=None):
    v = []
    for t in TABLES:
        src = f"SELECT * FROM file('{data_dir}/{t}/*.parquet')"
        if t == limit_table:
            # deterministic prefix of the table
            src += f" LIMIT {limit}"
        v.append(f"CREATE VIEW IF NOT EXISTS {t} AS {src};")
    return "\n".join(v)


queries = ClickHouseSpatialBenchBenchmark().queries()
# Diagnostic-only fix of the ClickHouse Q12 dialect bug: the outer query refers to
# `t.t_tripkey`, but `t` is only in scope inside the derived table `ranked`
# (ClickHouse error 47 UNKNOWN_IDENTIFIER).  The fix only renames the qualifier;
# it is used to measure what Q12 would cost once the dialect bug is fixed.
queries_fixed = dict(queries)
queries_fixed["q12"] = queries["q12"].replace("SELECT\n   t.t_tripkey,\n   AVG(b.dist)", "SELECT\n   ranked.t_tripkey,\n   AVG(ranked.dist)")
assert queries_fixed["q12"] != queries["q12"], "Q12 fix pattern did not apply"
(out / "q12_fixed.sql").write_text(f"{views()}\n{queries_fixed['q12'].strip().rstrip(';')};\n")
(out / "explain_plan_q12_fixed.sql").write_text(f"{views()}\nEXPLAIN PLAN actions=1, header=0\n{queries_fixed['q12'].strip().rstrip(';')};\n")
for name, sql in queries.items():
    sql = sql.strip().rstrip(";")
    (out / f"{name}.sql").write_text(f"{views()}\n{sql};\n")
    (out / f"explain_plan_{name}.sql").write_text(f"{views()}\nEXPLAIN PLAN actions=1, header=0\n{sql};\n")
    (out / f"explain_pipe_{name}.sql").write_text(f"{views()}\nEXPLAIN PIPELINE\n{sql};\n")
    (out / f"explain_ast_{name}.sql").write_text(f"{views()}\nEXPLAIN SYNTAX\n{sql};\n")

# scaled variants for the timed-out / memory-failed nested-loop joins
SCALED = {
    "q4": ("zone", [10000, 50000, 100000]),
    "q6": ("trip", [10000, 100000, 1000000]),
    "q8": ("trip", [1000, 10000, 100000]),
    "q9": ("building", [5000, 10000, 20000]),
    "q10": ("trip", [1000, 10000, 100000]),
    "q11": ("trip", [100, 1000, 10000]),
    "q12": ("trip", [10, 100, 1000]),
}
for name, (table, ns) in SCALED.items():
    sql = queries_fixed[name].strip().rstrip(";")
    for n in ns:
        (out / "scaled" / f"{name}_{table}_{n}.sql").write_text(f"{views(table, n)}\n{sql};\n")
print("SQL written to", out)
