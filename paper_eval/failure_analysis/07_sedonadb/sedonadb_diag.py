#!/usr/bin/env python3
"""E7 - SedonaDB physical plan and execution characteristics.

Extracts logical and physical plans for all 12 SpatialBench queries under SedonaDB
to document why SedonaDB handles all queries (including complex spatial joins like
Q9, Q10, Q11, Q12) efficiently and without OOM or timeouts.

Outputs:
  07_sedonadb/plans/explain_qN.txt
  07_sedonadb/plans/explain_summary.txt
"""
import sys
import time
from pathlib import Path
import sedonadb

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "benchmark"))
sys.path.insert(0, str(ROOT / "spatialbench-queries"))
from print_queries import SpatialBenchBenchmark
from run_benchmark import get_data_paths

data_dir = sys.argv[1]
out_dir = Path(sys.argv[2])
out_dir.mkdir(parents=True, exist_ok=True)

data_paths = get_data_paths(data_dir)
sedona = sedonadb.connect()

for table, path in data_paths.items():
    parquet_path = path
    if Path(path).is_dir():
        parquet_path = str(Path(path) / "*.parquet")
    sedona.read_parquet(parquet_path).to_view(table, overwrite=True)

queries = SpatialBenchBenchmark().queries()
summary = []

for qname, qsql in queries.items():
    try:
        t0 = time.perf_counter()
        df = sedona.sql(f"EXPLAIN {qsql}").to_pandas()
        logical = df[df["plan_type"] == "logical_plan"]["plan"].iloc[0]
        physical = df[df["plan_type"] == "physical_plan"]["plan"].iloc[0]
        (out_dir / f"explain_{qname}.txt").write_text(f"-- SedonaDB LOGICAL PLAN for {qname}\n{logical}\n\n-- SedonaDB PHYSICAL PLAN for {qname}\n{physical}\n")
        
        # Identify key spatial operators
        ops = []
        for line in physical.splitlines():
            line_s = line.strip()
            if any(k in line_s for k in ["SpatialJoin", "Broadcast", "KNN", "RTree", "SpatialIndex", "Join", "Filter", "Sort"]):
                ops.append(line_s[:100])
        summary.append(f"{qname}: physical plan {len(physical.splitlines())} lines, ops: {ops[:5]}")
        print(f"[{qname}] EXPLAIN extracted in {time.perf_counter() - t0:.2f}s")
    except Exception as e:
        print(f"[{qname}] ERROR: {e}")
        (out_dir / f"explain_{qname}.txt").write_text(f"ERROR: {e}\n")
        summary.append(f"{qname}: ERROR ({e})")

(out_dir / "explain_summary.txt").write_text("\n".join(summary) + "\n")
print("SedonaDB plans written to", out_dir)
