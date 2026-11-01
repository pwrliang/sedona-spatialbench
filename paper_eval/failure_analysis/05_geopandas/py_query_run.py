#!/usr/bin/env python3
"""E5/E6 - Run one GeoPandas or Spatial-Polars benchmark query function in isolation.

Used under common/diag_run.py (wall time + peak RSS of the process) and optionally
under py-spy (sampling profile).  Prints a JSON line with the result size and the
time spent in the query function, plus the RSS high-water mark reported by the
kernel (ru_maxrss) as a cross-check of the /proc sampler.

Usage: py_query_run.py <geopandas|spatial_polars> <data_dir> <qN>
"""
import importlib.util
import json
import resource
import sys
import time
from pathlib import Path

engine, data_dir, q = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "benchmark"))
from run_benchmark import get_data_paths  # noqa: E402

if engine == "spatial_polars":
    import spatial_polars  # noqa: F401  (registers the .spatial namespace)
fname = {"geopandas": "geopandas_queries.py", "spatial_polars": "spatial_polars.py"}[engine]
spec = importlib.util.spec_from_file_location("qmod", ROOT / "spatialbench-queries" / fname)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

paths = get_data_paths(str(data_dir))
t0 = time.perf_counter()
res = getattr(mod, q)(paths)
dt = time.perf_counter() - t0
print(json.dumps({"engine": engine, "query": q, "seconds": round(dt, 3), "rows": len(res),
                  "ru_maxrss_gib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20, 3)}))
