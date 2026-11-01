#!/usr/bin/env python3
"""E1b - Compare the SF10 dataset copies present on the machine.

The PostGIS database stores the path it was loaded from in _benchmark_meta.data_sig.
On DL190 PostGIS was loaded from /local/storage/liang/sf10 while all file-based
engines read paper_eval/benchmark-data-sf10 (HF apache-sedona/spatialbench).  This
script checks whether the two copies contain the same data (row counts, key
checksums, geometry checksums, validity of building polygons).

Usage: python compare_datasets.py <dirA> <dirB> <out_json>
"""
import json
import sys

import duckdb

a, b, out = sys.argv[1], sys.argv[2], sys.argv[3]
con = duckdb.connect()
con.execute("LOAD spatial")
CHECKS = {
    "building": "count(*) n, sum(hash(b_buildingkey)) k, sum(hash(b_boundary)) g, "
                "count(*) filter (where not ST_IsValid(ST_GeomFromWKB(b_boundary))) invalid, "
                "min(ST_NPoints(ST_GeomFromWKB(b_boundary))) min_v, max(ST_NPoints(ST_GeomFromWKB(b_boundary))) max_v",
    "zone": "count(*) n, sum(hash(z_zonekey)) k, sum(hash(z_boundary)) g, sum(ST_NPoints(ST_GeomFromWKB(z_boundary))) vertices",
    "trip": "count(*) n, sum(hash(t_tripkey)) k, sum(hash(t_pickuploc)) g, sum(hash(t_dropoffloc)) g2, sum(t_tip) tips",
    "customer": "count(*) n, sum(hash(c_custkey, c_name)) k",
}
res = {}
for d in (a, b):
    res[d] = {}
    for t, expr in CHECKS.items():
        row = con.execute(f"SELECT {expr} FROM read_parquet('{d}/{t}/*.parquet')").fetchone()
        cols = [x[0] for x in con.description]
        res[d][t] = dict(zip(cols, [int(v) if isinstance(v, int) else (float(v) if v is not None else None) for v in row]))
        print(d, t, res[d][t], flush=True)
    res[d]["building_schema"] = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{d}/building/*.parquet')").fetchall()
    res[d]["files"] = con.execute(f"SELECT file_name, count(*) FROM parquet_metadata('{d}/*/*.parquet') GROUP BY 1 ORDER BY 1").fetchall()

same = {t: res[a][t] == res[b][t] for t in CHECKS}
res["identical"] = same
print("identical per table:", same)
json.dump(res, open(out, "w"), indent=2, default=str)
