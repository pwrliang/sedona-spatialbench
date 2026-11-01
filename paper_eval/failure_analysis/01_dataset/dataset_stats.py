#!/usr/bin/env python3
"""E1 - Dataset characterisation for the failure analysis.

Computes the table / geometry statistics that explain *why* certain queries are
expensive (e.g. number and complexity of zone polygons, validity of building
polygons).  Uses DuckDB (spatial extension, GEOS-backed ST_IsValid) for scanning.

Usage: python dataset_stats.py <data_dir> <out_json>
"""
import json
import sys
import time
from pathlib import Path

import duckdb

data_dir, out_path = Path(sys.argv[1]), Path(sys.argv[2])
con = duckdb.connect()
con.execute("LOAD spatial")
for t in ["building", "customer", "driver", "trip", "vehicle", "zone"]:
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{data_dir}/{t}/*.parquet')")

stats = {"data_dir": str(data_dir)}


def q(name, sql):
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.description]
    stats[name] = [dict(zip(cols, r)) for r in rows]
    print(f"[{time.perf_counter()-t0:7.1f}s] {name}: {stats[name][:5]}", flush=True)


for t in ["building", "customer", "driver", "trip", "vehicle", "zone"]:
    q(f"rows_{t}", f"SELECT count(*) AS n FROM {t}")

# Geometry column types as seen by DuckDB (GEOMETRY vs BLOB matters for portability)
q("schema_trip", "SELECT column_name, column_type FROM (DESCRIBE trip)")
q("schema_zone", "SELECT column_name, column_type FROM (DESCRIBE zone)")
q("schema_building", "SELECT column_name, column_type FROM (DESCRIBE building)")

# Zone polygon complexity (drives the cost of every point-in-polygon / ST_Within join)
q("zone_complexity", """
SELECT count(*) AS zones,
       sum(ST_NPoints(ST_GeomFromWKB(z_boundary)))            AS total_vertices,
       avg(ST_NPoints(ST_GeomFromWKB(z_boundary)))            AS avg_vertices,
       median(ST_NPoints(ST_GeomFromWKB(z_boundary)))         AS median_vertices,
       quantile_cont(ST_NPoints(ST_GeomFromWKB(z_boundary)), 0.99) AS p99_vertices,
       max(ST_NPoints(ST_GeomFromWKB(z_boundary)))            AS max_vertices,
       avg(octet_length(z_boundary))                           AS avg_wkb_bytes,
       sum(octet_length(z_boundary))                           AS total_wkb_bytes
FROM zone""")
q("zone_types", "SELECT ST_GeometryType(ST_GeomFromWKB(z_boundary)) AS gtype, count(*) AS n FROM zone GROUP BY 1 ORDER BY 2 DESC")
q("zone_subtypes", "SELECT z_subtype, count(*) n, avg(ST_NPoints(ST_GeomFromWKB(z_boundary))) AS avg_vertices FROM zone GROUP BY 1 ORDER BY 2 DESC")
q("zone_top10_vertices", """
SELECT z_zonekey, z_name, z_subtype, ST_NPoints(ST_GeomFromWKB(z_boundary)) AS vertices
FROM zone ORDER BY vertices DESC LIMIT 10""")
q("coconino_county", """
SELECT z_zonekey, z_name, ST_NPoints(ST_GeomFromWKB(z_boundary)) AS vertices,
       ST_GeometryType(ST_GeomFromWKB(z_boundary)) AS gtype
FROM zone WHERE z_name = 'Coconino County'""")
q("q6_bbox_zones", """
SELECT count(*) AS zones_intersecting_q6_bbox,
       sum(ST_NPoints(ST_GeomFromWKB(z_boundary))) AS vertices
FROM zone WHERE ST_Intersects(ST_GeomFromWKB(z_boundary),
  ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'))""")
# Overlapping zones (nested admin levels) => a point can fall into many zones
q("zone_overlap_hint", "SELECT z_subtype, count(*) FROM zone GROUP BY 1 ORDER BY 2 DESC LIMIT 20")

# Building polygons: validity is what breaks PostGIS 3.0/GEOS 3.8 on Q9
q("building_complexity", """
SELECT count(*) AS buildings,
       avg(ST_NPoints(ST_GeomFromWKB(b_boundary))) AS avg_vertices,
       max(ST_NPoints(ST_GeomFromWKB(b_boundary))) AS max_vertices,
       sum(CASE WHEN ST_IsValid(ST_GeomFromWKB(b_boundary)) THEN 0 ELSE 1 END) AS invalid_polygons
FROM building""")
q("building_invalid_examples", """
SELECT b_buildingkey, ST_NPoints(ST_GeomFromWKB(b_boundary)) AS vertices,
       ST_AsText(ST_Centroid(ST_GeomFromWKB(b_boundary))) AS centroid
FROM building WHERE NOT ST_IsValid(ST_GeomFromWKB(b_boundary)) LIMIT 10""")

# Trip extent
q("trip_extent", """
SELECT min(ST_X(ST_GeomFromWKB(t_pickuploc))) AS min_x, max(ST_X(ST_GeomFromWKB(t_pickuploc))) AS max_x,
       min(ST_Y(ST_GeomFromWKB(t_pickuploc))) AS min_y, max(ST_Y(ST_GeomFromWKB(t_pickuploc))) AS max_y
FROM trip""")

out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(stats, indent=2, default=str))
print("wrote", out_path)
