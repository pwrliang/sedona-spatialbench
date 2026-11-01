#!/usr/bin/env python3
"""Load the HF v0.1.0 SF10 `building` table into PostGIS schema `hf` (diagnostic).

Uses the same path as the benchmark harness (DuckDB postgres extension,
CREATE TABLE AS SELECT * FROM read_parquet) plus the same GiST index definition,
so Q9 can be run on the HF data (0 invalid polygons) with `search_path=hf,public`
without reloading the 8 GB trip table.
Usage: load_hf_building.py <hf_sf10_dir>
"""
import os
import sys

import duckdb
import psycopg

d = sys.argv[1]
host, port = os.environ.get("POSTGIS_HOST", "127.0.0.1"), os.environ.get("POSTGIS_PORT", "5433")
db, user = os.environ.get("POSTGIS_DB", "spatialbench"), os.environ.get("POSTGIS_USER", os.environ["USER"])
cs = f"host={host} port={port} dbname={db} user={user}"
with psycopg.connect(cs, autocommit=True) as c:
    c.execute("CREATE SCHEMA IF NOT EXISTS hf")
    c.execute("DROP TABLE IF EXISTS hf.building")
con = duckdb.connect()
con.execute("LOAD postgres;")
con.execute("LOAD spatial;")
con.execute(f"ATTACH '{cs}' AS pg (TYPE POSTGRES);")
con.execute(f"CREATE TABLE pg.hf.building AS SELECT * FROM read_parquet('{d}/building/*.parquet');")
with psycopg.connect(cs, autocommit=True) as c:
    print(c.execute("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='hf.building'::regclass AND attname='b_boundary'").fetchone())
    c.execute("CREATE INDEX IF NOT EXISTS hf_idx_building_geom ON hf.building USING GIST (ST_GeomFromWKB(b_boundary));")
    c.execute("ANALYZE hf.building;")
    print(c.execute("SELECT count(*), count(*) FILTER (WHERE NOT ST_IsValid(b_boundary)) FROM hf.building").fetchone())
