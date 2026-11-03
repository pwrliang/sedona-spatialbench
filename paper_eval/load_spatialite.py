#!/usr/bin/env python3
"""
Load Sedona SpatialBench parquet datasets into an SQLite / SpatiaLite database.
Uses DuckDB's high-speed C++ SQLite export, then initializes SpatiaLite metadata
and builds R-Tree spatial indexes on geometry columns.
"""

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path
import duckdb

TABLES = ["building", "customer", "driver", "trip", "vehicle", "zone"]


def load_spatialite(data_dir: str, db_path: str, create_indexes: bool = True) -> None:
    tmp_dir = Path("/local/scratch/liang/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    os.environ["TMPDIR"] = str(tmp_dir)

    data_path = Path(data_dir).resolve()
    db_file = Path(db_path).resolve()
    db_file.parent.mkdir(parents=True, exist_ok=True)

    if db_file.exists():
        print(f"Removing existing database at {db_file}...")
        for p in [db_file, Path(f"{db_file}-wal"), Path(f"{db_file}-shm")]:
            if p.exists():
                p.unlink()

    print(f"Loading parquet tables from {data_path} into {db_file} via DuckDB...")
    t0 = time.time()
    ddb = duckdb.connect()
    ddb.execute(f"SET temp_directory = '{tmp_dir}';")
    ddb.execute("INSTALL sqlite; LOAD sqlite;")
    ddb.execute(f"ATTACH '{db_file}' AS sp (TYPE SQLITE);")

    # Set pragma on attached db for fast loading
    for tbl in TABLES:
        tbl_path = data_path / tbl
        if tbl_path.is_dir():
            pq_pattern = f"{tbl_path}/*.parquet"
        else:
            pq_pattern = str(tbl_path)

        tbl_start = time.time()
        print(f"  Exporting table {tbl}...")
        ddb.execute(f"CREATE TABLE sp.{tbl} AS SELECT * FROM '{pq_pattern}';")
        count = ddb.execute(f"SELECT count(*) FROM sp.{tbl}").fetchone()[0]
        print(f"    Table {tbl}: {count:,} rows exported in {time.time() - tbl_start:.2f}s")

    ddb.close()
    print(f"All tables loaded in {time.time() - t0:.2f}s")

    print("Initializing SpatiaLite metadata and spatial columns...")
    t1 = time.time()
    scon = sqlite3.connect(str(db_file))
    scon.enable_load_extension(True)
    scon.load_extension("mod_spatialite")
    scon.execute(f"PRAGMA temp_store_directory = '{tmp_dir}';")
    scon.execute("PRAGMA temp_store = MEMORY;")
    scon.execute("PRAGMA journal_mode = WAL;")
    scon.execute("PRAGMA synchronous = NORMAL;")
    scon.execute("PRAGMA cache_size = -2000000;")  # 2 GB cache
    scon.execute("SELECT InitSpatialMetaData(1);")
    scon.commit()

    if create_indexes:
        print("Creating SpatiaLite geometry columns and spatial indexes...")
        # 1. Building
        print("  Indexing building...")
        scon.execute("SELECT AddGeometryColumn('building', 'geom_boundary', 4326, 'MULTIPOLYGON', 2);")
        scon.execute("UPDATE building SET geom_boundary = CastToMulti(GeomFromWKB(b_boundary, 4326));")
        scon.execute("SELECT CreateSpatialIndex('building', 'geom_boundary');")
        scon.commit()

        # 2. Zone
        print("  Indexing zone...")
        scon.execute("SELECT AddGeometryColumn('zone', 'geom_boundary', 4326, 'MULTIPOLYGON', 2);")
        scon.execute("UPDATE zone SET geom_boundary = CastToMulti(GeomFromWKB(z_boundary, 4326));")
        scon.execute("SELECT CreateSpatialIndex('zone', 'geom_boundary');")
        scon.commit()

        # 3. Trip
        print("  Indexing trip pickups and dropoffs...")
        scon.execute("SELECT AddGeometryColumn('trip', 'geom_pickup', 4326, 'POINT', 2);")
        scon.execute("SELECT AddGeometryColumn('trip', 'geom_dropoff', 4326, 'POINT', 2);")
        scon.execute("UPDATE trip SET geom_pickup = GeomFromWKB(t_pickuploc, 4326), geom_dropoff = GeomFromWKB(t_dropoffloc, 4326);")
        scon.execute("SELECT CreateSpatialIndex('trip', 'geom_pickup');")
        scon.execute("SELECT CreateSpatialIndex('trip', 'geom_dropoff');")
        scon.commit()

    scon.close()
    print(f"SpatiaLite preparation completed in {time.time() - t1:.2f}s. Total time: {time.time() - t0:.2f}s")
    print(f"Database size: {db_file.stat().st_size / (1024*1024):.2f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load SpatialBench into SpatiaLite")
    parser.add_argument("--data-dir", required=True, help="Path to SF1 or SF10 parquet directory")
    parser.add_argument("--db-path", required=True, help="Destination SQLite/SpatiaLite database file")
    parser.add_argument("--no-index", action="store_true", help="Skip spatial index creation")
    args = parser.parse_args()

    load_spatialite(args.data_dir, args.db_path, create_indexes=not args.no_index)
