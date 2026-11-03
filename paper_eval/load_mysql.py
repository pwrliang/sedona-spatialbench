#!/usr/bin/env python3
"""
Load SpatialBench SF1 data into MySQL 8.0 with explicit LONGBLOB geometry columns.
"""

import subprocess
import time
from pathlib import Path
import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "paper_eval" / "benchmark-data-sf1"
MYSQL_BIN = "/local/scratch/liang/mysql8/bin/mysql"
MYSQL_SOCK = "/local/scratch/liang/mysql_db/mysql.sock"

print("=" * 60)
print(f"Loading SpatialBench SF1 into MySQL from {DATA_DIR}")
print("=" * 60)

TABLE_SCHEMAS = {
    "customer": """
        DROP TABLE IF EXISTS customer;
        CREATE TABLE customer (
            c_custkey BIGINT PRIMARY KEY,
            c_name VARCHAR(128),
            c_address VARCHAR(255),
            c_region VARCHAR(64),
            c_nation VARCHAR(64),
            c_phone VARCHAR(64)
        );
    """,
    "driver": """
        DROP TABLE IF EXISTS driver;
        CREATE TABLE driver (
            d_driverkey BIGINT PRIMARY KEY,
            d_name VARCHAR(128),
            d_address VARCHAR(255),
            d_region VARCHAR(64),
            d_nation VARCHAR(64),
            d_phone VARCHAR(64)
        );
    """,
    "vehicle": """
        DROP TABLE IF EXISTS vehicle;
        CREATE TABLE vehicle (
            v_vehiclekey BIGINT PRIMARY KEY,
            v_mfgr VARCHAR(64),
            v_brand VARCHAR(64),
            v_type VARCHAR(64),
            v_comment VARCHAR(255)
        );
    """,
    "building": """
        DROP TABLE IF EXISTS building;
        CREATE TABLE building (
            b_buildingkey BIGINT PRIMARY KEY,
            b_name VARCHAR(128),
            b_boundary LONGBLOB
        );
    """,
    "zone": """
        DROP TABLE IF EXISTS zone;
        CREATE TABLE zone (
            z_zonekey BIGINT PRIMARY KEY,
            z_gersid VARCHAR(64),
            z_country VARCHAR(64),
            z_region VARCHAR(64),
            z_name VARCHAR(128),
            z_subtype VARCHAR(64),
            z_boundary LONGBLOB
        );
    """,
    "trip": """
        DROP TABLE IF EXISTS trip;
        CREATE TABLE trip (
            t_tripkey BIGINT PRIMARY KEY,
            t_custkey BIGINT,
            t_driverkey BIGINT,
            t_vehiclekey BIGINT,
            t_pickuptime DATETIME,
            t_dropofftime DATETIME,
            t_fare DOUBLE,
            t_tip DOUBLE,
            t_totalamount DOUBLE,
            t_distance DOUBLE,
            t_pickuploc LONGBLOB,
            t_dropoffloc LONGBLOB
        );
    """,
}

# 1. Create tables in MySQL directly
print("Creating tables in MySQL...")
for tbl, ddl in TABLE_SCHEMAS.items():
    subprocess.run(
        [MYSQL_BIN, "-u", "root", "-S", MYSQL_SOCK, "spatialbench", "-e", ddl],
        check=True
    )
print("All tables created successfully with LONGBLOB support!")

# 2. Bulk insert via DuckDB MySQL connector
con = duckdb.connect()
con.execute("LOAD mysql;")
con.execute("ATTACH 'host=127.0.0.1 port=3307 user=root database=spatialbench' AS mysql_db (TYPE MYSQL);")

for tbl in TABLE_SCHEMAS.keys():
    t0 = time.time()
    parquet_path = str(DATA_DIR / tbl / "*.parquet")
    print(f"Loading {tbl}...")
    con.execute(f"INSERT INTO mysql_db.{tbl} SELECT * FROM read_parquet('{parquet_path}');")
    count = con.execute(f"SELECT COUNT(*) FROM mysql_db.{tbl};").fetchone()[0]
    elapsed = time.time() - t0
    print(f"  Loaded {tbl}: {count} rows in {elapsed:.2f}s ({count / elapsed:.0f} rows/s)")

print("\nAll tables successfully loaded into MySQL!")
