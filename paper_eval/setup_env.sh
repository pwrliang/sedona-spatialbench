#!/usr/bin/env bash
# Script to verify and configure the isolated environment for SpatialBench reproduction

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.env"

echo "=========================================================="
echo " Sedona SpatialBench: Environment Verification"
echo "=========================================================="

if [ ! -x "$PYTHON_EXEC" ]; then
    echo "[ERROR] Python executable not found at: $PYTHON_EXEC"
    echo "To create the conda environment, run:"
    echo "  conda create -n spatialbench python=3.11 -y"
    echo "  conda activate spatialbench"
    echo "  pip install duckdb geopandas spatial-polars pyarrow shapely matplotlib pandas"
    exit 1
fi

echo "[INFO] Using Python: $($PYTHON_EXEC --version) at $PYTHON_EXEC"
echo ""
echo "--- Checking Installed Packages ---"
$PYTHON_EXEC - << 'EOF'
import sys

packages = [
    ("duckdb", "DuckDB (In-process Vectorized OLAP)"),
    ("sedonadb", "SedonaDB (Columnar Spatial Engine)"),
    ("geopandas", "GeoPandas (Python Spatial DataFrame)"),
    ("spatial_polars", "Polaris / Spatial Polars (Rust/Arrow DataFrame)"),
    ("polars", "Polars Core"),
    ("pyarrow", "Apache Arrow"),
    ("shapely", "Shapely (GEOS Wrapper)"),
    ("psycopg", "Psycopg (PostgreSQL / PostGIS Driver)"),
    ("matplotlib", "Matplotlib (Plotting & Visualization)")
]

for pkg, desc in packages:
    try:
        mod = __import__(pkg)
        ver = getattr(mod, "__version__", "installed")
        print(f"  [OK]   {pkg:<16} : {ver:<12} ({desc})")
    except ImportError as e:
        print(f"  [FAIL] {pkg:<16} : MISSING      ({e})")
EOF

echo ""
echo "--- Testing Spatial Extensions ---"
$PYTHON_EXEC - << 'EOF'
import duckdb
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
print("  [OK] DuckDB spatial extension verified")
EOF

$PYTHON_EXEC - << 'EOF'
import os, psycopg
host = os.environ.get("POSTGIS_HOST", "127.0.0.1")
port = os.environ.get("POSTGIS_PORT", "5433")
dbname = os.environ.get("POSTGIS_DB", "spatialbench")
user = os.environ.get("POSTGIS_USER", os.environ.get("USER", "geng.161"))
try:
    with psycopg.connect(f"host={host} port={port} dbname={dbname} user={user}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT PostGIS_Version();")
            ver = cur.fetchone()[0]
            print(f"  [OK] PostGIS extension verified: {ver}")
except Exception as e:
    print(f"  [WARN] PostGIS check: {e}")
EOF

if command -v clickhouse &> /dev/null; then
    ch_ver=$(clickhouse --version | head -n 1)
    echo "  [OK] ClickHouse binary verified: $ch_ver"
else
    echo "  [WARN] ClickHouse binary not found in PATH"
fi

echo ""
echo "--- Checking Dataset Directories ---"
for dir_path in "$DATA_DIR_SF1" "$DATA_DIR_SF10"; do
    if [ -d "$dir_path" ]; then
        size=$(du -sh "$dir_path" | cut -f1)
        echo "  [OK] Found $dir_path (Size: $size)"
    else
        echo "  [WARN] Dataset directory not found: $dir_path"
    fi
done

echo ""
echo "[INFO] Environment verification completed successfully."
