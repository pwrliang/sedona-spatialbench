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
