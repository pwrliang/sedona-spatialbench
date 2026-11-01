#!/usr/bin/env bash
# Capture the hardware / software / data environment of the failure analysis.
# Output: env/environment.txt, env/pip_freeze.txt, env/data_manifest.txt,
#         env/git_state.txt, env/git_diff.patch
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../common/env.sh"
OUT="$FA_ROOT/env"
mkdir -p "$OUT"

{
  echo "# captured: $(date -Iseconds)"
  echo "## host";      hostname -f; uname -a
  echo "## cpu";       lscpu | grep -E "Model name|^CPU\(s\)|Thread|Core|Socket|MHz|L2|L3"
  echo "## memory";    free -g
  echo "## swap / overcommit"; cat /proc/sys/vm/overcommit_memory; swapon --show 2>/dev/null
  echo "## disks";     df -h "$PAPER_EVAL" "$POSTGIS_DIR" 2>/dev/null
  echo "## python";    "$PY" --version; echo "$PY"
  echo "## engine versions"
  "$PY" - <<'EOF'
import importlib.metadata as md
for p in ["duckdb","sedonadb","geopandas","shapely","polars","spatial-polars","pyarrow","pandas","psycopg","apache-sedona"]:
    try: print(f"{p:16s} {md.version(p)}")
    except Exception as e: print(f"{p:16s} n/a ({e})")
import shapely, duckdb, sedonadb
print("shapely GEOS     ", shapely.geos_version_string)
print("sedonadb.__version__", getattr(sedonadb, "__version__", "?"), "(module file:", sedonadb.__file__, ")")
con = duckdb.connect(); con.execute("LOAD spatial")
print("duckdb spatial ext:", con.execute("select extension_version from duckdb_extensions() where extension_name='spatial'").fetchone())
EOF
  echo "## sedona-db source checkout (sedonadb is an editable install)"
  SDB_SRC="$("$PY" -c 'import sedonadb,os;print(os.path.dirname(os.path.dirname(os.path.dirname(sedonadb.__file__))))' 2>/dev/null)"
  echo "path: $SDB_SRC"
  git -C "$SDB_SRC" log -1 --format='commit %H%n%ad %s' 2>/dev/null || echo "(not a git checkout)"
  git -C "$SDB_SRC" status --short 2>/dev/null | head -20
  echo "## clickhouse"; "$CLICKHOUSE_BIN" --version
  echo "## postgres / postgis"
  pg_start >/dev/null 2>&1
  psql_run -At -c "select version();" -c "select postgis_full_version();" 2>&1
  psql_run -At -c "select name||'='||setting||coalesce(unit,'') from pg_settings where name in ('shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','max_parallel_workers_per_gather','max_parallel_workers','max_worker_processes','jit','random_page_cost','parallel_setup_cost','parallel_tuple_cost') order by 1;" 2>&1
} > "$OUT/environment.txt" 2>&1

"$PY" -m pip freeze > "$OUT/pip_freeze.txt" 2>/dev/null

{
  echo "# Parquet files used (symlinks resolved; HF blob names are the sha256 of the file)"
  for d in "$SF1_DIR" "$SF10_DIR"; do
    for f in "$d"/*/*.parquet; do
      if [ -L "$f" ]; then h="$(basename "$(readlink -f "$f")")"; else h="$(sha256sum "$f" | cut -d' ' -f1)"; fi
      printf "%s\t%s\t%s\n" "${f#$PAPER_EVAL/}" "$(stat -L -c %s "$f")" "$h"
    done
  done
} > "$OUT/data_manifest.txt"

{
  echo "branch: $(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)"
  echo "commit: $(git -C "$REPO_ROOT" rev-parse HEAD)"
  git -C "$REPO_ROOT" status --short
} > "$OUT/git_state.txt"
git -C "$REPO_ROOT" diff > "$OUT/git_diff_at_analysis_start.patch"
echo "environment captured in $OUT"
