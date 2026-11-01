#!/usr/bin/env bash
# Shared environment for the SpatialBench failure analysis experiments.
# Source this file from every experiment script:
#     source "$(dirname "${BASH_SOURCE[0]}")/../common/env.sh"
#
# Everything is derived from paper_eval/config.env so that the failure analysis
# uses exactly the same binaries, data and PostGIS cluster as the main benchmark.

FA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"          # paper_eval/failure_analysis
PAPER_EVAL="$(cd "$FA_ROOT/.." && pwd)"                               # paper_eval
REPO_ROOT="$(cd "$PAPER_EVAL/.." && pwd)"                             # repository root
export FA_ROOT PAPER_EVAL REPO_ROOT

# shellcheck disable=SC1091
source "$PAPER_EVAL/config.env"

export PY="${PYTHON_EXEC}"
export PY_BIN_DIR="$(dirname "$PY")"
export CLICKHOUSE_BIN="${CLICKHOUSE_BIN:-$PY_BIN_DIR/clickhouse}"
export PG_BIN="${PG_BIN:-/usr/lib/postgresql/12/bin}"
export PSQL="${PSQL:-$PY_BIN_DIR/psql}"
[ -x "$PSQL" ] || PSQL="$(command -v psql)"
export POSTGIS_USER="${POSTGIS_USER:-$USER}"

# Datasets (absolute paths)
#  * SF10_ORIG : the copy that ALL original SF10 results (paper_eval/results/*sf10*)
#                were produced from (see "trip:" lines in original_run/logs/*.log and
#                PostGIS _benchmark_meta.data_sig).  Pre-release copy, Sep 2025.
#  * SF10_HF   : Hugging Face apache-sedona/spatialbench v0.1.0, which is what
#                paper_eval/run_benchmarks.sh picks up *today* (dir created after
#                the original runs).  See 01_dataset/compare_datasets_sf10.json.
#  SF10_DIR (used by the diagnosis scripts) defaults to SF10_ORIG so that the
#  diagnosis replays exactly the data behind the reported numbers.
export SF10_ORIG="${SF10_ORIG:-/local/storage/liang/sf10}"
export SF10_HF="${SF10_HF:-$PAPER_EVAL/$DATA_DIR_SF10}"
export SF1_DIR="${SF1_DIR:-$PAPER_EVAL/$DATA_DIR_SF1}"
export SF10_DIR="${SF10_DIR:-$SF10_ORIG}"

# Make the query modules / benchmark driver importable from python helpers
export PYTHONPATH="$REPO_ROOT/benchmark:$REPO_ROOT/spatialbench-queries:$FA_ROOT/common${PYTHONPATH:+:$PYTHONPATH}"

# Diagnostic (NOT benchmark) timeout used for "how long would it take" runs.
# The official benchmark uses 600 s; diagnostic runs may go longer so that the
# true completion time (or the failure mode) can be observed.
export DIAG_TIMEOUT="${DIAG_TIMEOUT:-3600}"

pg_start() {
    if ! "$PG_BIN/pg_isready" -h "$POSTGIS_HOST" -p "$POSTGIS_PORT" >/dev/null 2>&1; then
        echo "[env] starting PostgreSQL cluster at $POSTGIS_DIR"
        "$PG_BIN/pg_ctl" -D "$POSTGIS_DIR" -l "$POSTGIS_DIR/server.log" start -w
    fi
}

psql_run() {
    # usage: psql_run [psql args...]
    "$PSQL" -h "$POSTGIS_HOST" -p "$POSTGIS_PORT" -U "$POSTGIS_USER" -d "$POSTGIS_DB" -X -v ON_ERROR_STOP=1 "$@"
}

ts() { date +"%Y%m%d_%H%M%S"; }
