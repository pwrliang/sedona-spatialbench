#!/usr/bin/env bash
# E2 - ClickHouse failure analysis.
#   A) replay every failed SF10 query exactly as the benchmark ran it, but keep
#      ClickHouse's stderr (the harness only kept the exit code)
#   B) EXPLAIN PLAN / PIPELINE for all 12 queries (join algorithm chosen)
#   C) scaled runs (truncated input table) to measure nested-loop throughput and
#      extrapolate the runtime of the queries that hit the 600 s timeout
# Usage: bash 02_clickhouse/run_clickhouse.sh [A|B|C|all]
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../common/env.sh"
D="$FA_ROOT/02_clickhouse"
STEP="${1:-all}"
REC="$D/results/diag_runs.jsonl"
mkdir -p "$D/logs" "$D/plans" "$D/results"
"$PY" "$D/gen_sql.py" "$SF10_DIR" >/dev/null

ch() {  # ch <name> <sqlfile> <timeout>
    "$PY" "$FA_ROOT/common/diag_run.py" --name "$1" --timeout "$3" --record "$REC" \
        --stdin "$2" --stdout "$D/logs/$1.out" --stderr "$D/logs/$1.err" \
        --meta "{\"engine\":\"clickhouse\",\"sql\":\"${2#$FA_ROOT/}\"}" \
        -- "$CLICKHOUSE_BIN" local --multiquery --format JSONEachRow --time
}

if [[ "$STEP" == A || "$STEP" == all ]]; then
    # record the memory cap clickhouse-local derives at start-up (0.9 x available RAM)
    "$CLICKHOUSE_BIN" local -q "select name, value from system.server_settings where name like 'max_server_memory_usage%'" \
        > "$D/logs/memory_cap_$(ts).txt" 2>&1; free -b >> "$D/logs/memory_cap_$(ts).txt"
    for q in q4 q10 q12; do ch "replay_${q}_sf10" "$D/sql/$q.sql" 600; done
fi

if [[ "$STEP" == A2 || "$STEP" == all ]]; then
    # same queries with the server memory cap raised to 56 GB (90% of physical RAM):
    # does the query then finish, or does it merely turn into a timeout?
    for q in q4 q10; do
        "$PY" "$FA_ROOT/common/diag_run.py" --name "replay_${q}_sf10_mem56g" --timeout 600 --record "$REC" \
            --stdin "$D/sql/$q.sql" --stdout "$D/logs/replay_${q}_sf10_mem56g.out" --stderr "$D/logs/replay_${q}_sf10_mem56g.err" \
            --meta "{\"engine\":\"clickhouse\",\"max_server_memory_usage\":56000000000}" \
            -- "$CLICKHOUSE_BIN" local --multiquery --format JSONEachRow --time -- --max_server_memory_usage=56000000000
    done
fi

if [[ "$STEP" == B || "$STEP" == all ]]; then
    for i in $(seq 1 12); do
        for k in plan pipe ast; do
            timeout 300 "$CLICKHOUSE_BIN" local --multiquery < "$D/sql/explain_${k}_q$i.sql" \
                > "$D/plans/explain_${k}_q$i.txt" 2>&1
        done
    done
fi

if [[ "$STEP" == C || "$STEP" == all ]]; then
    for f in $(ls "$D"/sql/scaled/*.sql | sort -V); do
        n="$(basename "$f" .sql)"
        ch "scaled_${n}" "$f" 900
    done
fi
