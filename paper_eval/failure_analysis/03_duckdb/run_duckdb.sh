#!/usr/bin/env bash
# E3 - DuckDB failure analysis (SF10, original dataset).
#  A) EXPLAIN of the 12 benchmark queries + diagnostic rewrites (join operator chosen)
#  B) Q9 / Q11 : confirm that the timeout is caused by the planner NOT using
#                SPATIAL_JOIN when the join has an extra non-spatial predicate
#                (run the semantically identical rewrite that does get SPATIAL_JOIN)
#  C) Q10      : SPATIAL_JOIN *is* used, yet it timed out -> profile it on a 1M-trip
#                prefix (EXPLAIN ANALYZE) and test sensitivity to polygon complexity
#  D) Q12      : LATERAL top-k -> cross product; per-trip cost on small prefixes and
#                memory growth of the full query (capped at 40 GiB RSS)
#  E) Q9 orig  : quadratic growth of the PIECEWISE_MERGE_JOIN plan on building prefixes
# Usage: bash 03_duckdb/run_duckdb.sh [A|B|C|D|E|all]
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../common/env.sh"
D="$FA_ROOT/03_duckdb"; STEP="${1:-all}"; REC="$D/results/diag_runs.jsonl"
mkdir -p "$D/logs" "$D/plans" "$D/results"
DIAG="$PY $D/duckdb_diag.py"

drun() {  # drun <name> <timeout> <duckdb_diag args...>
    local name="$1" to="$2"; shift 2
    "$PY" "$FA_ROOT/common/diag_run.py" --name "$name" --timeout "$to" --record "$REC" --max-rss-gib 40 \
        --stdout "$D/logs/$name.out" --stderr "$D/logs/$name.err" --meta '{"engine":"duckdb"}' \
        -- $DIAG "$@"
}

if [[ "$STEP" == A || "$STEP" == all ]]; then
    $DIAG explain "$SF10_DIR" "$D/plans" | tee "$D/logs/explain_summary.txt"
    for f in "$D"/sql/*.sql; do $DIAG explain "$SF10_DIR" "$D/plans" --sql "$f"; done | tee "$D/logs/explain_rewrites_summary.txt"
    for f in q9_rewrite_where q9_rewrite_matcte; do
        $DIAG explain "$SF10_DIR" "$D/plans" --sql "$D/sql/$f.sql" --tag "${f}_nopushdown" \
            --set disabled_optimizers=filter_pushdown,cte_filter_pusher
    done | tee -a "$D/logs/explain_rewrites_summary.txt"
fi

if [[ "$STEP" == B || "$STEP" == all ]]; then
    drun q9_rewrite_nopushdown_sf10 1200 run "$SF10_DIR" "$D/results" q9 --sql "$D/sql/q9_rewrite_where.sql" \
        --set disabled_optimizers=filter_pushdown,cte_filter_pusher --tag q9_rewrite
    drun q11_rewrite_matcte_sf10 1800 run "$SF10_DIR" "$D/results" q11 --sql "$D/sql/q11_rewrite_matcte.sql" --tag q11_rewrite
fi

if [[ "$STEP" == C || "$STEP" == all ]]; then
    for v in q10_join_only_count q10_no_county q10_county_only; do
        drun "analyze_${v}_trip1M" 1800 analyze "$SF10_DIR" "$D/plans" q10 --sql "$D/sql/$v.sql" \
            --limit-table trip --limit 1000000 --tag "${v}_trip1M"
    done
    drun analyze_q10_trip1M 1800 analyze "$SF10_DIR" "$D/plans" q10 --limit-table trip --limit 1000000
    # full-size original Q10 with a 30-minute diagnostic budget
    drun q10_full_sf10_diag 1800 run "$SF10_DIR" "$D/results" q10
fi

if [[ "$STEP" == D || "$STEP" == all ]]; then
    for n in 100 1000; do
        drun "analyze_q12_trip$n" 1800 analyze "$SF10_DIR" "$D/plans" q12 --limit-table trip --limit $n
    done
    drun q12_full_sf10_rsscap40 900 run "$SF10_DIR" "$D/results" q12
fi

if [[ "$STEP" == E || "$STEP" == all ]]; then
    for n in 5000 10000 20000; do
        drun "analyze_q9_building$n" 1800 analyze "$SF10_DIR" "$D/plans" q9 --limit-table building --limit $n
    done
fi
