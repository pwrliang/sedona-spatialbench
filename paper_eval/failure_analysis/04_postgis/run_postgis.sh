#!/usr/bin/env bash
# E4 - PostGIS 3.0 / PostgreSQL 12 failure analysis (SF10, database loaded from the
#      original dataset copy, see _benchmark_meta.data_sig).
#  A) metadata: settings, index build times, EXPLAIN of all queries (indexed / noindex)
#  B) Q9   : GEOS TopologyException -> validity census, ST_MakeValid variant,
#            and the same query on the HF v0.1.0 building table (schema hf)
#  C) Q12  : missing KNN operator -> original vs <-> rewrite on trip prefixes,
#            then the full <-> rewrite with a diagnostic budget
#  D) Q10/Q11 : per-unit cost on samples (1% zones / 100k trips) -> extrapolation
#  E) raw query time of the indexed runs without the charged index-build overhead
# Usage: bash 04_postgis/run_postgis.sh [A|B|C|D|E|all]
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../common/env.sh"
D="$FA_ROOT/04_postgis"; STEP="${1:-all}"
mkdir -p "$D/logs" "$D/plans" "$D/results"
pg_start
PGD="$PY $D/pg_diag.py"
# peak RSS of the PostgreSQL backends serving the query is tracked via --extra-proc-regex
prun() {  # prun <name> <timeout> <pg_diag args...>
    local name="$1" to="$2"; shift 2
    "$PY" "$FA_ROOT/common/diag_run.py" --name "$name" --timeout "$((to + 60))" --record "$D/results/diag_runs.jsonl" \
        --extra-proc-regex "postgres: (parallel worker for PID [0-9]+|$POSTGIS_USER $POSTGIS_DB)" \
        --stdout "$D/logs/$name.out" --stderr "$D/logs/$name.err" --meta '{"engine":"postgis"}' \
        -- $PGD "$@" --timeout "$to"
}

if [[ "$STEP" == A || "$STEP" == all ]]; then
    psql_run -c "select name, setting, unit from pg_settings where name in ('shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','max_parallel_workers_per_gather','max_parallel_workers','jit','random_page_cost') order by 1" \
             -c "select postgis_full_version()" -c "select * from _benchmark_meta order by key" \
             -c "\di+" -c "\dt+" > "$D/logs/db_metadata.txt" 2>&1
    $PGD explain "$D/plans" > "$D/plans/explain_summary.txt"
fi

if [[ "$STEP" == B || "$STEP" == all ]]; then
    "$PY" "$D/load_hf_building.py" "$SF10_HF" > "$D/logs/load_hf_building.log" 2>&1
    psql_run -f "$D/sql/validity.sql" > "$D/logs/validity.txt" 2>&1
    prun q9_replay       1800 analyze "$D/plans" q9_replay --query q9
    prun q9_makevalid    1800 analyze "$D/plans" q9_makevalid --sql "$D/sql/q9_makevalid.sql"
    prun q9_hf_building  1800 analyze "$D/plans" q9_hf_building --query q9 --set search_path=hf,public
fi

if [[ "$STEP" == C || "$STEP" == all ]]; then
    for n in 1000 10000; do
        prun "q12_orig_trip$n" 3600 analyze "$D/plans" "q12_orig_trip$n" --sql "$D/sql/q12_orig_trip$n.sql"
        prun "q12_knn_trip$n"  3600 analyze "$D/plans" "q12_knn_trip$n"  --sql "$D/sql/q12_knn_trip$n.sql"
    done
    prun q12_knn_full 3600 analyze "$D/plans" q12_knn_full --sql "$D/sql/q12_knn_operator.sql"
fi

if [[ "$STEP" == D || "$STEP" == all ]]; then
    prun q11_trip100k 3600 analyze "$D/plans" q11_trip100k --sql "$D/sql/q11_trip100k.sql"
    prun q10_zone1pct 3600 analyze "$D/plans" q10_zone1pct --sql "$D/sql/q10_zone1pct.sql"
fi

if [[ "$STEP" == E || "$STEP" == all ]]; then
    for q in q1 q2 q3 q4 q5 q6 q7 q8; do prun "raw_${q}_indexed" 600 run "$D/plans" "raw_${q}_indexed" --query "$q"; done
    for q in q4 q6 q8; do prun "analyze_${q}_noindex" 1800 analyze "$D/plans" "${q}_noindex" --query "$q" --config noindex; done
fi
