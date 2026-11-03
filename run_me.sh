#!/usr/bin/env bash
# ==============================================================================
# Sedona SpatialBench: One-Hit Reproduction Runner
# ==============================================================================
# Delegates to paper_eval/run_me.sh
# ==============================================================================

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$REPO_ROOT/paper_eval/run_me.sh" "$@"
