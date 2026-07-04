#!/usr/bin/env bash
# Drive perf_selftest.sh across several RustDDS versions, one at a time, and
# collect every run's SUMMARY.csv under a single comparison directory.
#
# Runs are strictly sequential (never concurrent) so latency/throughput numbers
# are not skewed by cross-version CPU contention. Each version builds into its
# own CARGO_TARGET_DIR so binaries never clobber each other.
#
# Usage:
#   scripts/perf_compare.sh
#
# Edit the VERSIONS list below to add/remove versions. Each entry is
# "label=/abs/path/to/worktree".

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_REPO="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSIONS=(
  "timer_1ms=${MAIN_REPO}"
  "v0.13.0=/Users/juhe/rustdds-perf/v0.13.0"
  "v0.11.0=/Users/juhe/rustdds-perf/v0.11.0"
)

TS="$(date +%Y%m%d_%H%M%S)"
BASE="${BASE:-${MAIN_REPO}/target/perf/CMP_${TS}}"
mkdir -p "$BASE"

export DURATION="${DURATION:-10}"
export LAT_DURATION="${LAT_DURATION:-8}"
export BIG_DURATION="${BIG_DURATION:-30}"

echo "Comparison run -> $BASE"
echo "  DURATION=${DURATION}s LAT_DURATION=${LAT_DURATION}s BIG_DURATION=${BIG_DURATION}s"
echo

for entry in "${VERSIONS[@]}"; do
  label="${entry%%=*}"
  repo="${entry#*=}"
  echo "############################################################"
  echo "# $label   ($repo)"
  echo "############################################################"
  if [ ! -x "$repo/scripts/perf_selftest.sh" ] && [ ! -f "$repo/scripts/perf_selftest.sh" ]; then
    echo "  SKIP: no perf_selftest.sh in $repo/scripts" >&2
    continue
  fi
  export CARGO_TARGET_DIR="/tmp/rdds-tgt/${label}"
  export OUTDIR="${BASE}/${label}"
  bash "$repo/scripts/perf_selftest.sh" "$label"
  echo
done

echo "============================================================"
echo "All versions done. Combined results under: $BASE"
echo "Per-version SUMMARY.csv files:"
for entry in "${VERSIONS[@]}"; do
  label="${entry%%=*}"
  [ -f "${BASE}/${label}/SUMMARY.csv" ] && echo "  ${BASE}/${label}/SUMMARY.csv"
done
