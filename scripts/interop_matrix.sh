#!/bin/bash
# Unattended RustDDS interoperability matrix runner.
#
# Runs the OMG dds-rtps interoperability test suite with RustDDS paired against
# every other shape_main implementation found in the dds-rtps `executables/`
# directory, in BOTH directions (RustDDS as publisher and as subscriber), plus a
# RustDDS-vs-RustDDS baseline. XCDR1 encoding (-x 1) is used.
#
# Each pair has a hard wall-clock cap so a single hang cannot consume the whole
# run; orphaned shape_main / report processes are cleaned between pairs.
#
# Prerequisites:
#   - A checkout of https://github.com/omg-dds/dds-rtps (DDS_RTPS_DIR below),
#     with its Python venv set up (.venv) and the vendor shape_main binaries in
#     executables/.
#   - The RustDDS shape_main built and copied into executables/ as
#     rustdds-<ver>_shape_main_linux:
#         cargo build --release --example shape_main
#         cp target/release/examples/shape_main \
#            <dds-rtps>/executables/rustdds-0.12.0_shape_main_linux
#
# Usage:
#   scripts/interop_matrix.sh            # detaches into its own session and returns
#
# By default the script re-execs itself under `setsid`, fully detached from the
# launching terminal, so the multi-hour run survives terminal close / launch
# interruption. It prints the background PID and the console log path, then
# returns immediately. Progress is in <results>/MATRIX.log and SUMMARY.csv.
#
# Environment overrides (defaults in parentheses):
#   DDS_RTPS_DIR       path to the dds-rtps checkout (/home/juhe/cursor/dds-rtps)
#   RUST_EXE           RustDDS shape_main, relative to DDS_RTPS_DIR
#                      (executables/rustdds-0.12.0_shape_main_linux)
#   PAIR_TIMEOUT       hard cap per pair, seconds (2400 = 40 min)
#   MATRIX_NO_DETACH   set to 1 to run in the foreground (no setsid re-exec)
#   MATRIX_CONSOLE_LOG console log path for the detached run
#                      (/tmp/interop_matrix_<timestamp>.log)
#
# Results are written under <dds-rtps>/results/matrix_<timestamp>/:
#   MATRIX.log    high-level progress log
#   SUMMARY.csv   one row per pair: pair,direction,vendor,return_code,completed,ok,error
#   <tag>.xml     JUnit-style report per pair
#   logs/<tag>.log full stdout/stderr per pair

set -u

# Survive the launching shell/terminal. Re-exec once in a brand-new session
# (setsid) detached from the controlling terminal, so closing the terminal or
# interrupting the launch command cannot reap the (long) run. Opt out with
# MATRIX_NO_DETACH=1 to run in the foreground.
if [ -z "${MATRIX_DETACHED:-}" ] && [ -z "${MATRIX_NO_DETACH:-}" ]; then
  export MATRIX_DETACHED=1
  CONSOLE_LOG="${MATRIX_CONSOLE_LOG:-/tmp/interop_matrix_$(date +%Y%m%d_%H%M%S).log}"
  setsid bash "$0" "$@" </dev/null >"$CONSOLE_LOG" 2>&1 &
  echo "interop matrix detached into its own session (PID $!)"
  echo "console log: $CONSOLE_LOG"
  exit 0
fi

DDS_RTPS_DIR="${DDS_RTPS_DIR:-../dds-rtps}"
RUST="${RUST_EXE:-executables/rustdds-0.12.0_shape_main_linux}"
PAIR_TIMEOUT="${PAIR_TIMEOUT:-2400}"   # 40 min hard cap per pair

cd "$DDS_RTPS_DIR" || exit 1
source .venv/bin/activate

STAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="results/matrix_${STAMP}"
LOGDIR="${OUTDIR}/logs"
MASTER="${OUTDIR}/MATRIX.log"
SUMMARY="${OUTDIR}/SUMMARY.csv"
mkdir -p "$LOGDIR"

echo "pair,direction,vendor,return_code,completed,ok,error" > "$SUMMARY"

# Record the exact dds-rtps test-suite checkout used, so summaries can report it
# even if the working tree later moves. Read by scripts/interop_summary.py.
SUITE_VERSION=$(git -C "$DDS_RTPS_DIR" describe --tags --always --dirty 2>/dev/null)
echo "${SUITE_VERSION:-unknown}" > "${OUTDIR}/dds_rtps_version.txt"

# Record the test platform so summaries identify where the matrix ran.
uname -srvmo > "${OUTDIR}/platform.txt" 2>/dev/null || echo unknown > "${OUTDIR}/platform.txt"

echo "[$(date '+%F %T')] RustDDS interop matrix START (pair cap=${PAIR_TIMEOUT}s, dds-rtps ${SUITE_VERSION:-unknown})" | tee -a "$MASTER"

cleanup() {
  pkill -9 -f 'interoperability_report.py' 2>/dev/null
  pkill -9 -f 'shape_main_linux'           2>/dev/null
  sleep 3
}

run_pair() {
  local pexe="$1" sexe="$2" tag="$3" vendor="$4" dir="$5"
  echo "[$(date '+%F %T')] START $tag" | tee -a "$MASTER"
  timeout -k 30 "$PAIR_TIMEOUT" python3 -u interoperability_report.py \
      -P "$pexe" -S "$sexe" -x 1 -o "${OUTDIR}/${tag}.xml" \
      > "${LOGDIR}/${tag}.log" 2>&1
  local rc=$?
  cleanup
  local ok err done
  ok=$(grep -c ' : OK'    "${LOGDIR}/${tag}.log" 2>/dev/null || echo 0)
  err=$(grep -c ' : ERROR' "${LOGDIR}/${tag}.log" 2>/dev/null || echo 0)
  done=$((ok + err))
  echo "${tag},${dir},${vendor},${rc},${done},${ok},${err}" >> "$SUMMARY"
  echo "[$(date '+%F %T')] END   $tag rc=${rc} completed=${done} OK=${ok} ERR=${err}" | tee -a "$MASTER"
}

# clean slate
cleanup

# RustDDS vs itself (baseline, for a uniform matrix)
run_pair "$RUST" "$RUST" "rustdds_self" "rustdds" "self"

for v in executables/*shape_main_linux; do
  case "$v" in *rustdds*) continue ;; esac
  name=$(basename "$v" | sed 's/_shape_main_linux//')
  run_pair "$RUST" "$v"    "rustdds_P__${name}_S" "$name" "rustdds_pub"
  run_pair "$v"    "$RUST" "${name}_P__rustdds_S" "$name" "rustdds_sub"
done

echo "[$(date '+%F %T')] RustDDS interop matrix COMPLETE" | tee -a "$MASTER"
echo "Results in: ${OUTDIR}" | tee -a "$MASTER"
