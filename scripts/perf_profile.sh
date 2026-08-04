#!/usr/bin/env bash
# Execution profiler for RustDDS (and, by override, CycloneDDS) ddsperf.
#
# Goal: find where a ddsperf process spends its time and where it blocks, so the
# most promising optimization targets are obvious. It drives a pub/sub (or
# ping/pong) pair over loopback and profiles ONE side (the "target") three ways:
#
#   1. aux      - built-in tools, no Xcode needed, human-readable immediately:
#                   * ps -M         : thread inventory (how many, their states)
#                   * sample        : on-CPU call-tree snapshot (text)
#                   * spindump      : per-thread stacks incl. blocked state
#                                     (best with sudo; skipped/limited otherwise)
#   2. time     - xctrace 'Time Profiler': on-CPU sampling (where cycles go).
#   3. systrace - xctrace 'System Trace' : thread states, blocking, VM ops and
#                                          syscalls (where/why threads wait).
#
# Both ddsperf binaries are native Mach-O, so the SAME tools profile RustDDS and
# CycloneDDS -- override TARGET_CMD / PEER_CMD to point at CycloneDDS's ddsperf
# and you get an apples-to-apples comparison.
#
# ---------------------------------------------------------------------------
# Usage:
#   scripts/perf_profile.sh [LABEL]
#
# Common env overrides (defaults in parentheses):
#   ROLE        which side to profile: sub|pub|ping|pong           (sub)
#   RELIABLE    0 = best-effort (-u), 1 = reliable                 (0)
#   RATE        publish/ping rate in Hz; 0 = flat-out (pub only)   (sub/pub:0, ping/pong:100)
#   SIZE        payload bytes                                      (1024)
#   DURATION    seconds to record per pass                         (15)
#   WARMUP      seconds the peer runs before the target starts     (3)
#   PASSES      space list of passes to run: aux time systrace     (aux time systrace)
#   SAMPLE_SECS seconds for the built-in `sample` snapshot (aux)   (5)
#   BIN_DIR     dir holding ddsperf                                (target/profiling/examples)
#   NO_BUILD    1 to skip the cargo build                          (0)
#   OUTDIR      results dir                                        (target/perf/profile_<ts>[_label])
#
# Full command override (e.g. to profile CycloneDDS):
#   TARGET_CMD  full command line for the profiled side
#   PEER_CMD    full command line for the background peer
#   (When set, ROLE/RELIABLE/RATE/SIZE are ignored for command construction but
#    ROLE is still used for output naming.)
#
# Examples:
#   # Profile the RustDDS receive path under a best-effort 1 KB flood:
#   scripts/perf_profile.sh rustdds_sub_be_1k
#
#   # Profile the RustDDS send path, reliable, flat-out, 8 KB:
#   ROLE=pub RELIABLE=1 RATE=0 SIZE=8192 scripts/perf_profile.sh rustdds_pub_rel_8k
#
#   # Same scenario against CycloneDDS (adjust its CLI to taste):
#   NO_BUILD=1 ROLE=sub \
#     TARGET_CMD="/opt/cyclone/bin/ddsperf -L sub" \
#     PEER_CMD="/opt/cyclone/bin/ddsperf pub size 1024" \
#     scripts/perf_profile.sh cyclone_sub_1k
# ---------------------------------------------------------------------------

set -u

LABEL="${1:-}"

ROLE="${ROLE:-sub}"
RELIABLE="${RELIABLE:-0}"
SIZE="${SIZE:-1024}"
DURATION="${DURATION:-15}"
WARMUP="${WARMUP:-3}"
SAMPLE_SECS="${SAMPLE_SECS:-5}"
PASSES="${PASSES:-aux time systrace}"

# Rate default depends on role: throughput sides can run flat-out (0); ping/pong
# need a positive rate (the example computes 1e9/rate and would divide by zero).
case "$ROLE" in
  ping|pong) RATE="${RATE:-100}" ;;
  *)         RATE="${RATE:-0}" ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR" || exit 1

BIN_DIR="${BIN_DIR:-${CARGO_TARGET_DIR:-target}/profiling/examples}"
DDSPERF="$BIN_DIR/ddsperf"

STAMP="$(date +%Y%m%d_%H%M%S)"
if [ -n "$LABEL" ]; then
  OUTDIR="${OUTDIR:-target/perf/profile_${STAMP}_${LABEL}}"
else
  OUTDIR="${OUTDIR:-target/perf/profile_${STAMP}}"
fi
mkdir -p "$OUTDIR"

# --- reliability flag + default command lines --------------------------------
UFLAG=""
[ "$RELIABLE" = "0" ] && UFLAG="-u"

if [ -z "${TARGET_CMD:-}" ] || [ -z "${PEER_CMD:-}" ]; then
  case "$ROLE" in
    sub)  TARGET_CMD="$DDSPERF $UFLAG sub"
          PEER_CMD="$DDSPERF $UFLAG pub $RATE size $SIZE" ;;
    pub)  TARGET_CMD="$DDSPERF $UFLAG pub $RATE size $SIZE"
          PEER_CMD="$DDSPERF $UFLAG sub" ;;
    ping) TARGET_CMD="$DDSPERF $UFLAG ping $RATE size $SIZE"
          PEER_CMD="$DDSPERF $UFLAG pong" ;;
    pong) TARGET_CMD="$DDSPERF $UFLAG pong"
          PEER_CMD="$DDSPERF $UFLAG ping $RATE size $SIZE" ;;
    *) echo "ERROR: unknown ROLE '$ROLE' (want sub|pub|ping|pong)" >&2; exit 2 ;;
  esac
fi

# --- process bookkeeping -----------------------------------------------------
PEER_PID=""
TGT_PID=""

stop_all() {
  [ -n "$TGT_PID" ]  && kill "$TGT_PID"  2>/dev/null
  [ -n "$PEER_PID" ] && kill "$PEER_PID" 2>/dev/null
  # Belt and suspenders: mop up any strays from this binary.
  pkill -f "$DDSPERF" 2>/dev/null
  TGT_PID=""; PEER_PID=""
  sleep 1
}
trap 'stop_all' EXIT

start_peer() { # $1 = log path
  # shellcheck disable=SC2086
  $PEER_CMD > "$1" 2>&1 &
  PEER_PID=$!
}

echo "RustDDS execution profiler"
echo "  repo:      $REPO_DIR"
echo "  git:       $(git -C "$REPO_DIR" describe --tags --always --dirty 2>/dev/null || echo unknown)"
echo "  platform:  $(uname -srm)"
echo "  role:      $ROLE   reliable=$RELIABLE rate=$RATE size=${SIZE}B"
echo "  target:    $TARGET_CMD"
echo "  peer:      $PEER_CMD"
echo "  passes:    $PASSES"
echo "  duration:  ${DURATION}s (warmup ${WARMUP}s)"
echo "  results:   $OUTDIR"
echo

# --- build -------------------------------------------------------------------
if [ "${NO_BUILD:-0}" != "1" ] && [ -z "${TARGET_CMD_OVERRIDDEN:-}" ]; then
  if printf '%s %s' "$TARGET_CMD" "$PEER_CMD" | grep -q -- "$DDSPERF"; then
    echo "Building ddsperf (profile=profiling: release + debuginfo + dSYM)..."
    cargo build --profile profiling --example ddsperf || exit 1
  fi
fi
if printf '%s' "$TARGET_CMD" | grep -q -- "$DDSPERF" && [ ! -x "$DDSPERF" ]; then
  echo "ERROR: $DDSPERF not found/executable (set BIN_DIR or NO_BUILD/TARGET_CMD)" >&2
  exit 1
fi

{
  echo "timestamp=$STAMP"
  echo "label=$LABEL"
  echo "git=$(git -C "$REPO_DIR" describe --tags --always --dirty 2>/dev/null)"
  echo "commit=$(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null)"
  echo "platform=$(uname -srm)"
  echo "role=$ROLE reliable=$RELIABLE rate=$RATE size=$SIZE"
  echo "target_cmd=$TARGET_CMD"
  echo "peer_cmd=$PEER_CMD"
  echo "duration=$DURATION warmup=$WARMUP"
} > "$OUTDIR/run_info.txt"

# ============================================================================
# Pass 1: aux (built-in, no Xcode)
# ============================================================================
run_aux() {
  local dir="$OUTDIR/aux"; mkdir -p "$dir"
  echo "[aux] built-in thread/sample/spindump snapshot"

  start_peer "$dir/peer.log"
  sleep "$WARMUP"

  # shellcheck disable=SC2086
  $TARGET_CMD > "$dir/target.log" 2>&1 &
  TGT_PID=$!

  # Let it reach steady state before snapshotting.
  local settle=3
  [ "$DURATION" -lt 6 ] && settle=1
  sleep "$settle"

  if ! kill -0 "$TGT_PID" 2>/dev/null; then
    echo "  WARN: target exited early; see $dir/target.log" >&2
  else
    echo "  - thread inventory      -> aux/threads.txt"
    { echo "# ps -M $TGT_PID  ($(date))"; ps -M "$TGT_PID"; } > "$dir/threads.txt" 2>&1
    echo "  - on-CPU sample (${SAMPLE_SECS}s) -> aux/sample.txt"
    sample "$TGT_PID" "$SAMPLE_SECS" -f "$dir/sample.txt" >/dev/null 2>&1 \
      || echo "    (sample failed; try running the script with sudo)" >&2
    echo "  - spindump (blocking)   -> aux/spindump.txt"
    spindump "$TGT_PID" 3 -o "$dir/spindump.txt" >/dev/null 2>&1 \
      || echo "    (spindump needs sudo; skipped)" | tee "$dir/spindump.txt" >/dev/null
  fi

  # Run out the remaining duration so the peer log has steady-state stats too.
  local remain=$(( DURATION - settle ))
  [ "$remain" -gt 0 ] && sleep "$remain"
  stop_all
}

# ============================================================================
# Passes 2/3: xctrace (Time Profiler / System Trace)
# ============================================================================
run_xctrace() { # $1 = template name, $2 = short name
  local template="$1" name="$2"
  local trace="$OUTDIR/${name}.trace"
  rm -rf "$trace"
  echo "[$name] xctrace '$template' for ${DURATION}s -> ${name}.trace"

  if ! command -v xctrace >/dev/null 2>&1; then
    echo "  SKIP: xctrace not found (need Xcode: xcode-select -s /Applications/Xcode.app)" >&2
    return
  fi

  start_peer "$OUTDIR/${name}_peer.log"
  sleep "$WARMUP"

  # xctrace launches and owns the target; --time-limit ends the run and the
  # child. Target stdout is captured; there is no --target-stderr option.
  # shellcheck disable=SC2086
  xctrace record \
    --template "$template" \
    --time-limit "${DURATION}s" \
    --no-prompt \
    --output "$trace" \
    --target-stdout "$OUTDIR/${name}_target.log" \
    --launch -- $TARGET_CMD
  local rc=$?
  stop_all

  # xctrace often exits non-zero (e.g. 54) even on success, because the launched
  # target is terminated when --time-limit fires. Trust the saved trace bundle:
  # only treat a MISSING trace as failure.
  if [ ! -e "$trace" ]; then
    echo "  WARN: no trace saved (xctrace exit=$rc). If it's a permissions error," >&2
    echo "        enable Developer Tools access (or re-run this pass with sudo);" >&2
    echo "        System Trace in particular may need elevated privileges." >&2
    return
  fi
  [ $rc -ne 0 ] && echo "  note: xctrace exit=$rc but trace saved (benign at time-limit)."

  # Dump the schema table-of-contents so the trace's contents are discoverable
  # from the CLI (full analysis is best in the Instruments GUI: `open $trace`).
  xctrace export --input "$trace" --toc --output "$trace.toc.xml" 2>/dev/null \
    && echo "  - schema TOC -> ${name}.trace.toc.xml"
  echo "  - open in Instruments: open '$trace'"
}

for pass in $PASSES; do
  case "$pass" in
    aux)      run_aux ;;
    time)     run_xctrace "Time Profiler" "time" ;;
    systrace) run_xctrace "System Trace"  "systrace" ;;
    *) echo "WARN: unknown pass '$pass' (want aux|time|systrace)" >&2 ;;
  esac
  echo
done

# --- summary/readme ----------------------------------------------------------
cat > "$OUTDIR/README.txt" <<EOF
Execution profile: $ROLE (reliable=$RELIABLE rate=$RATE size=${SIZE}B)
Target: $TARGET_CMD
Peer:   $PEER_CMD

Artifacts:
  run_info.txt            scenario + git metadata
  aux/threads.txt         thread count & states (ps -M)
  aux/sample.txt          on-CPU call tree, text (sample) -- read this first
  aux/spindump.txt        per-thread stacks incl. blocked (needs sudo for full)
  aux/{target,peer}.log   ddsperf stdout (throughput/RTT/RSS)
  time.trace              on-CPU sampling      -> open time.trace
  systrace.trace          blocking/syscalls    -> open systrace.trace
  *.trace.toc.xml         trace schema listing (for xctrace export --xpath)

Where cycles go:      open time.trace      (Instruments Time Profiler)
Where threads block:  open systrace.trace  (Instruments System Trace)
Quick text view:      less aux/sample.txt  (heaviest stacks, no GUI)

Compare RustDDS vs CycloneDDS: run again with TARGET_CMD/PEER_CMD pointing at
the CycloneDDS ddsperf and diff aux/sample.txt + thread counts.
EOF

echo "===== done -> $OUTDIR ====="
echo "  read first:  less $OUTDIR/aux/sample.txt"
echo "  thread info: cat  $OUTDIR/aux/threads.txt"
echo "  cpu trace:   open $OUTDIR/time.trace"
echo "  block trace: open $OUTDIR/systrace.trace"
