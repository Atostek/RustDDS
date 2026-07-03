#!/usr/bin/env bash
# RustDDS local (loopback) performance self-test harness.
#
# Runs a fixed set of RustDDS-vs-RustDDS throughput and latency scenarios over
# loopback UDP and writes machine-readable results, so runs can be compared
# later (e.g. before/after a receive-path change). It is intentionally
# self-contained and portable to macOS and Linux: it does NOT depend on
# setsid / GNU timeout / the OMG dds-rtps vendor binaries.
#
# It uses two in-tree examples:
#   - shape_main : throughput. The subscriber prints exactly one line per
#                  received sample, so the received count is exact.
#   - ddsperf    : latency. ping/pong reports round-trip time.
#
# Usage:
#   scripts/perf_selftest.sh [LABEL]
#
#   LABEL is an optional tag folded into the results directory name, handy for
#   marking a build/commit (e.g. `scripts/perf_selftest.sh before-fix`).
#
# Environment overrides (defaults in parentheses):
#   DOMAIN        DDS domain id (0)
#   DURATION      seconds per throughput scenario (10)
#   LAT_DURATION  seconds per latency scenario (8)
#   OUTDIR        results directory (target/perf/<timestamp>[_<label>])
#   BIN_DIR       examples dir (${CARGO_TARGET_DIR:-target}/release/examples)
#   NO_BUILD      set to 1 to skip the cargo build step
#
# Output:
#   <OUTDIR>/SUMMARY.csv   one row per scenario (see header below)
#   <OUTDIR>/logs/*.log    raw stdout/stderr per process
#   a summary table on the console
#
# Compare two runs later with e.g.:
#   column -s, -t <OUTDIR>/SUMMARY.csv

set -u

LABEL="${1:-}"
DOMAIN="${DOMAIN:-0}"
DURATION="${DURATION:-10}"
LAT_DURATION="${LAT_DURATION:-8}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR" || exit 1

BIN_DIR="${BIN_DIR:-${CARGO_TARGET_DIR:-target}/release/examples}"

STAMP="$(date +%Y%m%d_%H%M%S)"
if [ -n "$LABEL" ]; then
  OUTDIR="${OUTDIR:-target/perf/${STAMP}_${LABEL}}"
else
  OUTDIR="${OUTDIR:-target/perf/${STAMP}}"
fi
LOGDIR="${OUTDIR}/logs"
SUMMARY="${OUTDIR}/SUMMARY.csv"
mkdir -p "$LOGDIR"

# Per-sample ShapeType overhead (bytes) beyond the -B payload: color string,
# x/y/shapesize, sequence length, and RTPS/CDR framing. Approximate; used only
# for the throughput MB/s estimate, which is labelled "approx".
SHAPE_BASE=48

SHAPE="$BIN_DIR/shape_main"
DDSPERF="$BIN_DIR/ddsperf"

cleanup_procs() {
  pkill -f "$BIN_DIR/shape_main" 2>/dev/null
  pkill -f "$BIN_DIR/ddsperf"    2>/dev/null
  sleep 1
}
trap 'cleanup_procs' EXIT

echo "RustDDS perf self-test"
echo "  repo:        $REPO_DIR"
echo "  git:         $(git -C "$REPO_DIR" describe --tags --always --dirty 2>/dev/null || echo unknown)"
echo "  branch:      $(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
echo "  platform:    $(uname -srmo 2>/dev/null || uname -srm)"
echo "  domain:      $DOMAIN"
echo "  duration:    ${DURATION}s throughput / ${LAT_DURATION}s latency"
echo "  results:     $OUTDIR"
echo

if [ "${NO_BUILD:-0}" != "1" ]; then
  echo "Building examples (release)..."
  cargo build --release --example shape_main --example ddsperf || exit 1
fi

if [ ! -x "$SHAPE" ] || [ ! -x "$DDSPERF" ]; then
  echo "ERROR: example binaries not found under $BIN_DIR" >&2
  echo "       (set BIN_DIR or CARGO_TARGET_DIR appropriately)" >&2
  exit 1
fi

# Record environment for later comparison.
{
  echo "timestamp=$STAMP"
  echo "label=$LABEL"
  echo "git=$(git -C "$REPO_DIR" describe --tags --always --dirty 2>/dev/null)"
  echo "branch=$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null)"
  echo "commit=$(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null)"
  echo "platform=$(uname -srmo 2>/dev/null || uname -srm)"
  echo "domain=$DOMAIN"
  echo "duration=$DURATION"
  echo "lat_duration=$LAT_DURATION"
} > "${OUTDIR}/run_info.txt"

echo "scenario,type,reliability,rate_hz,payload_bytes,duration_s,samples,samples_per_s,approx_mbytes_per_s,rtt_avg_us,rtt_max_us" > "$SUMMARY"

cleanup_procs

# --- Throughput via shape_main ---------------------------------------------
# args: name reliable(0/1) write_period_ms payload_bytes
run_throughput() {
  local name="$1" reliable="$2" period="$3" payload="$4"
  local topic="Perf_${name}"
  local rel_flag rel_txt
  if [ "$reliable" = "1" ]; then rel_flag="-r"; rel_txt="reliable"; else rel_flag="-b"; rel_txt="best_effort"; fi

  local sub_log="${LOGDIR}/tput_${name}_sub.log"
  local pub_log="${LOGDIR}/tput_${name}_pub.log"

  echo "[throughput] ${name}: ${rel_txt}, period=${period}ms, payload=${payload}B, ${DURATION}s"

  "$SHAPE" -S -d "$DOMAIN" -t "$topic" "$rel_flag" -c BLUE > "$sub_log" 2>&1 &
  local sub=$!
  sleep 2   # allow discovery/matching before publishing
  "$SHAPE" -P -d "$DOMAIN" -t "$topic" "$rel_flag" -c BLUE \
      --write-period "$period" -B "$payload" > "$pub_log" 2>&1 &
  local pub=$!

  sleep "$DURATION"
  kill "$pub" "$sub" 2>/dev/null
  wait "$pub" 2>/dev/null
  wait "$sub" 2>/dev/null

  # Count received samples. The subscriber prints exactly one data line per
  # sample, each containing a "[<shapesize>]" token (and nothing else does).
  # NB: the printed topic name is padded/truncated to a fixed width, so we must
  # not match on the topic string here.
  local samples rate mbps
  samples="$(grep -cE '\[[0-9]+\]' "$sub_log" 2>/dev/null)"
  samples="${samples:-0}"
  rate="$(awk -v s="$samples" -v d="$DURATION" 'BEGIN{printf "%.0f", s/d}')"
  mbps="$(awk -v r="$rate" -v p="$payload" -v b="$SHAPE_BASE" 'BEGIN{printf "%.2f", r*(p+b)/1000000.0}')"

  echo "             -> ${samples} samples, ${rate}/s, ~${mbps} MB/s"
  echo "${name},throughput,${rel_txt},NA,${payload},${DURATION},${samples},${rate},${mbps},NA,NA" >> "$SUMMARY"
  cleanup_procs
}

# --- Latency via ddsperf ping/pong -----------------------------------------
# args: name reliable(0/1) rate_hz payload_bytes
run_latency() {
  local name="$1" reliable="$2" rate="$3" payload="$4"
  local rel_flag rel_txt
  if [ "$reliable" = "1" ]; then rel_flag=""; rel_txt="reliable"; else rel_flag="-u"; rel_txt="best_effort"; fi

  local ping_log="${LOGDIR}/lat_${name}_ping.log"
  local pong_log="${LOGDIR}/lat_${name}_pong.log"

  echo "[latency]    ${name}: ${rel_txt}, rate=${rate}Hz, payload=${payload}B, ${LAT_DURATION}s"

  # shellcheck disable=SC2086
  "$DDSPERF" $rel_flag pong > "$pong_log" 2>&1 &
  local pong=$!
  sleep 1
  # shellcheck disable=SC2086
  "$DDSPERF" $rel_flag ping "$rate" size "$payload" > "$ping_log" 2>&1 &
  local ping=$!

  sleep "$LAT_DURATION"
  kill "$ping" "$pong" 2>/dev/null
  wait "$ping" 2>/dev/null
  wait "$pong" 2>/dev/null

  # Parse steady-state RTT lines. format_duration prints "<n> μs", "<n> ms" or
  # "<n>sec" (note: no space before "sec"), so normalise with sed first, then
  # convert everything to microseconds. Skip the first two RTT lines (discovery
  # warm-up) and any line whose avg is 0. Report the mean of per-second averages
  # and the max of per-second maxima.
  local parsed
  parsed="$(sed -E 's/([0-9])sec/\1 sec/g' "$ping_log" | awk -F'RTT avg ' '
    /RTT avg/ {
      seen++; if (seen<=2) next;   # drop warm-up
      s=$2; gsub(/,/," ",s); split(s,a," ");
      # a[1]=avg a[2]=unit a[3]="max" a[4]=maxval a[5]=unit
      avg=a[1]*unit(a[2]); mx=a[4]*unit(a[5]);
      if (avg>0) { sum+=avg; cnt++; if (mx>maxv) maxv=mx; }
    }
    function unit(u){ if(u=="ms")return 1000; if(u=="sec")return 1000000; return 1; }
    END{ if(cnt>0) printf "%.0f %.0f", sum/cnt, maxv; else printf "NA NA"; }
  ')"
  local rtt_avg rtt_max samp_per_s
  rtt_avg="${parsed%% *}"
  rtt_max="${parsed##* }"
  # Average received samples/s over the steady-state RTT lines (skip 2 warm-up).
  samp_per_s="$(awk '/RTT avg/ {seen++; if(seen>2){sum+=$1;cnt++}} END{if(cnt>0)printf "%.0f", sum/cnt; else printf "0"}' "$ping_log")"

  echo "             -> RTT avg ${rtt_avg} us, max ${rtt_max} us, ${samp_per_s} samples/s"
  echo "${name},latency,${rel_txt},${rate},${payload},${LAT_DURATION},NA,${samp_per_s},NA,${rtt_avg},${rtt_max}" >> "$SUMMARY"
  cleanup_procs
}

# ---------------------------------------------------------------------------
# Scenario matrix. The small/medium/large payloads deliberately span the UDP
# datagram size: `large` (> 64 KiB) forces RTPS fragmentation + reassembly.
# ---------------------------------------------------------------------------

# Throughput (name, reliable, write_period_ms, payload_bytes)
# NB: write_period_ms must be > 0. A period of 0 starves the single-threaded
# smol executor used by the example (discovery never completes).
run_throughput "be_64b"         0 1    64       # best-effort, tiny payload
run_throughput "be_1k"          0 1    1024     # best-effort, ~1 KB
run_throughput "be_8k"          0 1    8192     # best-effort, ~8 KB
run_throughput "be_large_frag"  0 2    70000    # best-effort, > 64 KiB -> fragmented
run_throughput "rel_1k"         1 1    1024     # reliable, ~1 KB
run_throughput "rel_large_frag" 1 5    70000    # reliable, > 64 KiB -> fragmented

# Latency (name, reliable, rate_hz, payload_bytes)
run_latency "be_100hz"   0 100 64
run_latency "be_200hz"   0 200 256
run_latency "rel_100hz"  1 100 64

echo
echo "===== SUMMARY (${OUTDIR}/SUMMARY.csv) ====="
if command -v column >/dev/null 2>&1; then
  column -s, -t "$SUMMARY"
else
  cat "$SUMMARY"
fi
echo
echo "Done. Results saved under: $OUTDIR"
