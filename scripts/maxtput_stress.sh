#!/usr/bin/env bash
# Max-throughput + traffic-pressure ("does it collapse?") harness.
#
# Publishes FLAT OUT (no per-sample pacing) and measures what the subscriber
# actually sustains, for both RustDDS (in-tree `ddsperf`, patched so `pub 0`
# means flat out) and CycloneDDS (`ddsperf pub` with no rate = flat out).
#
# For each scenario it records the per-second received series so we can tell a
# healthy plateau from a collapse (rate decaying toward 0, RSS growing without
# bound, or the process dying).
#
# Usage:
#   FLAVOR=rustdds BIN_DIR=/tmp/rdds-tgt/master/release/examples \
#     OUTDIR=/tmp/mtp/master scripts/maxtput_stress.sh
#   FLAVOR=cyclone DDSPERF=../cyclonedds/build/bin/ddsperf \
#     OUTDIR=/tmp/mtp/cyclone scripts/maxtput_stress.sh
#
# Env:
#   FLAVOR         rustdds | cyclone   (required)
#   BIN_DIR        (rustdds) dir with ddsperf                (default target/release/examples)
#   DDSPERF        (cyclone) path to ddsperf binary
#   CYC_DIR        (cyclone) source dir, for the loopback XML (default ../cyclonedds)
#   DURATION       seconds per max-throughput scenario       (12)
#   STRESS_DURATION seconds per stress scenario              (60)
#   OUTDIR         results dir                                (/tmp/mtp/<flavor>_<ts>)

set -u

FLAVOR="${FLAVOR:?set FLAVOR=rustdds|cyclone}"
DURATION="${DURATION:-12}"
STRESS_DURATION="${STRESS_DURATION:-60}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUTDIR="${OUTDIR:-/tmp/mtp/${FLAVOR}_${STAMP}}"
LOGDIR="$OUTDIR/logs"
SUMMARY="$OUTDIR/SUMMARY_MAXTPUT.csv"
mkdir -p "$LOGDIR"

if [ "$FLAVOR" = rustdds ]; then
  BIN_DIR="${BIN_DIR:-target/release/examples}"
  DDSPERF="$BIN_DIR/ddsperf"
elif [ "$FLAVOR" = cyclone ]; then
  CYC_DIR="${CYC_DIR:-../cyclonedds}"
  DDSPERF="${DDSPERF:-$CYC_DIR/build/bin/ddsperf}"
  # Force loopback UDP, no multicast, no shared memory (apples-to-apples w/ RustDDS).
  CFG="$OUTDIR/cyclone_loopback.xml"
  printf '%s\n' \
    '<?xml version="1.0" encoding="UTF-8" ?>' \
    '<CycloneDDS xmlns="https://cdds.io/config">' \
    '  <Domain Id="any">' \
    '    <General>' \
    '      <Interfaces>' \
    '        <NetworkInterface name="lo0" priority="default" multicast="false"/>' \
    '      </Interfaces>' \
    '      <AllowMulticast>false</AllowMulticast>' \
    '      <EnableMulticastLoopback>false</EnableMulticastLoopback>' \
    '    </General>' \
    '    <Discovery>' \
    '      <Peers><Peer address="127.0.0.1"/></Peers>' \
    '      <ParticipantIndex>auto</ParticipantIndex>' \
    '    </Discovery>' \
    '  </Domain>' \
    '</CycloneDDS>' > "$CFG"
  export CYCLONEDDS_URI="file://$CFG"
else
  echo "unknown FLAVOR=$FLAVOR" >&2; exit 1
fi

if [ ! -x "$DDSPERF" ]; then echo "ERROR: ddsperf not executable at $DDSPERF" >&2; exit 1; fi

cleanup() { pkill -f "$DDSPERF" 2>/dev/null; sleep 1; }
trap cleanup EXIT
cleanup

echo "Max-throughput / stress harness"
echo "  flavor:   $FLAVOR"
echo "  ddsperf:  $DDSPERF"
echo "  duration: ${DURATION}s (tput) / ${STRESS_DURATION}s (stress)"
echo "  results:  $OUTDIR"
echo

echo "scenario,flavor,type,reliability,payload_bytes,duration_s,mean_samples_per_s,mbytes_per_s,min_sps,max_sps,last_sps,collapse_ratio,rss_start_mb,rss_end_mb,rss_max_mb,survived" > "$SUMMARY"

# --- convert a format_count token like "1.23M"/"456k"/"789" to a plain number
CONV='function conv(t,  v,u){ v=t+0; u=t; sub(/^[0-9.]+/,"",u);
        if(u=="k")return v*1000; if(u=="M")return v*1e6; if(u=="G")return v*1e9; return v }'

# --- per-second received-samples series (one value per line) ----------------
# RustDDS sub line:  "<N> samples <M> bytes"          (N,M are format_count)
# Cyclone -1 sub:    "... rate <R> [kS/s] <MB> Mb/s"  (R kS/s -> *1000)
series_sps() {
  local log="$1"
  if [ "$FLAVOR" = rustdds ]; then
    # awk field-splitting collapses runs of whitespace, so match loosely on the
    # words and read $1 (samples) regardless of format_count's padding.
    awk "$CONV"'/samples/ && /bytes/ { print conv($1) }' "$log"
  else
    awk '/ rate /{ for(i=1;i<=NF;i++) if($i=="rate"){ r=$(i+1)+0; if($(i+2)=="kS/s")r*=1000; print r } }' "$log"
  fi
}
# --- per-second received bytes/s series (MB/s) ------------------------------
series_mbps() {
  local log="$1"
  if [ "$FLAVOR" = rustdds ]; then
    awk "$CONV"'/samples/ && /bytes/ { printf "%.3f\n", conv($3)/1e6 }' "$log"
  else
    # cyclone prints Mbit/s; convert to MB/s (/8)
    awk '/ rate /{ for(i=1;i<=NF;i++) if($i=="Mb/s"){ printf "%.3f\n", ($(i-1)+0)/8.0 } }' "$log"
  fi
}
# --- RSS (MB) series --------------------------------------------------------
# RustDDS: "... RSS <n>[kMG]B"   Cyclone: "rss:<n>[kMG]B"
series_rss() {
  local log="$1"
  if [ "$FLAVOR" = rustdds ]; then
    sed -nE 's/.*RSS[[:space:]]*([0-9.]+)([kMG]?)B.*/\1 \2/p' "$log" | awk '
      { v=$1; u=$2; if(u=="k")v/=1000; else if(u=="G")v*=1000; else if(u=="")v/=1e6; printf "%.2f\n", v }'
  else
    grep -oE 'rss:[0-9.]+[kMG]?B' "$log" | sed -E 's/rss://' | awk '
      { u=""; if(match($0,/[kMG]B$/))u=substr($0,RSTART,1); v=$0+0;
        if(u=="k")v/=1000; else if(u=="G")v*=1000; printf "%.2f\n", v }'
  fi
}

run_scenario() {  # scenario_kind(tput/stress) name reliable size dur
  local kind="$1" name="$2" rel="$3" size="$4" dur="$5"
  local uflag rtxt
  [ "$rel" = 1 ] && { uflag=""; rtxt=reliable; } || { uflag="-u"; rtxt=best_effort; }
  local sub="$LOGDIR/${kind}_${name}_sub.log" pub="$LOGDIR/${kind}_${name}_pub.log"

  echo "[$kind] $name: $rtxt flat-out size=${size}B ${dur}s"

  if [ "$FLAVOR" = rustdds ]; then
    # shellcheck disable=SC2086
    "$DDSPERF" $uflag sub > "$sub" 2>&1 &
    local subpid=$!
    sleep 2
    # shellcheck disable=SC2086
    "$DDSPERF" $uflag pub 0 size "$size" > "$pub" 2>&1 &
    local pubpid=$!
    sleep "$dur"
    kill "$pubpid" "$subpid" 2>/dev/null
    wait "$pubpid" 2>/dev/null; wait "$subpid" 2>/dev/null
  else
    # shellcheck disable=SC2086
    "$DDSPERF" $uflag -1 -D"$((dur+3))" sub > "$sub" 2>&1 &
    local subpid=$!
    sleep 2
    # shellcheck disable=SC2086
    "$DDSPERF" $uflag -D"$dur" pub size "$size" > "$pub" 2>&1 &
    local pubpid=$!
    sleep "$((dur+3))"
    kill "$pubpid" "$subpid" 2>/dev/null
    wait "$pubpid" 2>/dev/null; wait "$subpid" 2>/dev/null
  fi

  # Was the subscriber still alive at the end (i.e. no panic/abort)?
  local survived="yes"
  grep -qiE 'panic|thread .* panicked|abort|fatal' "$sub" && survived="no"

  # Received-rate stats over the steady state. Skip only the LEADING zero ticks
  # (discovery/matching before any data flows); keep every tick afterwards so a
  # mid-run drop to zero (a real collapse) is counted.
  local sps_series; sps_series="$(series_sps "$sub")"
  local stats; stats="$(printf '%s\n' "$sps_series" | awk '
    { x=$1+0; if(!started){ if(x>0){started=1} else next } }
    { sum+=x; n++; if(min==""||x<min)min=x; if(x>max)max=x; last=x;
        # split into thirds for collapse detection
        vals[n]=x }
    END{
      if(n==0){ print "0 0 0 0 0 1"; exit }
      # collapse_ratio = mean(last third) / mean(first third); <1 means decay
      t=int(n/3); if(t<1)t=1;
      for(i=1;i<=t;i++)f+=vals[i];
      for(i=n-t+1;i<=n;i++)l+=vals[i];
      fr=(f/t); lr=(l/t); cr=(fr>0? lr/fr : 0);
      printf "%.0f %.0f %.0f %.0f %.2f %d", sum/n, min, max, last, cr, n }')"
  local mean_sps min_sps max_sps last_sps collapse n_ticks
  mean_sps="$(echo "$stats"|awk '{print $1}')"
  min_sps="$(echo "$stats"|awk '{print $2}')"
  max_sps="$(echo "$stats"|awk '{print $3}')"
  last_sps="$(echo "$stats"|awk '{print $4}')"
  collapse="$(echo "$stats"|awk '{print $5}')"

  local mean_mbps; mean_mbps="$(series_mbps "$sub" | awk '{x=$1+0; if(!st){if(x==0)next; st=1} s+=x; n++} END{if(n>0)printf "%.2f",s/n; else print 0}')"

  local rss_series rss_start rss_end rss_max
  rss_series="$(series_rss "$sub")"
  rss_start="$(echo "$rss_series"|head -n1)"; rss_start="${rss_start:-NA}"
  rss_end="$(echo "$rss_series"|tail -n1)"; rss_end="${rss_end:-NA}"
  rss_max="$(echo "$rss_series"|sort -n|tail -n1)"; rss_max="${rss_max:-NA}"

  echo "             -> mean ${mean_sps}/s (min ${min_sps}, max ${max_sps}, last ${last_sps}), ${mean_mbps} MB/s, collapse_ratio ${collapse}, RSS ${rss_start}->${rss_end} (max ${rss_max}) MB, survived=${survived}"
  # Save the raw per-second series for plotting/inspection.
  printf '%s\n' "$sps_series" > "$LOGDIR/${kind}_${name}_sps_series.txt"
  printf '%s\n' "$rss_series" > "$LOGDIR/${kind}_${name}_rss_series.txt"

  echo "${name},${FLAVOR},${kind},${rtxt},${size},${dur},${mean_sps},${mean_mbps},${min_sps},${max_sps},${last_sps},${collapse},${rss_start},${rss_end},${rss_max},${survived}" >> "$SUMMARY"
  cleanup
}

# ---- max-throughput sweep (short) -----------------------------------------
run_scenario tput be_64b   0 64    "$DURATION"
run_scenario tput be_1k    0 1024  "$DURATION"
run_scenario tput be_8k    0 8192  "$DURATION"
run_scenario tput be_64k   0 65000 "$DURATION"
run_scenario tput rel_1k   1 1024  "$DURATION"
run_scenario tput rel_8k   1 8192  "$DURATION"

# ---- sustained traffic-pressure / collapse watch (long) --------------------
run_scenario stress be_64b_stress 0 64    "$STRESS_DURATION"
run_scenario stress rel_1k_stress 1 1024  "$STRESS_DURATION"
run_scenario stress be_64k_stress 0 65000 "$STRESS_DURATION"

echo
echo "===== MAX-THROUGHPUT/STRESS SUMMARY ($SUMMARY) ====="
column -s, -t "$SUMMARY" 2>/dev/null || cat "$SUMMARY"
echo
echo "Done: $OUTDIR"
