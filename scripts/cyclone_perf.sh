#!/usr/bin/env bash
# CycloneDDS performance reference, mirroring scripts/perf_selftest.sh scenarios
# so RustDDS numbers can be compared apples-to-apples.
#
# Both stacks run over UDP/IP on the loopback interface (lo0). Shared-memory /
# PSMX transport is NOT built (ENABLE_ICEORYX=OFF) and is disabled here anyway,
# so this is a pure UDP/IP comparison.
#
# Usage:
#   scripts/cyclone_perf.sh
#
# Env overrides:
#   CYC_DIR       CycloneDDS source/build dir (default ../cyclonedds)
#   DDSPERF       path to cyclone ddsperf binary (default $CYC_DIR/build/bin/ddsperf)
#   DURATION      throughput seconds (10)
#   LAT_DURATION  latency seconds (8)
#   BIG_DURATION  large-object seconds (60)
#   OUTDIR        results dir

set -u

CYC_DIR="${CYC_DIR:-../cyclonedds}"
DDSPERF="${DDSPERF:-$CYC_DIR/build/bin/ddsperf}"
DURATION="${DURATION:-10}"
LAT_DURATION="${LAT_DURATION:-8}"
BIG_DURATION="${BIG_DURATION:-60}"

STAMP="$(date +%Y%m%d_%H%M%S)"
OUTDIR="${OUTDIR:-/tmp/cyclone_perf/${STAMP}}"
LOGDIR="$OUTDIR/logs"
SUMMARY="$OUTDIR/SUMMARY.csv"
mkdir -p "$LOGDIR"

# --- Force a specific interface, multicast off, no shared memory ------------
# Env knobs (defaults keep the historical loopback behaviour):
#   IFACE    network interface name to bind CycloneDDS to        (lo0)
#   PEER_IP  unicast discovery peer address on that interface    (127.0.0.1)
#   MAX_MSG  optional Internal/MaxMessageSize, e.g. "1472B" to    (unset)
#            emulate a 1500-byte Ethernet MTU. On loopback the real
#            path MTU is huge (macOS lo0 = 16384), so CycloneDDS otherwise
#            packs many DATA submessages into one large UDP datagram; capping
#            MaxMessageSize forces ~one 1 KB sample per datagram as it would be
#            on a 1500-MTU wire.
IFACE="${IFACE:-lo0}"
PEER_IP="${PEER_IP:-127.0.0.1}"
MAX_MSG="${MAX_MSG:-}"

INTERNAL_BLOCK=""
if [ -n "$MAX_MSG" ]; then
  INTERNAL_BLOCK="    <Internal><MaxMessageSize>${MAX_MSG}</MaxMessageSize></Internal>"
fi

CFG="$OUTDIR/cyclone_${IFACE}.xml"
{
  printf '%s\n' \
'<?xml version="1.0" encoding="UTF-8" ?>' \
'<CycloneDDS xmlns="https://cdds.io/config">' \
'  <Domain Id="any">' \
'    <General>' \
'      <Interfaces>' \
"        <NetworkInterface name=\"${IFACE}\" priority=\"default\" multicast=\"false\"/>" \
'      </Interfaces>' \
'      <AllowMulticast>false</AllowMulticast>' \
'      <EnableMulticastLoopback>false</EnableMulticastLoopback>' \
'    </General>'
  [ -n "$INTERNAL_BLOCK" ] && printf '%s\n' "$INTERNAL_BLOCK"
  printf '%s\n' \
'    <Discovery>' \
"      <Peers><Peer address=\"${PEER_IP}\"/></Peers>" \
'      <ParticipantIndex>auto</ParticipantIndex>' \
'    </Discovery>' \
'  </Domain>' \
'</CycloneDDS>'
} > "$CFG"
export CYCLONEDDS_URI="file://$CFG"

if [ ! -x "$DDSPERF" ]; then
  echo "ERROR: ddsperf not found/executable at $DDSPERF" >&2
  exit 1
fi

echo "CycloneDDS perf reference"
echo "  ddsperf:  $DDSPERF"
echo "  version:  $(git -C "$CYC_DIR" describe --tags --always 2>/dev/null)"
echo "  config:   $CFG (iface=$IFACE peer=$PEER_IP${MAX_MSG:+ MaxMessageSize=$MAX_MSG}, no multicast, no SHM)"
echo "  results:  $OUTDIR"
echo

cleanup() { pkill -f "$DDSPERF" 2>/dev/null; sleep 1; }
trap cleanup EXIT
cleanup

echo "scenario,type,reliability,rate_hz,payload_bytes,duration_s,samples_per_s,mbits_per_s,rtt_avg_us,rtt_max_us,sub_rss_start_mb,sub_rss_end_mb,sub_rss_max_mb" > "$SUMMARY"

# convert "<num><unit>" latency token to microseconds
to_us='function to_us(t,   v,u){ if(match(t,/[a-z]+$/)){u=substr(t,RSTART);v=substr(t,1,RSTART-1)+0}else{v=t+0;u="us"}
        if(u=="ns")return v/1000.0; if(u=="us")return v; if(u=="ms")return v*1000.0; if(u=="s")return v*1000000.0; return v }'

rss_mb() {  # parse "rss:NN.NMB|kB|GB" -> MB, one per line
  grep -oE 'rss:[0-9.]+[kMG]?B' "$1" | sed -E 's/rss://' | awk '
    { if(match($0,/[kMG]?B$/)){u=substr($0,RSTART,1)} else u="";
      v=$0+0; if(u=="k")v=v/1000.0; else if(u=="G")v=v*1000.0;
      printf "%.2f\n", v }'
}

# --- latency: name reliable(0/1) rate payload ------------------------------
run_lat() {
  local name="$1" rel="$2" rate="$3" pay="$4"
  local uflag rtxt; if [ "$rel" = 1 ]; then uflag=""; rtxt=reliable; else uflag="-u"; rtxt=best_effort; fi
  local pong="$LOGDIR/lat_${name}_pong.log" ping="$LOGDIR/lat_${name}_ping.log"
  echo "[latency]    $name: $rtxt rate=${rate}Hz size=${pay}B ${LAT_DURATION}s"
  # shellcheck disable=SC2086
  "$DDSPERF" $uflag -D"$((LAT_DURATION+2))" pong > "$pong" 2>&1 &
  sleep 1
  # shellcheck disable=SC2086
  "$DDSPERF" $uflag -D"$LAT_DURATION" ping "${rate}Hz" size "$pay" > "$ping" 2>&1 &
  sleep "$((LAT_DURATION+2))"; cleanup
  local parsed
  parsed="$(awk "$to_us"'
    / mean /{ for(i=1;i<=NF;i++){ if($i=="mean")m=to_us($(i+1)); if($i=="max")x=to_us($(i+1)); if($i=="cnt")c=$(i+1) }
      seen++; if(seen<=1) next; sum+=m; n++; if(x>mx)mx=x; if(c>0){csum+=c;cn++} }
    END{ if(n>0) printf "%.1f %.1f %.0f", sum/n, mx, (cn>0?csum/cn:0); else printf "NA NA 0" }' "$ping")"
  local avg="${parsed%% *}" rest="${parsed#* }"; local max="${rest%% *}"; local sps="${rest##* }"
  echo "             -> RTT avg ${avg}us max ${max}us, ${sps} samples/s"
  echo "${name},latency,${rtxt},${rate},${pay},${LAT_DURATION},${sps},NA,${avg},${max},NA,NA,NA" >> "$SUMMARY"
}

# --- throughput: name reliable(0/1) rate payload ---------------------------
run_tput() {
  local name="$1" rel="$2" rate="$3" pay="$4"
  local uflag rtxt; if [ "$rel" = 1 ]; then uflag=""; rtxt=reliable; else uflag="-u"; rtxt=best_effort; fi
  local sub="$LOGDIR/tput_${name}_sub.log" pub="$LOGDIR/tput_${name}_pub.log"
  echo "[throughput] $name: $rtxt rate=${rate}Hz size=${pay}B ${DURATION}s"
  # shellcheck disable=SC2086
  "$DDSPERF" $uflag -1 -D"$((DURATION+2))" sub > "$sub" 2>&1 &
  sleep 1
  # shellcheck disable=SC2086
  "$DDSPERF" $uflag -D"$DURATION" pub "${rate}Hz" size "$pay" > "$pub" 2>&1 &
  sleep "$((DURATION+2))"; cleanup
  local parsed
  parsed="$(awk '
    / rate /{ for(i=1;i<=NF;i++){ if($i=="rate"){r=$(i+1)+0; if($(i+2)=="kS/s")r=r*1000; mb=$(i+3)+0} }
      if(r>0){ sum+=r; mbs+=mb; n++ } }
    END{ if(n>0) printf "%.0f %.2f", sum/n, mbs/n; else printf "0 0" }' "$sub")"
  local sps="${parsed%% *}" mbps="${parsed##* }"
  echo "             -> ${sps} samples/s, ${mbps} Mb/s"
  echo "${name},throughput,${rtxt},${rate},${pay},${DURATION},${sps},${mbps},NA,NA,NA,NA,NA" >> "$SUMMARY"
}

# --- big-object leak watch: name rate size ---------------------------------
run_big() {
  local name="$1" rate="$2" size="$3"
  local sub="$LOGDIR/big_${name}_sub.log" pub="$LOGDIR/big_${name}_pub.log"
  echo "[bigobj]     $name: best_effort rate=${rate}Hz size=${size}B ${BIG_DURATION}s"
  "$DDSPERF" -u -1 -D"$((BIG_DURATION+2))" sub > "$sub" 2>&1 &
  sleep 1
  "$DDSPERF" -u -D"$BIG_DURATION" pub "${rate}Hz" size "$size" > "$pub" 2>&1 &
  sleep "$((BIG_DURATION+2))"; cleanup
  local sps
  # low rates round to 0.00 kS/s, so derive samples/s from per-interval "delta"
  sps="$(awk '/ delta /{for(i=1;i<=NF;i++)if($i=="delta"){d=$(i+1)+0; if(d>0){s+=d;n++}}} END{if(n>0)printf "%.1f",s/n; else printf 0}' "$sub")"
  local series start end mx
  series="$(rss_mb "$sub")"
  start="$(echo "$series"|head -1)"; end="$(echo "$series"|tail -1)"; mx="$(echo "$series"|sort -n|tail -1)"
  echo "             -> ${sps} samples/s; sub RSS ${start:-NA}->${end:-NA} MB (max ${mx:-NA})"
  echo "${name},bigobj,best_effort,${rate},${size},${BIG_DURATION},${sps},NA,NA,NA,${start:-NA},${end:-NA},${mx:-NA}" >> "$SUMMARY"
}

# ---- scenario matrix (mirrors RustDDS perf_selftest.sh) --------------------
run_tput be_64b         0 1000 64
run_tput be_1k          0 1000 1024
run_tput be_8k          0 1000 8192
run_tput be_large_frag  0 500  70000
run_tput rel_1k         1 1000 1024
run_tput rel_large_frag 1 200  70000

run_lat  be_100hz       0 100  64
run_lat  be_200hz       0 200  256
run_lat  rel_100hz      1 100  64

run_big  be_2mb_5hz     5 2M

echo
echo "===== CycloneDDS SUMMARY ($SUMMARY) ====="
column -s, -t "$SUMMARY" 2>/dev/null || cat "$SUMMARY"
echo
echo "Done: $OUTDIR"
