#!/usr/bin/env bash
# Flat-out throughput comparison that makes the effect of the network MTU on
# CycloneDDS's DATA-submessage packing explicit.
#
# Background
# ----------
# CycloneDDS reaches very high 1 KB throughput on loopback by packing many RTPS
# DATA submessages into one large UDP datagram. That is only possible because
# the loopback MTU is huge (macOS lo0 = 16384), so a ~13.5 KB datagram carrying
# ~13 samples fits in a single, unfragmented datagram. On a real 1500-byte
# Ethernet link a 1 KB sample is ~one datagram, so the packing advantage largely
# disappears.
#
# macOS caveat: on a single host, UDP addressed to a local interface IP
# (e.g. en0's 192.168.1.161) is short-circuited through the kernel loopback
# path, so it still sees the 16384-byte loopback MTU regardless of which
# interface we "bind" to. Lowering lo0's MTU needs sudo. The sudo-free way to
# emulate a 1500-byte wire is therefore to cap each RTPS message (UDP datagram)
# to one MTU. For CycloneDDS that is Internal/MaxMessageSize; RustDDS already
# emits ~one sample per datagram, so it is unaffected.
#
# This script forces both stacks onto a chosen interface and reports, per
# scenario: delivered samples/s, UDP datagrams sent/s, IP fragments created,
# and the resulting samples-per-datagram.
#
# Usage:
#   scripts/mtu_compare.sh          # needs to read kernel netstat counters
#
# Forcing a *physical* interface on a single host is not physically meaningful:
# UDP to a local IP is short-circuited through the kernel loopback path, so it
# keeps the 16384-byte lo0 MTU regardless of interface. Worse, RustDDS same-host
# discovery relies on multicast that only loops back over lo0, so pinning both
# ends to en0 makes them never discover each other. We therefore run on the real
# same-host path (lo0 / 127.0.0.1) and emulate a 1500-byte wire with a datagram
# cap. Set IFACE/LOCAL_IP to test a physical interface anyway (expect breakage).
#
# Env overrides:
#   IFACE     interface name for CycloneDDS           (lo0)
#   LOCAL_IP  discovery peer / RustDDS locator IPv4    (127.0.0.1)
#   RUSTDDS_IFACE  if set, force RustDDS onto this IP  (unset: all interfaces)
#   MTU_MSG   CycloneDDS MaxMessageSize for the capped run (1472B ~= 1500 MTU)
#   SIZE      payload bytes                            (1024)
#   DUR       seconds per scenario                     (8)
#   CYC       CycloneDDS ddsperf path                  (~/cyclonedds/build/bin/ddsperf)
#   MASTER    RustDDS ddsperf path       (target/profiling/examples/ddsperf)

set -u

IFACE="${IFACE:-lo0}"
LOCAL_IP="${LOCAL_IP:-127.0.0.1}"
RUSTDDS_IFACE="${RUSTDDS_IFACE:-}"
MTU_MSG="${MTU_MSG:-1472B}"
SIZE="${SIZE:-1024}"
DUR="${DUR:-8}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CYC="${CYC:-$HOME/cyclonedds/build/bin/ddsperf}"
MASTER="${MASTER:-$REPO_DIR/target/profiling/examples/ddsperf}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/mtucmp.XXXXXX")"
trap 'pkill -f "$CYC" 2>/dev/null; pkill -f "$MASTER" 2>/dev/null; rm -rf "$WORK"' EXIT

for b in "$CYC" "$MASTER"; do
  [ -x "$b" ] || { echo "ERROR: not executable: $b" >&2; exit 1; }
done

# --- interface / MTU sanity -------------------------------------------------
IF_MTU="$(ifconfig "$IFACE" 2>/dev/null | awk '/mtu/{print $NF}')"
echo "CycloneDDS interface $IFACE: mtu=${IF_MTU:-?} peer=$LOCAL_IP"
echo "lo0: mtu=$(ifconfig lo0 2>/dev/null | awk '/mtu/{print $NF}') (same-host traffic uses this path)"
echo "RustDDS: ${RUSTDDS_IFACE:+forced onto $RUSTDDS_IFACE}${RUSTDDS_IFACE:-all interfaces (normal same-host discovery)}"
echo "MTU emulation for capped run: MaxMessageSize=$MTU_MSG (~1500-byte Ethernet)"
echo "payload=${SIZE}B duration=${DUR}s"
echo

udp_out(){ netstat -s -p udp | awk '/datagrams output/{print $1; exit}'; }
frag()   { netstat -s -p ip  | awk '/fragments created/{print $1; exit}'; }

# Parse steady-state delivered kS/s from a CycloneDDS sub log ("rate N kS/s").
cyc_rate(){ awk '/ rate /{for(i=1;i<=NF;i++) if($i=="rate"){v=$(i+1)+0; n++; if(n>2){s+=v;c++}}}
                 END{if(c>0)printf "%.1f",s/c; else printf "0"}' "$1"; }
# Parse steady-state delivered kS/s from a RustDDS sub log ("<count> samples ... bytes").
rust_rate(){ awk '/samples/ && /bytes/{t=$1; u=substr(t,length(t),1); v=t+0;
                    if(u=="M")v*=1000; else if(u=="k")v=v; else v/=1000;
                    n++; if(n>2){s+=v;c++}}
                  END{if(c>0)printf "%.1f",s/c; else printf "0"}' "$1"; }

RESULTS="$WORK/results.csv"
echo "scenario,delivered_kSs,datagrams_per_s,ip_fragments,samples_per_datagram" > "$RESULTS"

# run <label> <rate_parser> <sub_cmd> <pub_cmd>
run() {
  local label="$1" parser="$2" sub="$3" pub="$4"
  local key; key="$(printf '%s' "$label" | tr -c 'A-Za-z0-9' '_')"
  pkill -f "$CYC" 2>/dev/null; pkill -f "$MASTER" 2>/dev/null; sleep 1
  local u0 f0 u1 f1
  u0=$(udp_out); f0=$(frag)
  eval "$sub" > "$WORK/${key}_sub.txt" 2>&1 &
  sleep 1
  eval "$pub" > "$WORK/${key}_pub.txt" 2>&1 &
  sleep "$DUR"
  pkill -f "$CYC" 2>/dev/null; pkill -f "$MASTER" 2>/dev/null; sleep 1
  u1=$(udp_out); f1=$(frag)

  local dg_total dg_ps kss frags spd
  dg_total=$((u1-u0)); dg_ps=$((dg_total/DUR)); frags=$((f1-f0))
  kss=$($parser "$WORK/${key}_sub.txt")
  # samples per datagram = delivered samples / datagrams sent (>=1 => packing)
  spd=$(awk -v k="$kss" -v d="$dg_ps" 'BEGIN{if(d>0)printf "%.2f",(k*1000)/d; else printf "NA"}')

  printf "%-26s delivered=%8s kS/s  datagrams=%8d/s  ip_frag=%-6d  samples/datagram=%s\n" \
    "$label" "$kss" "$dg_ps" "$frags" "$spd"
  echo "${label},${kss},${dg_ps},${frags},${spd}" >> "$RESULTS"
}

# CycloneDDS config generator: $1=cfg path, $2=optional MaxMessageSize
cyc_cfg() {
  local cfg="$1" maxmsg="${2:-}"
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
    [ -n "$maxmsg" ] && printf '    <Internal><MaxMessageSize>%s</MaxMessageSize></Internal>\n' "$maxmsg"
    printf '%s\n' \
'    <Discovery>' \
"      <Peers><Peer address=\"${LOCAL_IP}\"/></Peers>" \
'      <ParticipantIndex>auto</ParticipantIndex>' \
'    </Discovery>' \
'  </Domain>' \
'</CycloneDDS>'
  } > "$cfg"
}

CFG_UNCAPPED="$WORK/cyc_uncapped.xml"; cyc_cfg "$CFG_UNCAPPED"
CFG_CAPPED="$WORK/cyc_capped.xml";     cyc_cfg "$CFG_CAPPED" "$MTU_MSG"

echo "== best-effort flat-out, ${SIZE}B, forced onto $IFACE ($LOCAL_IP) =="
run "cyclone (uncapped/pack)" cyc_rate \
  "CYCLONEDDS_URI=file://$CFG_UNCAPPED $CYC -u -1 sub" \
  "CYCLONEDDS_URI=file://$CFG_UNCAPPED $CYC -u pub size $SIZE"

run "cyclone (MTU=${MTU_MSG})" cyc_rate \
  "CYCLONEDDS_URI=file://$CFG_CAPPED $CYC -u -1 sub" \
  "CYCLONEDDS_URI=file://$CFG_CAPPED $CYC -u pub size $SIZE"

RD_PREFIX="${RUSTDDS_IFACE:+RUSTDDS_IFACE=$RUSTDDS_IFACE }"
run "rustdds (1/datagram)" rust_rate \
  "${RD_PREFIX}$MASTER -u sub" \
  "${RD_PREFIX}$MASTER -u pub 0 size $SIZE"

echo
echo "===== SUMMARY ====="
column -s, -t "$RESULTS" 2>/dev/null || cat "$RESULTS"
