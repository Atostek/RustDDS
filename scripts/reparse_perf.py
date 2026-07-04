#!/usr/bin/env python3
"""Recompute the max-throughput / stress summary from saved ddsperf sub logs.

Robust to RustDDS format_count padding (e.g. " 206M") that broke the shell
parser. Reads <BASE>/mtp_<label>/logs/*_sub.log and prints a combined table.

Usage: reparse_perf.py <BASE_DIR>
  where BASE_DIR contains mtp_master, mtp_v0130, mtp_cyclone (any subset).
"""
import os
import re
import sys
import glob

def conv(tok: str) -> float:
    """'1.23M'/'456k'/'789' -> number."""
    m = re.match(r'^([0-9.]+)([kMG]?)$', tok)
    if not m:
        return 0.0
    v = float(m.group(1))
    return v * {'': 1, 'k': 1e3, 'M': 1e6, 'G': 1e9}[m.group(2)]

def rustdds_series(path):
    """Return (sps_list, mbps_list, rss_list, matched_tick) from a RustDDS sub log."""
    sps, mbps, rss = [], [], []
    matched_tick = None
    tick = 0
    with open(path, errors='replace') as f:
        for line in f:
            if 'Matched with publisher' in line and matched_tick is None:
                matched_tick = tick
            if 'samples' in line and 'bytes' in line:
                tick += 1
                parts = line.split()
                # parts: [<samples> 'samples' <bytes> 'bytes']
                try:
                    sps.append(conv(parts[0]))
                    mbps.append(conv(parts[2]) / 1e6)
                except (IndexError, ValueError):
                    pass
            m = re.search(r'RSS\s*([0-9.]+)([kMG]?)B', line)
            if m:
                v = float(m.group(1))
                v = {'': v/1e6, 'k': v/1e3, 'M': v, 'G': v*1e3}[m.group(2)]
                rss.append(v)
    return sps, mbps, rss, matched_tick

def cyclone_series(path):
    sps, mbps, rss = [], [], []
    matched_tick = None
    tick = 0
    with open(path, errors='replace') as f:
        for line in f:
            if ': new' in line and 'self' not in line and matched_tick is None:
                matched_tick = tick
            # instantaneous per-interval: "... rate <R> kS/s <M> Mb/s (cumulative...)"
            m = re.search(r'\brate\s+([0-9.]+)\s+kS/s\s+([0-9.]+)\s+Mb/s', line)
            if m:
                tick += 1
                sps.append(float(m.group(1)) * 1000.0)
                mbps.append(float(m.group(2)) / 8.0)  # Mbit/s -> MB/s
            m2 = re.search(r'rss:([0-9.]+)([kMG]?)B', line)
            if m2:
                v = float(m2.group(1))
                v = {'': v, 'k': v/1e3, 'M': v, 'G': v*1e3}[m2.group(2)]
                rss.append(v)
    return sps, mbps, rss, matched_tick

def stats(sps):
    """Skip leading zero (pre-match) ticks; keep the rest incl. mid-run zeros."""
    started = False
    vals = []
    for x in sps:
        if not started:
            if x > 0:
                started = True
            else:
                continue
        vals.append(x)
    if not vals:
        return None
    n = len(vals)
    t = max(1, n // 3)
    first = sum(vals[:t]) / t
    last3 = sum(vals[-t:]) / t
    cr = (last3 / first) if first > 0 else 0.0
    return {
        'mean': sum(vals) / n, 'min': min(vals), 'max': max(vals),
        'last': vals[-1], 'cr': cr, 'nsteady': n,
    }

def main():
    base = sys.argv[1]
    labels = [('master', 'mtp_master', 'rustdds'),
              ('v0.13.0', 'mtp_v0130', 'rustdds'),
              ('cyclone', 'mtp_cyclone', 'cyclone')]
    rows = []
    hdr = ['scenario', 'version', 'reliab', 'size', 'match_s',
           'mean_sps', 'MB/s', 'min_sps', 'max_sps', 'last_sps', 'collapse', 'rss0', 'rssE', 'rssMax', 'ok']
    for label, sub, flavor in labels:
        d = os.path.join(base, sub, 'logs')
        if not os.path.isdir(d):
            continue
        for path in sorted(glob.glob(os.path.join(d, '*_sub.log'))):
            name = os.path.basename(path)[:-len('_sub.log')]
            kind, scen = (name.split('_', 1) + [''])[:2]
            reliab = 'rel' if 'rel' in name else 'be'
            if flavor == 'rustdds':
                sps, mbps, rss, mt = rustdds_series(path)
                survived = 'no' if re.search(r'panic', open(path, errors="replace").read(), re.I) else 'yes'
            else:
                sps, mbps, rss, mt = cyclone_series(path)
                survived = 'yes'
            st = stats(sps)
            mb = 0.0
            mbps_started = [x for i, x in enumerate(mbps) if any(s > 0 for s in sps[:i+1])]
            if st:
                # mean MB/s over steady ticks (align by dropping leading zero sps)
                steady_mbps = mbps[len(sps)-st['nsteady']:] if st['nsteady'] <= len(mbps) else mbps
                mb = sum(steady_mbps)/len(steady_mbps) if steady_mbps else 0.0
            rss0 = f'{rss[0]:.0f}' if rss else 'NA'
            rssE = f'{rss[-1]:.0f}' if rss else 'NA'
            rssMax = f'{max(rss):.0f}' if rss else 'NA'
            if st:
                rows.append([name, label, reliab, size_of(name), mt if mt is not None else 'NEVER',
                             f"{st['mean']:.0f}", f"{mb:.1f}", f"{st['min']:.0f}", f"{st['max']:.0f}",
                             f"{st['last']:.0f}", f"{st['cr']:.2f}", rss0, rssE, rssMax, survived])
            else:
                rows.append([name, label, reliab, size_of(name), mt if mt is not None else 'NEVER',
                             '0', '0', '0', '0', '0', 'NODATA', rss0, rssE, rssMax, survived])
    # print grouped by scenario for easy 3-way comparison
    widths = [max(len(str(r[i])) for r in ([hdr] + rows)) for i in range(len(hdr))]
    def fmt(r):
        return '  '.join(str(c).ljust(widths[i]) for i, c in enumerate(r))
    print(fmt(hdr))
    print('-' * (sum(widths) + 2*len(widths)))
    for scen in sorted(set(r[0] for r in rows), key=lambda s: (s.split('_',1)[0], s)):
        for r in rows:
            if r[0] == scen:
                print(fmt(r))
        print()

def size_of(name):
    if '64b' in name: return '64'
    if '64k' in name: return '65000'
    if '8k' in name: return '8192'
    if '1k' in name: return '1024'
    return '?'

if __name__ == '__main__':
    main()
