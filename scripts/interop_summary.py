#!/usr/bin/env python3
"""Summarize a dds-rtps interoperability matrix run into per-vendor tables.

Reads the per-pair JUnit-style XML reports produced by
``scripts/interop_matrix.sh`` (see the ``results/matrix_<stamp>/`` directory) and
emits, for each chosen vendor, a table comparing that vendor against every peer
it was tested with (including itself). Rows are peers; columns are the main test
categories of the suite (Domain, DataRepresentation, Reliability, ...). Each cell
reports three counts: tests passed / unsupported / failed.

"Unsupported" is distinguished from a genuine failure: a failed test case is
counted as *unsupported* when either endpoint reported a
``PUB_UNSUPPORTED_FEATURE`` / ``SUB_UNSUPPORTED_FEATURE`` produced code (i.e. the
program under test declared it does not implement the feature), otherwise it is
counted as *failed*.

The two test directions of each peer (chosen vendor as Publisher / as Subscriber)
are summed into a single row.

Output is written in two copies -- Markdown and standalone HTML -- with the file
name including the chosen implementation and its tested version, e.g.
``interop_summary_rustdds-0.12.0.md`` / ``.html``.

Note on the current matrix: ``interop_matrix.sh`` only pairs RustDDS against each
other vendor (both directions) plus a RustDDS-vs-RustDDS baseline. Consequently
only ``--vendor rustdds`` yields a fully populated table; choosing another vendor
yields just its RustDDS comparison (peers with no report are listed as not-run).
This is a property of the run, not of this script.

Usage:
    scripts/interop_summary.py [RESULTS_DIR] [--vendor NAME ...] [--out-dir DIR]

    RESULTS_DIR   matrix results directory (default: newest matrix_* under
                  $DDS_RTPS_DIR/results, DDS_RTPS_DIR defaulting to
                  /home/juhe/cursor/dds-rtps).
    --vendor      implementation name to summarize (repeatable; default: rustdds).
                  Matched against the implementation name, version-insensitive.
    --out-dir     output directory (default: the results directory).
"""

import argparse
import csv
import datetime as _dt
import glob
import html
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, OrderedDict

DEFAULT_DDS_RTPS_DIR = os.environ.get("DDS_RTPS_DIR", "/home/juhe/cursor/dds-rtps")

# testcase name -> category token, e.g. rtps_test_suite_1_Test_DataRepresentation_1
CATEGORY_RE = re.compile(r"Test_([A-Za-z0-9]+)_")
# impl-version label -> (impl, version); version starts at the first '-' before a digit.
LABEL_RE = re.compile(r"^(.*?)-(\d.*)$")
# failure-table row: <th>Entity</th> <th>Expected</th> <th>Produced</th>
FAILROW_RE = re.compile(
    r"<th>\s*(?:Publisher|Subscriber)_\d\s*</th>\s*"
    r"<th>\s*[A-Z_]+\s*</th>\s*"
    r"<th>\s*([A-Z_]+)\s*</th>"
)

# Human-friendly column headers for known categories (fallback: the raw token).
CATEGORY_LABELS = {
    "DataRepresentation": "Data Repr.",
    "TimeBasedFilter": "TimeBasedFilter",
    "FinalInstanceState": "FinalInstState",
    "OrderedAccess": "OrderedAccess",
    "CoherentSets": "CoherentSets",
    "LargeData": "LargeData",
    "Cft": "CFT",
}

PASS, UNSUPPORTED, FAILED = "pass", "unsupported", "failed"


def split_label(label):
    """('rustdds-0.12.0') -> ('rustdds', '0.12.0'); unknown form -> (label, '')."""
    m = LABEL_RE.match(label)
    if m:
        return m.group(1), m.group(2)
    return label, ""


def impl_of(label):
    return split_label(label)[0]


def find_latest_results_dir():
    pattern = os.path.join(DEFAULT_DDS_RTPS_DIR, "results", "matrix_*")
    candidates = sorted(
        (d for d in glob.glob(pattern) if os.path.isdir(d)),
        key=os.path.getmtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def classify_failure(message):
    """Classify a failed testcase as UNSUPPORTED (any side declared the feature
    unsupported) or FAILED (any genuine mismatch)."""
    produced = FAILROW_RE.findall(message or "")
    if any("UNSUPPORTED_FEATURE" in code for code in produced):
        return UNSUPPORTED
    return FAILED


def parse_report(path):
    """Parse one XML report.

    Returns (pub_label, sub_label, per_category_status) where per_category_status
    is {category: Counter({pass/unsupported/failed: n})}, or None if unparseable.
    """
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        print(f"warning: cannot parse {path}: {exc}", file=sys.stderr)
        return None

    suite = root.find(".//testsuite")
    if suite is None:
        suite = root if root.tag == "testsuite" else None
    if suite is None or "---" not in (suite.get("name") or ""):
        print(f"warning: no testsuite/name in {path}", file=sys.stderr)
        return None

    pub_label, sub_label = suite.get("name").split("---", 1)

    per_cat = OrderedDict()
    for tc in root.iter("testcase"):
        name = tc.get("name") or ""
        m = CATEGORY_RE.search(name)
        cat = m.group(1) if m else "Other"
        counter = per_cat.setdefault(cat, Counter())
        failure = tc.find("failure")
        if failure is None:
            counter[PASS] += 1
        else:
            msg = failure.get("message") or failure.text or ""
            counter[classify_failure(msg)] += 1
    return pub_label, sub_label, per_cat


def load_summary_csv(results_dir):
    """Return {vendor_label: worst_return_code} from SUMMARY.csv (if present)."""
    path = os.path.join(results_dir, "SUMMARY.csv")
    rc_by_vendor = {}
    if not os.path.isfile(path):
        return rc_by_vendor
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            vendor = (row.get("vendor") or "").strip()
            if not vendor:
                continue
            try:
                rc = int(row.get("return_code") or 0)
            except ValueError:
                rc = 0
            # Keep the most alarming (non-zero) return code seen for the vendor.
            prev = rc_by_vendor.get(vendor, 0)
            rc_by_vendor[vendor] = rc if rc != 0 else prev
    return rc_by_vendor


def collect(reports, categories_order):
    """reports: list of (pub_label, sub_label, per_cat). Populate categories_order
    (list, first-seen) and return it unchanged (mutated in place)."""
    for _pub, _sub, per_cat in reports:
        for cat in per_cat:
            if cat not in categories_order:
                categories_order.append(cat)
    return categories_order


def build_vendor_table(vendor, reports, categories):
    """Aggregate all reports where `vendor` (impl name) participates.

    Returns (chosen_label, rows) where rows is an ordered dict
    peer_label -> {category: Counter}. Both test directions are summed. The self
    pairing (vendor vs vendor) becomes the peer row equal to the chosen label.
    """
    chosen_label = None
    # peer_label -> {cat: Counter}
    agg = OrderedDict()

    for pub_label, sub_label, per_cat in reports:
        pub_is = impl_of(pub_label) == vendor
        sub_is = impl_of(sub_label) == vendor
        if not (pub_is or sub_is):
            continue
        # Resolve the chosen vendor's full label from wherever it appears.
        if chosen_label is None:
            chosen_label = pub_label if pub_is else sub_label
        # Peer is the other side; for a self pairing the peer is the vendor itself.
        if pub_is and sub_is:
            peer_label = pub_label
        else:
            peer_label = sub_label if pub_is else pub_label

        peer_cats = agg.setdefault(peer_label, OrderedDict())
        for cat in categories:
            src = per_cat.get(cat)
            if not src:
                continue
            dst = peer_cats.setdefault(cat, Counter())
            dst.update(src)

    return chosen_label, agg


def order_peers(agg, chosen_label):
    """Chosen vendor's self row first, then peers sorted by implementation name."""
    peers = list(agg.keys())
    def key(label):
        return (label != chosen_label, impl_of(label).lower(), label.lower())
    return sorted(peers, key=key)


def cell_counts(counter):
    return counter.get(PASS, 0), counter.get(UNSUPPORTED, 0), counter.get(FAILED, 0)


def category_header(cat):
    return CATEGORY_LABELS.get(cat, cat)


def not_run_note(rc):
    """Annotation for a peer that produced no report. A non-zero return code
    (e.g. 124 = timeout) is informative; rc==0 typically means the pairing was
    simply not part of this (RustDDS-centric) matrix run."""
    return f"not run (rc={rc})" if rc else "not paired"


# --------------------------- Markdown rendering ---------------------------

def render_markdown(chosen_label, agg, peers, categories, not_run, results_dir):
    now = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    lines.append(f"# Interoperability summary: {chosen_label}")
    lines.append("")
    lines.append(f"- Chosen implementation: **{chosen_label}**")
    lines.append(f"- Source results: `{results_dir}`")
    lines.append(f"- Generated: {now}")
    lines.append("")
    lines.append(
        "Each cell shows **passed / unsupported / failed** test cases. "
        "*Unsupported* means the program under test reported the feature as "
        "unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other "
        "mismatch. Both test directions (chosen vendor as Publisher and as "
        "Subscriber) are summed per peer."
    )
    lines.append("")

    header = ["Peer"] + [category_header(c) for c in categories] + ["Total"]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join(["---"] * len(header)) + "|")

    # Column totals accumulator.
    col_totals = {c: Counter() for c in categories}
    grand = Counter()

    def fmt(counter):
        p, u, f = cell_counts(counter)
        return f"{p} / {u} / {f}"

    for peer in peers:
        peer_cats = agg[peer]
        row = [f"`{peer}`"]
        row_total = Counter()
        for cat in categories:
            counter = peer_cats.get(cat, Counter())
            row.append(fmt(counter))
            col_totals[cat].update(counter)
            row_total.update(counter)
        row.append(f"**{fmt(row_total)}**")
        grand.update(row_total)
        lines.append("| " + " | ".join(row) + " |")

    # Not-run peers.
    for peer, rc in not_run:
        note = f"_{not_run_note(rc)}_"
        row = [f"`{peer}`"] + [note] + [""] * (len(categories) - 1) + [note]
        lines.append("| " + " | ".join(row) + " |")

    # Totals row.
    total_row = ["**Total**"]
    for cat in categories:
        total_row.append(f"**{fmt(col_totals[cat])}**")
    total_row.append(f"**{fmt(grand)}**")
    lines.append("| " + " | ".join(total_row) + " |")
    lines.append("")
    lines.append("Legend: `passed / unsupported / failed`.")
    lines.append("")
    return "\n".join(lines)


# ----------------------------- HTML rendering -----------------------------

HTML_STYLE = """
:root { color-scheme: light dark; }
body { font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
       margin: 2rem; line-height: 1.4; }
h1 { font-size: 1.4rem; }
table { border-collapse: collapse; margin-top: 1rem; font-size: 0.9rem; }
th, td { border: 1px solid #bbb; padding: 4px 8px; text-align: center; white-space: nowrap; }
th { background: #f0f0f0; }
td.peer, th.peer { text-align: left; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
tr.total td, td.total { font-weight: bold; background: #f7f7f7; }
tr.notrun td { color: #888; font-style: italic; }
.pass { color: #1a7f37; }
.unsup { color: #9a6700; }
.fail { color: #cf222e; }
.meta { color: #555; font-size: 0.9rem; }
.legend { margin-top: 1rem; font-size: 0.9rem; }
@media (prefers-color-scheme: dark) {
  th { background: #222; } tr.total td, td.total { background: #1c1c1c; }
  th, td { border-color: #555; } .meta { color: #aaa; }
  .pass { color: #3fb950; } .unsup { color: #d29922; } .fail { color: #f85149; }
}
"""


def html_cell(counter):
    p, u, f = cell_counts(counter)
    return (
        f'<span class="pass">{p}</span> / '
        f'<span class="unsup">{u}</span> / '
        f'<span class="fail">{f}</span>'
    )


def render_html(chosen_label, agg, peers, categories, not_run, results_dir):
    now = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    esc = html.escape
    out = []
    out.append("<!DOCTYPE html>")
    out.append('<html lang="en"><head><meta charset="utf-8">')
    out.append(f"<title>Interop summary: {esc(chosen_label)}</title>")
    out.append(f"<style>{HTML_STYLE}</style>")
    out.append("</head><body>")
    out.append(f"<h1>Interoperability summary: {esc(chosen_label)}</h1>")
    out.append('<div class="meta">')
    out.append(f"Chosen implementation: <strong>{esc(chosen_label)}</strong><br>")
    out.append(f"Source results: <code>{esc(results_dir)}</code><br>")
    out.append(f"Generated: {esc(now)}")
    out.append("</div>")
    out.append(
        '<p>Each cell shows <strong>passed / unsupported / failed</strong> test '
        "cases. <em>Unsupported</em> means the program under test reported the "
        "feature as unsupported (<code>PUB_/SUB_UNSUPPORTED_FEATURE</code>); "
        "<em>failed</em> is any other mismatch. Both test directions (chosen "
        "vendor as Publisher and as Subscriber) are summed per peer.</p>"
    )

    out.append("<table>")
    # Header
    out.append("<thead><tr>")
    out.append('<th class="peer">Peer</th>')
    for cat in categories:
        out.append(f"<th>{esc(category_header(cat))}</th>")
    out.append("<th>Total</th>")
    out.append("</tr></thead><tbody>")

    col_totals = {c: Counter() for c in categories}
    grand = Counter()

    for peer in peers:
        peer_cats = agg[peer]
        out.append("<tr>")
        out.append(f'<td class="peer">{esc(peer)}</td>')
        row_total = Counter()
        for cat in categories:
            counter = peer_cats.get(cat, Counter())
            out.append(f"<td>{html_cell(counter)}</td>")
            col_totals[cat].update(counter)
            row_total.update(counter)
        out.append(f'<td class="total">{html_cell(row_total)}</td>')
        grand.update(row_total)
        out.append("</tr>")

    for peer, rc in not_run:
        span = len(categories) + 1
        out.append('<tr class="notrun">')
        out.append(f'<td class="peer">{esc(peer)}</td>')
        out.append(f'<td colspan="{span}">{esc(not_run_note(rc))}</td>')
        out.append("</tr>")

    out.append('<tr class="total">')
    out.append('<td class="peer">Total</td>')
    for cat in categories:
        out.append(f"<td>{html_cell(col_totals[cat])}</td>")
    out.append(f"<td>{html_cell(grand)}</td>")
    out.append("</tr>")

    out.append("</tbody></table>")
    out.append(
        '<div class="legend">Legend: '
        '<span class="pass">passed</span> / '
        '<span class="unsup">unsupported</span> / '
        '<span class="fail">failed</span>.</div>'
    )
    out.append("</body></html>")
    return "\n".join(out)


# --------------------------------- main ---------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Summarize a dds-rtps interop matrix run into per-vendor tables."
    )
    parser.add_argument(
        "results_dir",
        nargs="?",
        default=None,
        help="matrix results directory (default: newest matrix_* under "
        "$DDS_RTPS_DIR/results).",
    )
    parser.add_argument(
        "--vendor",
        action="append",
        dest="vendors",
        default=None,
        help="implementation name to summarize (repeatable; default: rustdds).",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="output directory (default: the results directory).",
    )
    args = parser.parse_args(argv)

    results_dir = args.results_dir or find_latest_results_dir()
    if not results_dir or not os.path.isdir(results_dir):
        parser.error(
            f"results directory not found: {results_dir!r}. Pass it explicitly or "
            f"set DDS_RTPS_DIR (currently {DEFAULT_DDS_RTPS_DIR!r})."
        )
    results_dir = os.path.abspath(results_dir)
    out_dir = os.path.abspath(args.out_dir) if args.out_dir else results_dir
    os.makedirs(out_dir, exist_ok=True)
    vendors = args.vendors or ["rustdds"]

    xml_paths = sorted(glob.glob(os.path.join(results_dir, "*.xml")))
    if not xml_paths:
        parser.error(f"no *.xml reports found in {results_dir}")

    reports = []
    for path in xml_paths:
        parsed = parse_report(path)
        if parsed is not None:
            reports.append(parsed)
    if not reports:
        parser.error(f"no parseable reports in {results_dir}")

    categories = collect(reports, [])
    rc_by_vendor = load_summary_csv(results_dir)

    written = []
    for vendor in vendors:
        chosen_label, agg = build_vendor_table(vendor, reports, categories)
        if chosen_label is None:
            print(
                f"warning: vendor {vendor!r} does not appear in any report; skipping.",
                file=sys.stderr,
            )
            continue
        peers = order_peers(agg, chosen_label)

        # Peers present in SUMMARY.csv but with no parsed report (e.g. timed out).
        covered_impls = {impl_of(p) for p in agg.keys()}
        not_run = []
        for vlabel, rc in sorted(rc_by_vendor.items()):
            if impl_of(vlabel) == vendor:
                continue  # the chosen vendor's own self row is handled above
            if impl_of(vlabel) not in covered_impls:
                not_run.append((vlabel, rc))

        md = render_markdown(chosen_label, agg, peers, categories, not_run, results_dir)
        htmldoc = render_html(chosen_label, agg, peers, categories, not_run, results_dir)

        base = os.path.join(out_dir, f"interop_summary_{chosen_label}")
        md_path = base + ".md"
        html_path = base + ".html"
        with open(md_path, "w") as fh:
            fh.write(md)
        with open(html_path, "w") as fh:
            fh.write(htmldoc)
        written.extend([md_path, html_path])
        print(f"wrote {md_path}")
        print(f"wrote {html_path}")

    if not written:
        print("no summaries written.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
