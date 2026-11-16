#!/usr/bin/env python3
"""Validate the dedicated one-million-entry B+ tree scale run."""
import json
import re
from pathlib import Path

report_path = Path("storage-index-million-scale.json")
time_path = Path("storage-index-million-time.txt")
if not report_path.is_file() or not time_path.is_file():
    raise SystemExit("missing scale report or /usr/bin/time diagnostic")

report = json.loads(report_path.read_text(encoding="utf-8"))
required = {
    "Schema": "fld-storage-index-million-scale-v1",
    "EntryCount": 1_000_000,
    "LookupCount": 10_000,
    "PrefixScanCount": 1_000_000,
}
for key, expected in required.items():
    if report.get(key) != expected:
        raise SystemExit(f"{key}: expected {expected!r}, got {report.get(key)!r}")

for key in ("PageCount", "RootPageId", "BuildAllocatedBytes", "TotalAllocatedBytes",
            "BuildMilliseconds", "PointLookupMilliseconds", "PrefixScanMilliseconds",
            "ProcessPeakWorkingSetBytes"):
    value = report.get(key)
    if not isinstance(value, (int, float)) or value <= 0:
        raise SystemExit(f"invalid or missing positive metric {key}: {value!r}")

time_log = time_path.read_text(encoding="utf-8", errors="replace")
match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", time_log)
if not match:
    raise SystemExit("could not parse Maximum resident set size from /usr/bin/time")
max_rss_kib = int(match.group(1))
if max_rss_kib <= 0:
    raise SystemExit("maximum RSS must be positive")

print("Validated 1,000,000 B+ tree entries; pages={}; build={:.1f} ms; lookups={:.1f} ms; scan={:.1f} ms; build-alloc={:.1f} MiB; process max-RSS={:.1f} MiB".format(
    report["PageCount"],
    report["BuildMilliseconds"],
    report["PointLookupMilliseconds"],
    report["PrefixScanMilliseconds"],
    report["BuildAllocatedBytes"] / (1024 * 1024),
    max_rss_kib / 1024))
