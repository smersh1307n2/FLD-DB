#!/usr/bin/env python3
"""Fail the Storage index benchmark workflow if any benchmark case did not execute."""
import csv
import glob
import os
import sys

reports = sorted(glob.glob("BenchmarkDotNet.Artifacts/results/*-report.csv"))
if len(reports) != 1:
    raise SystemExit(f"expected exactly one BenchmarkDotNet CSV report, found {len(reports)}")

with open(reports[0], newline="", encoding="utf-8-sig") as stream:
    rows = list(csv.DictReader(stream))

expected_methods = {
    "FreshOpenAndPointLookup",
    "FreshOpenAndSnapshotReadOneEntry",
    "CachedSnapshotAndReadOneEntry",
    "WarmPointLookup",
}
expected_counts = {"1000", "10000", "100000"}
if len(rows) != 12:
    raise SystemExit(f"expected 12 benchmark rows, found {len(rows)}")

seen = set()
for row in rows:
    method = row.get("Method", "").strip()
    count = row.get("EntryCount", "").strip()
    mean = row.get("Mean", "").strip()
    allocated = row.get("Allocated", "").strip()
    if method not in expected_methods or count not in expected_counts:
        raise SystemExit(f"unexpected benchmark row: {row}")
    if not mean or mean.upper() == "NA" or not allocated or allocated.upper() == "NA":
        raise SystemExit(f"benchmark did not produce a measurement: {row}")
    seen.add((method, count))

expected = {(method, count) for method in expected_methods for count in expected_counts}
if seen != expected:
    missing = sorted(expected - seen)
    extra = sorted(seen - expected)
    raise SystemExit(f"benchmark matrix mismatch; missing={missing}, extra={extra}")

print(f"Validated {len(rows)} measured Storage index benchmark cases in {reports[0]}")
