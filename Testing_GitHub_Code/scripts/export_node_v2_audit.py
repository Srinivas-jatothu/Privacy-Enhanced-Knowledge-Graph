#!/usr/bin/env python3
# scripts/export_node_v2_audit.py

import json, csv, os, sys, collections

# Always calculate paths relative to the project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

IN = os.path.join(PROJECT_ROOT, "results", "node_v2.json")
OUT_DIR = os.path.join(PROJECT_ROOT, "results", "kg_check")
OUT = os.path.join(OUT_DIR, "node_v2_audit.csv")

os.makedirs(OUT_DIR, exist_ok=True)

print("Script directory:", SCRIPT_DIR)
print("Project root:", PROJECT_ROOT)
print("Loading:", IN)

if not os.path.exists(IN):
    print(f"[ERROR] Input file not found: {IN}")
    sys.exit(2)

with open(IN, "r", encoding="utf-8", errors="replace") as fh:
    try:
        nodes = json.load(fh)
    except Exception as e:
        print("ERROR loading JSON:", e, file=sys.stderr)
        sys.exit(2)

total = len(nodes)
print("Total nodes:", total)

pr_counter = collections.Counter()
wrote = 0

print("Writing:", OUT)
with open(OUT, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow([
        "id","path","introduced_by_commit","introduced_by_pr",
        "introduced_by_pr_title","modified_by_commits"
    ])

    for n in nodes:
        nid = n.get("id", "")
        path = n.get("path") or (n.get("attrs") or {}).get("file") or ""
        c = n.get("introduced_by_commit") or (n.get("attrs") or {}).get("introduced_by_commit") or ""
        pr = n.get("introduced_by_pr") or (n.get("attrs") or {}).get("introduced_by_pr") or ""
        prt = n.get("introduced_by_pr_title") or (n.get("attrs") or {}).get("introduced_by_pr_title") or ""
        mods = n.get("modified_by_commits") or (n.get("attrs") or {}).get("modified_by_commits") or []
        mods_str = ";".join(mods) if isinstance(mods, list) else str(mods)

        w.writerow([nid, path, c, pr, prt, mods_str])
        wrote += 1

        if pr:
            pr_counter[pr] += 1

print("Wrote rows:", wrote)
print("Output saved to:", OUT)

print("Top 10 PRs:")
for pr, cnt in pr_counter.most_common(10):
    print(f"  PR {pr}: {cnt} occurrences")
