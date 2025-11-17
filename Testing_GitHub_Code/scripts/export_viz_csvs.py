#!/usr/bin/env python3
# scripts/export_viz_csvs.py
"""
Export node_v2.json + edges.json -> viz_nodes.csv + viz_edges.csv
- viz_nodes.csv: id, label, type, path, file_hash, introduced_by_commit, introduced_by_pr, introduced_by_pr_title, group
- viz_edges.csv: start_id, end_id, type, weight, attrs_json
"""
import os, json, csv, sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# if you run from project root, fallback to CWD
if not os.path.exists(os.path.join(PROJECT_ROOT, "results")):
    PROJECT_ROOT = os.getcwd()

NODES_IN = os.path.join(PROJECT_ROOT, "results", "node_v2.json")
EDGES_IN = os.path.join(PROJECT_ROOT, "results", "edges.json")
OUT_DIR = os.path.join(PROJECT_ROOT, "results", "visualization")
os.makedirs(OUT_DIR, exist_ok=True)
NODES_CSV = os.path.join(OUT_DIR, "viz_nodes.csv")
EDGES_CSV = os.path.join(OUT_DIR, "viz_edges.csv")

def load_json(path):
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return json.load(fh)

print("Loading:", NODES_IN, "and", EDGES_IN)
nodes = load_json(NODES_IN)
edges = load_json(EDGES_IN)

print("Writing nodes to:", NODES_CSV)
with open(NODES_CSV, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["id","label","type","path","file_hash","introduced_by_commit","introduced_by_pr","introduced_by_pr_title","group"])
    for n in nodes:
        nid = n.get("id","")
        label = n.get("label") or (n.get("attrs") or {}).get("label") or nid
        ntype = n.get("type") or (n.get("attrs") or {}).get("type") or ""
        path = n.get("path") or (n.get("attrs") or {}).get("file") or ""
        file_hash = n.get("hash") or (n.get("attrs") or {}).get("file_hash") or ""
        introduced_by_commit = n.get("introduced_by_commit") or (n.get("attrs") or {}).get("introduced_by_commit") or ""
        introduced_by_pr = n.get("introduced_by_pr") or (n.get("attrs") or {}).get("introduced_by_pr") or ""
        introduced_by_pr_title = n.get("introduced_by_pr_title") or (n.get("attrs") or {}).get("introduced_by_pr_title") or ""
        group = ntype
        w.writerow([nid,label,ntype,path,file_hash,introduced_by_commit,introduced_by_pr,introduced_by_pr_title,group])

print("Writing edges to:", EDGES_CSV)
with open(EDGES_CSV, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["start_id","end_id","type","weight","attrs_json"])
    for e in edges:
        s = e.get("source") or e.get("start") or e.get("from") or e.get("start_id") or ""
        t = e.get("target") or e.get("end") or e.get("to") or e.get("end_id") or ""
        etype = e.get("type") or (e.get("attrs") or {}).get("type") or ""
        weight = e.get("weight") or (e.get("attrs") or {}).get("weight") or 1
        attrs = e.get("attrs") or {}
        # prefer canonical mapping if keys differ
        if not s and e.get("start_id"):
            s = e.get("start_id")
        if not t and e.get("end_id"):
            t = e.get("end_id")
        w.writerow([s,t,etype,weight,json.dumps(attrs, ensure_ascii=False)])

print("Done. Files at:", OUT_DIR)
