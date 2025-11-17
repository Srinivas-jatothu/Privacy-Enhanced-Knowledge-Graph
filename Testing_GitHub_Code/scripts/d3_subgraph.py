#!/usr/bin/env python3
# scripts/export_d3_subgraph.py
"""
Export small subgraph for D3/vis.js:
- usage: edit SEED_NODE or PR_FILTER or set K_HOP
Outputs: results/visualization/d3_subgraph.json
"""
import os,json,collections

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if not os.path.exists(os.path.join(PROJECT_ROOT,"results")):
    PROJECT_ROOT = os.getcwd()

NODES_IN = os.path.join(PROJECT_ROOT, "results", "node_v2.json")
EDGES_IN = os.path.join(PROJECT_ROOT, "results", "edges.json")
OUT = os.path.join(PROJECT_ROOT, "results", "visualization", "d3_subgraph.json")
os.makedirs(os.path.dirname(OUT), exist_ok=True)

nodes = json.load(open(NODES_IN, "r", encoding="utf-8", errors="replace"))
edges = json.load(open(EDGES_IN, "r", encoding="utf-8", errors="replace"))

# Configure these variables:
SEED_NODE = None   # e.g. "Function:dags.src.correlation.correlation_check"
PR_FILTER = "54"   # or None
K_HOP = 2          # how many hops from seed (ignored if PR_FILTER set)

id2node = {n["id"]: n for n in nodes}
adj = collections.defaultdict(list)
for e in edges:
    s = e.get("source") or e.get("start_id") or e.get("start")
    t = e.get("target") or e.get("end_id") or e.get("end")
    if s and t:
        adj[s].append((t,e))
        adj[t].append((s,e))  # undirected for k-hop

selected_ids = set()
selected_edges = set()

if PR_FILTER:
    # select nodes introduced by PR_FILTER
    for n in nodes:
        pr = n.get("introduced_by_pr") or (n.get("attrs") or {}).get("introduced_by_pr")
        if pr and str(pr) == str(PR_FILTER):
            selected_ids.add(n["id"])
    # also include immediate neighbors
    for sid in list(selected_ids):
        for (nbr,e) in adj.get(sid,[]):
            selected_ids.add(nbr)
            selected_edges.add((sid,nbr))
else:
    # k-hop from SEED_NODE
    if not SEED_NODE:
        raise SystemExit("Set SEED_NODE or PR_FILTER in script")
    frontier = {SEED_NODE}
    visited = set(frontier)
    for _ in range(K_HOP):
        new = set()
        for u in frontier:
            for (v,e) in adj.get(u,[]):
                if v not in visited:
                    new.add(v)
                    selected_edges.add((u,v))
            visited |= new
        frontier = new
    selected_ids = visited | {SEED_NODE}

# collect edges between selected nodes
for e in edges:
    s = e.get("source") or e.get("start_id") or e.get("start")
    t = e.get("target") or e.get("end_id") or e.get("end")
    if s in selected_ids and t in selected_ids:
        selected_edges.add((s,t))

nodes_out = []
for nid in selected_ids:
    n = id2node.get(nid)
    if not n:
        continue
    nodes_out.append({
        "id": n["id"],
        "label": n.get("label") or n.get("attrs",{}).get("file") or n["id"],
        "type": n.get("type") or n.get("attrs",{}).get("type"),
        "path": n.get("path") or n.get("attrs",{}).get("file"),
        "introduced_by_pr": n.get("introduced_by_pr") or n.get("attrs",{}).get("introduced_by_pr")
    })

links = []
for (s,t) in selected_edges:
    links.append({"source": s, "target": t})

out = {"nodes": nodes_out, "links": links}
open(OUT,"w",encoding="utf-8").write(json.dumps(out, indent=2, ensure_ascii=False))
print("Wrote d3 subgraph:", OUT, "nodes:", len(nodes_out), "links:", len(links))
