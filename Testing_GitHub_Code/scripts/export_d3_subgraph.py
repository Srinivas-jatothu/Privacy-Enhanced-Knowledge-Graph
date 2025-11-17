#!/usr/bin/env python3
# scripts/export_d3_subgraph.py

import os, json, collections, sys

# -----------------------------------------------
# Resolve project root
# -----------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

if not os.path.exists(os.path.join(PROJECT_ROOT, "results")):
    # fallback to CWD
    PROJECT_ROOT = os.getcwd()

print("[INFO] Script directory:", SCRIPT_DIR)
print("[INFO] Project root:", PROJECT_ROOT)

# -----------------------------------------------
# Inputs
# -----------------------------------------------
NODES_IN = os.path.join(PROJECT_ROOT, "results", "node_v2.json")
EDGES_IN = os.path.join(PROJECT_ROOT, "results", "edges.json")

OUT = os.path.join(PROJECT_ROOT, "results", "visualization", "d3_subgraph.json")
os.makedirs(os.path.dirname(OUT), exist_ok=True)

def load_json(path):
    """Safe loader"""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"[ERROR] Failed to load {path}: {e}")
        sys.exit(1)

print("[INFO] Loading nodes and edges...")
nodes = load_json(NODES_IN)
edges = load_json(EDGES_IN)

# -----------------------------------------------
# CONFIGURE SUBGRAPH FILTER HERE
# Comment/uncomment lines as needed
# -----------------------------------------------

# Option 1 — filter by PR number:
PR_FILTER = "54"     # <= TOP PR in your audit
SEED_NODE = None     # <= unused when PR_FILTER is set
K_HOP = 2            # <= unused when PR_FILTER is set

# Option 2 — K-hop from a specific node:
# PR_FILTER = None
# SEED_NODE = "Function:dags.src.correlation.correlation_check"
# K_HOP = 2

# -----------------------------------------------
# Build adj list
# -----------------------------------------------
print("[INFO] Building adjacency...")
id2node = {n["id"]: n for n in nodes}
adj = collections.defaultdict(list)

def edge_endpoints(e):
    return (
        e.get("source") or e.get("start_id") or e.get("start"),
        e.get("target") or e.get("end_id") or e.get("end"),
    )

for e in edges:
    s, t = edge_endpoints(e)
    if s and t:
        adj[s].append((t, e))
        adj[t].append((s, e))  # undirected for subgraph extraction

# -----------------------------------------------
# Extract subgraph
# -----------------------------------------------
selected_ids = set()
selected_edges = set()

if PR_FILTER:
    print(f"[INFO] Filtering by PR={PR_FILTER}")
    for n in nodes:
        pr = n.get("introduced_by_pr") or (n.get("attrs") or {}).get("introduced_by_pr")
        if pr and str(pr) == str(PR_FILTER):
            selected_ids.add(n["id"])

    # add 1-hop neighbors
    for sid in list(selected_ids):
        for (nbr, e) in adj.get(sid, []):
            selected_ids.add(nbr)
            selected_edges.add((sid, nbr))

else:
    if not SEED_NODE:
        print("[ERROR] Set SEED_NODE or PR_FILTER in script!")
        sys.exit(1)

    print(f"[INFO] Extracting {K_HOP}-hop neighbors from {SEED_NODE}")
    frontier = {SEED_NODE}
    visited = set(frontier)

    for _ in range(K_HOP):
        new = set()
        for u in frontier:
            for (v, e) in adj.get(u, []):
                if v not in visited:
                    new.add(v)
                    selected_edges.add((u, v))
        visited |= new
        frontier = new

    selected_ids = visited | {SEED_NODE}

# Add edges fully
for e in edges:
    s, t = edge_endpoints(e)
    if s in selected_ids and t in selected_ids:
        selected_edges.add((s, t))

# -----------------------------------------------
# Build JSON output
# -----------------------------------------------
nodes_out = []
for nid in selected_ids:
    n = id2node.get(nid)
    if not n:
        continue
    nodes_out.append({
        "id": n["id"],
        "label": n.get("label") or (n.get("attrs") or {}).get("file") or n["id"],
        "type": n.get("type") or (n.get("attrs") or {}).get("type"),
        "path": n.get("path") or (n.get("attrs") or {}).get("file"),
        "introduced_by_pr": n.get("introduced_by_pr") or (n.get("attrs") or {}).get("introduced_by_pr")
    })

links = [{"source": s, "target": t} for (s, t) in selected_edges]

out_obj = {"nodes": nodes_out, "links": links}

with open(OUT, "w", encoding="utf-8") as fh:
    json.dump(out_obj, fh, indent=2, ensure_ascii=False)

print("[INFO] Wrote subgraph:", OUT)
print("[INFO] Nodes:", len(nodes_out), "Edges:", len(links))
