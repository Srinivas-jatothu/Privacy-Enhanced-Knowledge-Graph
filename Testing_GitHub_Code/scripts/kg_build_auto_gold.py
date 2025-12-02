#!/usr/bin/env python3
# scripts/kg_build_auto_gold.py
"""
Automatically build a *silver-standard* evaluation set:
- kg_eval_gold_entities.json
- kg_eval_gold_triples.json

Strategy:
- Load node_v2.json and edges.json
- Select a subset of nodes (e.g., files + functions under certain path prefixes)
- Keep only high-signal edge types between those nodes (DEFINES, CALLS, IMPORTS, DEPENDS_ON)
- Dump as gold entities + gold triples

Later, other KG variants can be evaluated against this reference.
"""

import os, json, sys, random

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if not os.path.exists(os.path.join(PROJECT_ROOT, "results")):
    PROJECT_ROOT = os.getcwd()

RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
NODES_IN = os.path.join(RESULTS_DIR, "node_v2.json")
EDGES_IN = os.path.join(RESULTS_DIR, "edges.json")
GOLD_ENTITIES_OUT = os.path.join(RESULTS_DIR, "kg_eval_gold_entities.json")
GOLD_TRIPLES_OUT = os.path.join(RESULTS_DIR, "kg_eval_gold_triples.json")

# CONFIG: adjust to control scope and size
PATH_PREFIXES = [
    "dags/src/",
    "gcpdeploy/src/",
]
ALLOWED_TYPES = {"File", "Function"}           # entities we care about
ALLOWED_EDGE_TYPES = {"DEFINES", "CALLS", "IMPORTS", "DEPENDS_ON"}
MAX_ENTITIES = 150
MAX_TRIPLES = 300
RANDOM_SEED = 42

random.seed(RANDOM_SEED)

def load_json(path, what):
    if not os.path.exists(path):
        print(f"[ERROR] Missing {what}: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return json.load(fh)

print("[INFO] Loading nodes from:", NODES_IN)
nodes = load_json(NODES_IN, "nodes")
print("[INFO] Loading edges from:", EDGES_IN)
edges = load_json(EDGES_IN, "edges")

# ---------------------------------------------------------------------------
# 1. Select candidate entities
# ---------------------------------------------------------------------------

def get_node_type(n):
    return n.get("type") or (n.get("attrs") or {}).get("type")

def get_node_path(n):
    return n.get("path") or (n.get("attrs") or {}).get("file") or ""

selected_nodes = []
for n in nodes:
    nid = n.get("id")
    if not nid:
        continue
    ntype = get_node_type(n)
    if ntype not in ALLOWED_TYPES:
        continue
    path = get_node_path(n)
    # keep if path starts with any of our prefixes (for Files)
    # or if it's a Function whose file path matches
    if any(path.startswith(pref) for pref in PATH_PREFIXES):
        selected_nodes.append(n)

print(f"[INFO] Candidate entities in selected scope: {len(selected_nodes)}")

# if too many, downsample
if len(selected_nodes) > MAX_ENTITIES:
    selected_nodes = random.sample(selected_nodes, MAX_ENTITIES)
    print(f"[INFO] Downsampled entities to MAX_ENTITIES={MAX_ENTITIES}")

selected_ids = {n["id"] for n in selected_nodes}

gold_entities = []
for n in selected_nodes:
    gold_entities.append({
        "id": n["id"],
        "type": get_node_type(n)
    })

print(f"[INFO] Gold entities to write: {len(gold_entities)}")

# ---------------------------------------------------------------------------
# 2. Select candidate triples between selected entities
# ---------------------------------------------------------------------------

def edge_endpoints(e):
    return (
        e.get("source") or e.get("start_id") or e.get("start"),
        e.get("target") or e.get("end_id") or e.get("end"),
    )

gold_triples = []
for e in edges:
    s, o = edge_endpoints(e)
    p = e.get("type") or (e.get("attrs") or {}).get("type")
    if not (s and o and p):
        continue
    if p not in ALLOWED_EDGE_TYPES:
        continue
    # only keep triples where BOTH endpoints are in our selected entity set
    if s in selected_ids and o in selected_ids:
        gold_triples.append({
            "subject": s,
            "predicate": p,
            "object": o
        })

print(f"[INFO] Candidate triples in scope: {len(gold_triples)}")

if len(gold_triples) > MAX_TRIPLES:
    gold_triples = random.sample(gold_triples, MAX_TRIPLES)
    print(f"[INFO] Downsampled triples to MAX_TRIPLES={MAX_TRIPLES}")

print(f"[INFO] Gold triples to write: {len(gold_triples)}")

# ---------------------------------------------------------------------------
# 3. Write outputs
# ---------------------------------------------------------------------------

with open(GOLD_ENTITIES_OUT, "w", encoding="utf-8") as fh:
    json.dump(gold_entities, fh, indent=2, ensure_ascii=False)
print("[INFO] Wrote gold entities to:", GOLD_ENTITIES_OUT)

with open(GOLD_TRIPLES_OUT, "w", encoding="utf-8") as fh:
    json.dump(gold_triples, fh, indent=2, ensure_ascii=False)
print("[INFO] Wrote gold triples to:", GOLD_TRIPLES_OUT)

print("\n[INFO] Done. You can now run kg_extraction_eval.py to compute precision/recall/F1\n"
      "against this silver-standard reference subset.")
