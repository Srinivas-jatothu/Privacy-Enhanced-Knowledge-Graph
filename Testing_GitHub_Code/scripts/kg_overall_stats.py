        #!/usr/bin/env python3
# scripts/kg_overall_stats.py

import os, json, sys, collections, statistics

try:
    import networkx as nx
except ImportError:
    print("[ERROR] Please install networkx: pip install networkx", file=sys.stderr)
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if not os.path.exists(os.path.join(PROJECT_ROOT, "results")):
    PROJECT_ROOT = os.getcwd()

NODES_IN = os.path.join(PROJECT_ROOT, "results", "node_v2.json")
EDGES_IN = os.path.join(PROJECT_ROOT, "results", "edges.json")

def load_json(path):
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return json.load(fh)

print("[INFO] Loading:", NODES_IN)
nodes = load_json(NODES_IN)
print("[INFO] Loading:", EDGES_IN)
edges = load_json(EDGES_IN)

# --- Node & edge type stats ---------------------------------------------------
node_type_counts = collections.Counter()
for n in nodes:
    t = n.get("type") or (n.get("attrs") or {}).get("type") or "UNKNOWN"
    node_type_counts[t] += 1

edge_type_counts = collections.Counter()
for e in edges:
    et = e.get("type") or (e.get("attrs") or {}).get("type") or "UNKNOWN"
    edge_type_counts[et] += 1

print("\n=== Node Type Counts ===")
total_nodes = len(nodes)
print("Total nodes:", total_nodes)
for t, c in node_type_counts.most_common():
    print(f"  {t:20s} {c:5d}")

print("\n=== Edge Type Counts ===")
total_edges = len(edges)
print("Total edges:", total_edges)
for t, c in edge_type_counts.most_common():
    print(f"  {t:20s} {c:5d}")

# --- Build graph for degree stats & path length -------------------------------
G = nx.Graph()
for n in nodes:
    nid = n.get("id")
    if nid:
        G.add_node(nid)

def edge_endpoints(e):
    return (
        e.get("source") or e.get("start_id") or e.get("start"),
        e.get("target") or e.get("end_id") or e.get("end")
    )

missing = 0
for e in edges:
    s, t = edge_endpoints(e)
    if s and t:
        G.add_edge(s, t)
    else:
        missing += 1

if missing:
    print(f"\n[WARN] {missing} edges had missing endpoints and were skipped.")

print("\n=== Degree Statistics ===")
degrees = [deg for _, deg in G.degree()]
print("Nodes in graph:", G.number_of_nodes())
print("Edges in graph:", G.number_of_edges())
print("Min degree:", min(degrees) if degrees else 0)
print("Max degree:", max(degrees) if degrees else 0)
print("Mean degree:", statistics.mean(degrees) if degrees else 0)
print("Median degree:", statistics.median(degrees) if degrees else 0)

# --- Connected components & approximate path length ---------------------------
print("\n=== Connected Components ===")
components = list(nx.connected_components(G))
print("Number of connected components:", len(components))
if components:
    largest_cc = max(components, key=len)
    print("Largest CC size:", len(largest_cc))
    Gcc = G.subgraph(largest_cc).copy()

    # Approximate average shortest path length (sample)
    sample_nodes = list(largest_cc)
    sample_nodes = sample_nodes[:min(len(sample_nodes), 200)]  # cap for speed

    path_lengths = []
    for i, u in enumerate(sample_nodes):
        lengths = nx.single_source_shortest_path_length(Gcc, u, cutoff=10)
        path_lengths.extend(lengths.values())

    if path_lengths:
        print("Approx. avg shortest path length (sampled, cutoff=10):",
              round(statistics.mean(path_lengths), 3))
    else:
        print("No path length data (graph too small?)")

    # Approximate diameter using eccentricity on sampled nodes
    eccs = []
    for u in sample_nodes:
        try:
            ecc = max(nx.single_source_shortest_path_length(Gcc, u).values())
            eccs.append(ecc)
        except Exception:
            continue
    if eccs:
        print("Approx pseudo-diameter (max ecc over sample):", max(eccs))
else:
    print("Graph has no components (empty graph?).")
