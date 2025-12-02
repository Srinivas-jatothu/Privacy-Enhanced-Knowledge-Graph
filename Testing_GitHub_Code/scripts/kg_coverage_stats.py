#!/usr/bin/env python3
# scripts/kg_coverage_stats.py

import os, json, collections

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if not os.path.exists(os.path.join(PROJECT_ROOT, "results")):
    PROJECT_ROOT = os.getcwd()

def load_json(path, default=None):
    if not os.path.exists(path):
        print(f"[WARN] Missing file: {path}")
        return default
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return json.load(fh)

results_dir = os.path.join(PROJECT_ROOT, "results")
nodes = load_json(os.path.join(results_dir, "node_v2.json"), [])
edges = load_json(os.path.join(results_dir, "edges.json"), [])
manifest = load_json(os.path.join(results_dir, "manifest.json"), {})
code_entities = load_json(os.path.join(results_dir, "code_entities_full.json"), [])
commits = load_json(os.path.join(results_dir, "commits.json"), [])
prs = load_json(os.path.join(results_dir, "pull_requests.json"), [])

print("=== Coverage of Software Artifacts ===")

# --- Helper: index nodes by type and path -------------------------------------
node_by_type = collections.defaultdict(list)
file_paths_in_kg = set()
functions_in_kg = set()
commits_in_kg = set()
prs_in_kg = set()

for n in nodes:
    t = n.get("type") or (n.get("attrs") or {}).get("type")
    node_by_type[t].append(n)
    if t == "File":
        p = n.get("path") or (n.get("attrs") or {}).get("path") or (n.get("attrs") or {}).get("file")
        if p:
            file_paths_in_kg.add(p)
    if t == "Function":
        qname = n.get("qualified_name") or (n.get("attrs") or {}).get("qualified_name") or n.get("id")
        if qname:
            functions_in_kg.add(qname)
    if t == "Commit":
        sha = n.get("sha") or (n.get("attrs") or {}).get("sha") or n.get("id")
        if sha:
            commits_in_kg.add(sha)
    if t == "PullRequest":
        num = n.get("number") or (n.get("attrs") or {}).get("number") or n.get("id")
        if num:
            prs_in_kg.add(str(num))

# --- File coverage ------------------------------------------------------------
all_files = []
if isinstance(manifest, dict):
    # assume manifest["files"] or similar
    if "files" in manifest and isinstance(manifest["files"], list):
        all_files = [f.get("path") for f in manifest["files"] if isinstance(f, dict)]
    else:
        # fallback: treat keys as paths if manifest is {path: {...}}
        all_files = list(manifest.keys())
elif isinstance(manifest, list):
    all_files = [f.get("path") for f in manifest if isinstance(f, dict)]

all_files = [p for p in all_files if p]
all_files_set = set(all_files)

files_in_kg = file_paths_in_kg
file_coverage = (len(files_in_kg) / len(all_files_set) * 100.0) if all_files_set else 0.0

print("\n-- File Coverage --")
print("Files in repo (manifest):", len(all_files_set))
print("Files represented as File nodes in KG:", len(files_in_kg))
print(f"File coverage: {file_coverage:.2f}%")

# --- Function coverage --------------------------------------------------------
all_functions = set()
if isinstance(code_entities, list):
    for e in code_entities:
        qn = e.get("qualified_name") or e.get("name")
        if qn:
            all_functions.add(qn)

func_coverage = (len(functions_in_kg) / len(all_functions) * 100.0) if all_functions else 0.0

print("\n-- Function Coverage --")
print("Functions in code_entities_full:", len(all_functions))
print("Functions represented in KG:", len(functions_in_kg))
print(f"Function coverage: {func_coverage:.2f}%")

# --- Commit & PR coverage -----------------------------------------------------
commit_total = len(commits) if isinstance(commits, list) else 0
pr_total = len(prs) if isinstance(prs, list) else 0

print("\n-- Commit Coverage (approx) --")
print("Commits fetched:", commit_total)
print("Commit nodes in KG:", len(commits_in_kg))
if commit_total:
    print("Commit coverage (approx): {:.2f}%".format(len(commits_in_kg) / commit_total * 100.0))

print("\n-- PR Coverage (approx) --")
print("PRs fetched:", pr_total)
print("PR nodes in KG:", len(prs_in_kg))
if pr_total:
    print("PR coverage (approx): {:.2f}%".format(len(prs_in_kg) / pr_total * 100.0))

# --- Edge-level coverage idea (optional summary) ------------------------------
print("\n-- Edge Connectivity --")
print("Total edges in KG:", len(edges))
print("Average degree (undirected approx): will be in kg_overall_stats.py")
print("\n[NOTE] For the paper, use these numbers in a table like:\n"
      "  - % of files represented in KG\n"
      "  - % of functions represented in KG\n"
      "  - % of commits / PRs represented in KG\n")
