"""
callgraph_infer_locations.py

Infer missing Function.file and Function.lineno using CALLS edges (call-graph inference).

Usage:
  python results/callgraph_infer_locations.py \
    --nodes .\results\nodes_ast_backfilled.json \
    --edges .\results\edges.json \
    --results-dir .\results \
    --repo-dir ..\Ecommerce-Data-MLOps

Outputs:
  - results/nodes_callgraph_backfilled.json
  - results/kg_check/callgraph_infer_report.json
  - results/kg_check/remaining_after_callgraph.txt
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from collections import defaultdict, Counter
from typing import Dict, List, Tuple

def load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"ERROR loading {path}: {e}", file=sys.stderr)
        return None

def write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def build_call_targets(edges: List[Dict]) -> Dict[str, List[str]]:
    """
    Return mapping: target_node_id -> list of caller_node_ids for CALLS edges.
    """
    callers = defaultdict(list)
    for e in edges:
        if e.get("type") == "CALLS" or e.get("type") == "CALLS_FILE_LEVEL":
            tgt = e.get("target")
            src = e.get("source")
            if tgt and src:
                callers[tgt].append(src)
    return callers

def infer_locations(
    nodes: List[Dict],
    edges: List[Dict],
    repo_dir: str,
    symbol_table: Dict = None,
    module_index: Dict = None,
    min_caller_count: int = 2,
    attempt_lineno_search: bool = True
):
    # Index nodes by id
    id_to_node = {n["id"]: n for n in nodes}
    # Build function list missing file or lineno
    missing_funcs = [n for n in nodes if n.get("type")=="Function" and not n.get("file")]
    # Build callers map
    callers_map = build_call_targets(edges)
    # Helper: find file for a node id
    def node_file(node_id):
        n = id_to_node.get(node_id)
        if not n:
            return None
        return n.get("file")

    # For searching lineno in a file for a function short name (def <name>)
    import re
    def search_lineno_in_file(relpath: str, short_name: str, repo_dir_local: str):
        if not relpath:
            return None
        fp = os.path.join(repo_dir_local, relpath)
        if not os.path.exists(fp):
            return None
        pat = re.compile(r'^\s*def\s+' + re.escape(short_name) + r'\s*\(', re.IGNORECASE)
        try:
            with open(fp, "r", encoding="utf-8", errors="ignore") as fh:
                for i, line in enumerate(fh, start=1):
                    if pat.match(line):
                        return i
        except Exception:
            return None
        return None

    inferred = []
    remaining = []
    ambiguous = []
    for fnode in missing_funcs:
        fid = fnode["id"]
        # gather caller files
        callers = callers_map.get(fid, [])
        caller_files = [node_file(c) for c in callers if node_file(c)]
        caller_files = [c for c in caller_files if c]  # filter None
        # count occurrences
        file_counts = Counter(caller_files)
        inferred_file = None
        infer_reason = None
        details = {"caller_files": dict(file_counts), "num_callers": len(callers)}
        if not caller_files:
            # no callers with known file -> cannot infer from call graph
            remaining.append((fid, "no_callers_with_file"))
            continue
        # choose most common file
        most_common_file, most_common_count = file_counts.most_common(1)[0]
        # apply conservative rule:
        # - if only one distinct caller file -> accept it
        # - OR if most_common_count >= min_caller_count -> accept it
        if len(file_counts) == 1 or most_common_count >= min_caller_count:
            inferred_file = most_common_file
            infer_reason = "most_common_caller_file"
        else:
            # attempt module-level consensus: look for directory prefix majority
            # map files -> module prefix (dir)
            dir_counts = Counter([os.path.dirname(p) for p in caller_files])
            dir_most, dir_count = dir_counts.most_common(1)[0]
            # accept module-level if majority of callers come from same dir
            if dir_count >= min_caller_count:
                # try to pick a likely file inside that dir (if unique)
                candidate_files = [p for p in caller_files if os.path.dirname(p) == dir_most]
                # if exactly one unique candidate file in that dir, pick it; else mark ambiguous
                unique_cands = sorted(set(candidate_files))
                if len(unique_cands) == 1:
                    inferred_file = unique_cands[0]
                    infer_reason = "module_dir_single_file"
                else:
                    ambiguous.append((fid, "module_dir_multiple_files", unique_cands, dict(dir_counts)))
            else:
                ambiguous.append((fid, "no_strong_consensus", dict(file_counts)))

        # If inferred_file found, optionally try to find lineno
        if inferred_file:
            short = (fnode.get("qualified_name") or "").split(".")[-1] or fnode.get("id","").split(":")[-1]
            lineno = None
            if attempt_lineno_search:
                lineno = search_lineno_in_file(inferred_file, short, repo_dir)
            # apply inferred values
            fnode["file"] = inferred_file
            if lineno:
                fnode["lineno"] = lineno
            # add provenance
            attrs = fnode.setdefault("attrs", {})
            attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method":"call_graph", "reason": infer_reason, "details": details}]
            inferred.append({"id": fid, "inferred_file": inferred_file, "lineno": lineno, "reason": infer_reason, "details": details})
        # if not inferred and not already added to ambiguous/remaining
        elif not any(fid == r[0] for r in remaining) and not any(fid == a[0] for a in ambiguous):
            remaining.append((fid, "no_inference"))

    return inferred, remaining, ambiguous

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes", default=os.path.join("results","nodes_ast_backfilled.json"), help="Input nodes JSON (AST-backfilled recommended)")
    p.add_argument("--edges", default=os.path.join("results","edges.json"), help="Edges JSON")
    p.add_argument("--results-dir", default="results", help="Results directory to write outputs")
    p.add_argument("--repo-dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps")), help="Repo root (for lineno search)")
    p.add_argument("--min-caller-count", type=int, default=2, help="Minimum caller count threshold to accept a file inference")
    p.add_argument("--no-lineno-search", action="store_true", help="Do not attempt to find lineno by searching the inferred file")
    return p.parse_args()

def main():
    args = parse_args()
    nodes_path = args.nodes
    edges_path = args.edges
    results_dir = args.results_dir
    repo_dir = args.repo_dir
    os.makedirs(results_dir, exist_ok=True)
    kg_check_dir = os.path.join(results_dir, "kg_check")
    os.makedirs(kg_check_dir, exist_ok=True)

    print("Loading nodes from:", nodes_path)
    nodes = load_json(nodes_path)
    if nodes is None:
        print("ERROR: nodes could not be loaded, aborting.", file=sys.stderr)
        sys.exit(1)
    edges = load_json(edges_path) or []

    print("Running call-graph inference (min_caller_count=%d)..." % args.min_caller_count)
    inferred, remaining, ambiguous = infer_locations(
        nodes, edges, repo_dir, min_caller_count=args.min_caller_count, attempt_lineno_search=not args.no_lineno_search
    )

    # Write outputs
    out_nodes = os.path.join(results_dir, "nodes_callgraph_backfilled.json")
    write_json(out_nodes, nodes)

    report = {
        "inferred_count": len(inferred),
        "remaining_count": len(remaining),
        "ambiguous_count": len(ambiguous),
        "inferred": inferred[:500],
        "remaining_sample": remaining[:500],
        "ambiguous_sample": ambiguous[:500],
    }
    report_path = os.path.join(kg_check_dir, "callgraph_infer_report.json")
    write_json(report_path, report)
    rem_path = os.path.join(kg_check_dir, "remaining_after_callgraph.txt")
    with open(rem_path, "w", encoding="utf-8") as fh:
        for fid, reason in remaining:
            fh.write(fid + "\n")

    print("Wrote nodes:", out_nodes)
    print("Wrote report:", report_path)
    print("Remaining (count):", len(remaining), "Ambiguous (count):", len(ambiguous))
    print("Done.")

if __name__ == "__main__":
    main()
