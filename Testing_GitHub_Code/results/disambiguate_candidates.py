"""
disambiguate_candidates.py

Resolve ambiguous text-search candidates for Function nodes using conservative heuristics.

Inputs (defaults):
 - results/nodes_textsearch_backfilled.json  (or results/nodes_ast_backfilled.json if you didn't run text search)
 - results/kg_check/textsearch_report.json  (optional: will also read ambiguous_sample from it)
 - results/manifest.json
 - results/symbol_table.json
 - results/function_commits.json (optional)

Outputs:
 - results/nodes_disambiguated.json  (nodes with disambiguated file/lineno where picked)
 - results/kg_check/disambiguation_report.json
 - results/kg_check/remaining_after_disambiguation.txt

Usage:
  python .\\results\\disambiguate_candidates.py \
    --nodes .\\results\\nodes_textsearch_backfilled.json \
    --report .\\results\\kg_check\\textsearch_report.json \
    --results-dir .\\results \
    --repo-dir "C:\\Users\\jsrin\\OneDrive\\Desktop\\PEKG\\Ecommerce-Data-MLOps"
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from collections import defaultdict, Counter
from typing import List, Tuple

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def score_candidate(candidate_file: str, manifest_set: set, sym_count_by_file: dict, func_commits_by_file: dict):
    """
    Return a numeric score and a dict explanation for candidate file path.
    """
    score = 0
    explain = {}
    # canonicalize path
    p = candidate_file.replace("\\", "/")
    explain["file"] = p
    # 1) src/ preference
    if "/src/" in f"/{p}" or p.startswith("src/"):
        score += 20
        explain["src_pref"] = True
    else:
        explain["src_pref"] = False
    # 2) manifest
    if p in manifest_set:
        score += 10
        explain["in_manifest"] = True
    else:
        explain["in_manifest"] = False
    # 3) symbol density
    sym_count = sym_count_by_file.get(p, 0)
    score += sym_count
    explain["sym_count"] = sym_count
    # 4) commit activity (number of commits touching this file) if available
    commit_count = func_commits_by_file.get(p, 0)
    score += min(commit_count, 5)  # cap small boost
    explain["commit_count"] = commit_count
    # 5) deployment bias (small negative for gcpdeploy/dags)
    low_pref = False
    if any(tok in p for tok in ("/gcpdeploy/", "/dags/", "/deploy/", "/airflow/")):
        score -= 5
        low_pref = True
    explain["deployment_bias"] = low_pref
    explain["final_score"] = score
    return score, explain

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes", default=os.path.join("results","nodes_textsearch_backfilled.json"), help="Nodes file to update")
    p.add_argument("--report", default=os.path.join("results","kg_check","textsearch_report.json"), help="Textsearch report (contains ambiguous_sample)")
    p.add_argument("--manifest", default=os.path.join("results","manifest.json"), help="Manifest file (optional)")
    p.add_argument("--symbol_table", default=os.path.join("results","symbol_table.json"), help="Symbol table (optional)")
    p.add_argument("--function_commits", default=os.path.join("results","function_commits.json"), help="Function -> commits mapping (optional)")
    p.add_argument("--results-dir", default="results", help="Results dir to write outputs")
    p.add_argument("--repo-dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps")), help="Repo root")
    args = p.parse_args()

    nodes_path = args.nodes
    report_path = args.report
    nodes = load_json(nodes_path)
    if nodes is None:
        print("ERROR: cannot load nodes:", nodes_path); sys.exit(1)

    report = load_json(report_path) or {}
    ambiguous_sample = report.get("ambiguous_sample") or report.get("ambiguous") or []

    # fallback: if ambiguous_sample not in report, try to load remaining_after_textsearch list
    if not ambiguous_sample:
        rem_path = os.path.join(args.results_dir, "kg_check", "remaining_after_textsearch.txt")
        if os.path.exists(rem_path):
            with open(rem_path, "r", encoding="utf-8") as fh:
                lines = [l.strip() for l in fh if l.strip()]
            # each line is an id; but we need candidate lists; fallback to no-op
            print("No ambiguous candidates found in report; aborting.")
            sys.exit(0)

    # build helper indexes
    id_to_node = {n["id"]: n for n in nodes}
    manifest = load_json(args.manifest) or []
    manifest_set = set()
    if isinstance(manifest, list):
        for e in manifest:
            pth = e.get("path") or e.get("file") or e.get("relpath") or e.get("name")
            if pth:
                manifest_set.add(pth.replace("\\","/"))
    elif isinstance(manifest, dict) and "files" in manifest:
        for e in manifest["files"]:
            pth = e.get("path") or e.get("file") or e.get("relpath") or e.get("name")
            if pth:
                manifest_set.add(pth.replace("\\","/"))

    # symbol table => count defs per file
    sym = load_json(args.symbol_table) or {}
    sym_count_by_file = {}
    if isinstance(sym, dict):
        for q, info in sym.items():
            file_rel = info.get("file") or info.get("path") or info.get("filename")
            if file_rel:
                file_rel = file_rel.replace("\\","/")
                sym_count_by_file[file_rel] = sym_count_by_file.get(file_rel, 0) + 1

    # function_commits mapping -> count commits per file
    fcomm = load_json(args.function_commits) or {}
    func_commits_by_file = {}
    if isinstance(fcomm, dict):
        for func, commits in fcomm.items():
            # we don't have mapping file->commits directly; try to infer if commit entries include file (rare)
            # instead we will count by searching function->node mapping to get a file if present
            pass

    # Build from ambiguous_sample: each item expected like {"id": .., "candidates": [[file,lineno], ...]}
    inferred = []
    left = []
    details = []
    for item in ambiguous_sample:
        if isinstance(item, dict):
            fid = item.get("id")
            cand = item.get("candidates") or []
        elif isinstance(item, list) and len(item)>=2:
            # older format: [id, candidates]
            fid = item[0]
            cand = item[1]
        else:
            continue
        if not cand:
            left.append((fid,"no_candidates"))
            continue
        # compute scores for each candidate
        scored = []
        for (file_rel, ln) in cand:
            score, explain = score_candidate(file_rel.replace("\\","/"), manifest_set, sym_count_by_file, func_commits_by_file)
            explain["lineno"] = ln
            scored.append((score, file_rel, ln, explain))
        # pick highest score
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        top_score, top_file, top_ln, top_explain = scored[0]
        # if top is not clearly above second, mark low confidence: require delta >= 5
        if len(scored) > 1 and (top_score - scored[1][0]) < 5:
            # not decisive enough; mark as left
            left.append((fid, "no_clear_winner", [s[1:] for s in scored]))
            details.append({"id":fid,"scored":scored})
            continue
        # apply selection
        node = id_to_node.get(fid)
        if node is None:
            left.append((fid, "node_not_found"))
            continue
        node["file"] = top_file.replace("\\","/")
        node["lineno"] = top_ln
        attrs = node.setdefault("attrs", {})
        attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method":"disambiguation", "picked": top_file, "lineno": top_ln, "score": top_score, "explain": top_explain}]
        inferred.append({"id":fid, "picked": top_file, "lineno": top_ln, "score": top_score, "explain": top_explain})

    # write outputs
    out_nodes = os.path.join(args.results_dir, "nodes_disambiguated.json")
    write_json(out_nodes, nodes)
    report_out = {
        "inferred_count": len(inferred),
        "left_count": len(left),
        "inferred": inferred[:100],
        "left_sample": left[:200],
        "details_sample": details[:200],
    }
    kg_check_dir = os.path.join(args.results_dir, "kg_check")
    os.makedirs(kg_check_dir, exist_ok=True)
    write_json(os.path.join(kg_check_dir, "disambiguation_report.json"), report_out)
    with open(os.path.join(kg_check_dir, "remaining_after_disambiguation.txt"), "w", encoding="utf-8") as fh:
        for item in left:
            # item might be a tuple like (fid, reason) or (fid, reason, details)
            try:
                # prefer first element if it's a sequence
                if isinstance(item, (list, tuple)) and len(item) >= 1:
                    fid = item[0]
                else:
                    fid = str(item)
            except Exception:
                fid = str(item)
            fh.write(fid + "\n")

    print("Wrote nodes:", out_nodes)
    print("Wrote report:", os.path.join(kg_check_dir, "disambiguation_report.json"))
    print("Inferred:", len(inferred), "Left:", len(left))

if __name__ == "__main__":
    main()
