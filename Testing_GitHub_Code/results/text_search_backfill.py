#!/usr/bin/env python3
"""
text_search_backfill.py

Conservative text-based backfill for Function.file and Function.lineno.
Searches repository files for function definitions like:
  - def function_name(
  - async def function_name(
  - class ClassName: ... def method_name(

Only fills when:
  - A single unambiguous match is found (unique file + line), OR
  - Multiple matches exist but they are all in the same file (then pick first occurrence).

Adds provenance in node.attrs._inferred_from (list) with {"method":"text_search","file":..., "lineno":..., "confidence":"high|medium"}.

Inputs (defaults):
 - results/nodes_ast_backfilled.json
 - repo_dir (path to repository to search) - set to your Ecommerce-Data-MLOps path

Outputs:
 - results/nodes_textsearch_backfilled.json
 - results/kg_check/textsearch_report.json
 - results/kg_check/remaining_after_textsearch.txt

Usage (example):
  python .\\results\\text_search_backfill.py --repo-dir "C:\\Users\\jsrin\\OneDrive\\Desktop\\PEKG\\Ecommerce-Data-MLOps" --results-dir .\\results
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
from collections import defaultdict, Counter
from typing import List, Tuple, Dict

def load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"ERROR loading {path}: {e}", file=sys.stderr)
        return None

def write_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)

def find_definitions_in_file(filepath: str, short_name: str) -> List[int]:
    """
    Return list of line numbers where 'def short_name(' or 'async def short_name(' appears.
    Also detect class methods by scanning 'class' blocks (simple heuristic).
    """
    linenos = []
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as fh:
            for i, line in enumerate(fh, start=1):
                if re.match(r'^\s*(async\s+)?def\s+' + re.escape(short_name) + r'\s*\(', line):
                    linenos.append(i)
    except Exception:
        pass
    return linenos

def collect_search_files(repo_dir: str, extensions: Tuple[str,...] = ('.py','.ipynb','.pyw','.ipy')):
    files = []
    for root, _, names in os.walk(repo_dir):
        for n in names:
            if n.startswith('.') or n.endswith('.pyc') or n.endswith('.pyo'):
                continue
            if n.endswith(extensions):
                files.append(os.path.join(root, n))
            # also include .md or .rst if you want doc scraping (not by default)
    return files

def safe_relpath(path: str, base: str):
    try:
        return os.path.relpath(path, base).replace("\\","/")
    except Exception:
        return path

def run_backfill(nodes_path: str, repo_dir: str, results_dir: str):
    nodes = load_json(nodes_path)
    if nodes is None:
        print("ERROR: cannot load nodes file:", nodes_path)
        return 1

    # Build index of functions missing file
    missing = [n for n in nodes if n.get("type")=="Function" and not n.get("file")]
    print(f"Functions missing before text-search: {len(missing)}")

    # Collect repo files to search
    search_files = collect_search_files(repo_dir)
    print(f"Files to search: {len(search_files)} (extensions .py/.ipynb/.pyw/.ipy)")

    # Map short_name -> list of matches (file, lineno)
    recovered = []
    ambiguous = []
    still_missing = []

    # Prebuild a small inverted index? For 24 items linear scan is fine
    for fnode in missing:
        short = (fnode.get("qualified_name") or fnode.get("id","")).split(".")[-1]
        matches = []
        for fp in search_files:
            lnlist = find_definitions_in_file(fp, short)
            if lnlist:
                rel = safe_relpath(fp, repo_dir)
                for ln in lnlist:
                    matches.append((rel, ln))
            # small optimization: if many matches found across many files, keep scanning to measure ambiguity
        if not matches:
            still_missing.append(fnode["id"])
            continue
        # If all matches are in same file, accept first lineno (confidence medium)
        files = set(m[0] for m in matches)
        if len(files) == 1:
            file0 = matches[0][0]
            lineno0 = matches[0][1]
            fnode["file"] = file0
            fnode["lineno"] = lineno0
            attrs = fnode.setdefault("attrs", {})
            attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method":"text_search", "file":file0, "lineno":lineno0, "confidence":"medium", "matches_total": len(matches)}]
            recovered.append({"id": fnode["id"], "file": file0, "lineno": lineno0, "matches": matches})
        else:
            # If unique single match (single (file,lineno) pair), accept high confidence
            uniq_pairs = list({(m[0],m[1]) for m in matches})
            if len(uniq_pairs) == 1:
                file0, lineno0 = uniq_pairs[0]
                fnode["file"] = file0
                fnode["lineno"] = lineno0
                attrs = fnode.setdefault("attrs", {})
                attrs["_inferred_from"] = attrs.get("_inferred_from", []) + [{"method":"text_search", "file":file0, "lineno":lineno0, "confidence":"high", "matches_total": len(matches)}]
                recovered.append({"id": fnode["id"], "file": file0, "lineno": lineno0, "matches": matches})
            else:
                # ambiguous across multiple files/lines -> do not fill automatically
                ambiguous.append({"id": fnode["id"], "candidates": matches})
                still_missing.append(fnode["id"])

    # Write outputs
    out_nodes = os.path.join(results_dir, "nodes_textsearch_backfilled.json")
    write_json(out_nodes, nodes)
    kg_check_dir = os.path.join(results_dir, "kg_check")
    os.makedirs(kg_check_dir, exist_ok=True)
    report = {
        "total_missing_before": len(missing),
        "recovered_count": len(recovered),
        "ambiguous_count": len(ambiguous),
        "remaining_after": len(still_missing),
        "recovered_sample": recovered[:200],
        "ambiguous_sample": ambiguous[:200]
    }
    write_json(os.path.join(kg_check_dir, "textsearch_report.json"), report)
    with open(os.path.join(kg_check_dir, "remaining_after_textsearch.txt"), "w", encoding="utf-8") as fh:
        for rid in still_missing:
            fh.write(rid + "\n")

    print("Wrote nodes:", out_nodes)
    print("Report:", os.path.join(kg_check_dir, "textsearch_report.json"))
    print("Remaining after text-search:", len(still_missing))
    return 0

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes", default=os.path.join("results","nodes_ast_backfilled.json"), help="Input nodes (AST-backfilled recommended)")
    p.add_argument("--repo-dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps")), help="Repo root to scan")
    p.add_argument("--results-dir", default=os.path.join(os.path.dirname(__file__), "." ,"results"), help="Results dir")
    return p.parse_args()

def main():
    args = parse_args()
    nodes_path = args.nodes
    repo_dir = args.repo_dir
    results_dir = args.results_dir
    if not os.path.exists(nodes_path):
        print("ERROR: nodes file not found:", nodes_path, file=sys.stderr)
        sys.exit(1)
    if not os.path.isdir(repo_dir):
        print("ERROR: repo_dir not found:", repo_dir, file=sys.stderr)
        sys.exit(1)
    rc = run_backfill(nodes_path, repo_dir, results_dir)
    sys.exit(rc)

if __name__ == "__main__":
    main()
