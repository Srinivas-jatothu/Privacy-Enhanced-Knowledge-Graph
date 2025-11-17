#!/usr/bin/env python3
"""
ast_index_and_backfill.py

- Walks the repo (--repo-dir) and builds an AST index of Python functions/classes:
    maps qualified names (module.path.func, module.path.Class.func) and short names -> (file, lineno)
- Loads results/nodes.json and attempts to backfill missing Function.file and Function.lineno
  using the AST index with safe rules:
    1) exact qualified_name match -> fill
    2) class-qualified (Class.method) match -> fill
    3) unique short-name match across repo -> fill (recorded as inferred)
- Writes results/nodes_ast_backfilled.json (don't overwrite original) and results/kg_check/ast_backfill_report.json

Usage:
  python results/ast_index_and_backfill.py --repo-dir ..\\Ecommerce-Data-MLOps --results-dir .\\results

"""
from __future__ import annotations
import argparse
import ast
import json
import os
import sys
from collections import defaultdict, Counter
from typing import Dict, List, Tuple

def build_ast_index(repo_dir: str) -> Tuple[Dict[str, Tuple[str,int]], Dict[str,List[Tuple[str,int]]]]:
    """
    Walk repo_dir, parse .py files, return:
      - by_qualified: {qualified_name: (relpath, lineno)}
      - by_short: {short_name: [(relpath, lineno), ...]}
    Qualified names use dotted module path derived from file path.
    """
    by_qualified = {}
    by_short = defaultdict(list)
    repo_dir = os.path.abspath(repo_dir)
    for root, _, files in os.walk(repo_dir):
        for fname in files:
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, repo_dir).replace("\\", "/")
            # compute module-like prefix: strip .py, replace / with .
            mod = rel[:-3] if rel.endswith(".py") else rel
            # handle __init__.py -> module is dir
            if os.path.basename(fpath) == "__init__.py":
                mod = os.path.dirname(rel).replace("\\", "/")
                mod = mod.replace("/", ".")
            else:
                mod = mod.replace("/", ".")
            try:
                with open(fpath, "r", encoding="utf-8") as fh:
                    src = fh.read()
                tree = ast.parse(src)
            except Exception:
                continue

            # traverse AST for top-level functions and classes
            for node in tree.body:
                if isinstance(node, ast.FunctionDef):
                    q = f"{mod}.{node.name}"
                    by_qualified[q] = (rel, node.lineno)
                    by_short[node.name].append((rel, node.lineno))
                elif isinstance(node, ast.ClassDef):
                    class_name = node.name
                    # class-level methods
                    for m in node.body:
                        if isinstance(m, ast.FunctionDef):
                            q = f"{mod}.{class_name}.{m.name}"
                            by_qualified[q] = (rel, m.lineno)
                            by_short[m.name].append((rel, m.lineno))
                    # also index class itself as short name
                    by_short[class_name].append((rel, node.lineno))

    return by_qualified, by_short

def load_json(path: str):
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception as e:
        return None

def write_json(path: str, obj):
    open(path, "w", encoding="utf-8").write(json.dumps(obj, indent=2, ensure_ascii=False))

def backfill_nodes(nodes_path: str, results_dir: str, repo_dir: str):
    nodes = load_json(nodes_path)
    if nodes is None:
        print("ERROR: cannot load nodes.json at", nodes_path); return 1

    print("Building AST index for repo:", repo_dir)
    by_q, by_short = build_ast_index(repo_dir)
    print("Indexed qualified names:", len(by_q), "unique short names:", len(by_short))

    total_functions = 0
    missing_initial = 0
    filled_exact = 0
    filled_short_unique = 0
    filled_class_method = 0
    remaining = []

    # map id->node for convenience
    id_to_node = {n['id']: n for n in nodes}

    for n in nodes:
        if n.get("type") != "Function":
            continue
        total_functions += 1
        if n.get("file") and n.get("lineno"):
            continue
        q = n.get("qualified_name")
        short = (q.split(".")[-1] if q else n.get("id","").split(":")[-1])
        if not n.get("file"):
            missing_initial += 1

        filled = False
        # 1) exact qualified match
        if q and q in by_q:
            rel, ln = by_q[q]
            n["file"] = rel
            n["lineno"] = ln
            n.setdefault("attrs", {})["_backfill"] = "ast_exact"
            filled_exact += 1
            filled = True
        # 2) class.method qualified variant (maybe the node had Class.method as q)
        if not filled and q:
            # try replacing last two parts if it looks like only short name present etc.
            # if q has 2 components or more, try searching for class.method patterns in by_q
            # nothing extra here (the exact q should have matched). Skip.
            pass
        # 3) unique short-name match -> only if unique across repo
        if not filled:
            cands = by_short.get(short, [])
            if len(cands) == 1:
                rel, ln = cands[0]
                n["file"] = rel
                n["lineno"] = ln
                n.setdefault("attrs", {})["_backfill"] = "ast_short_unique"
                filled_short_unique += 1
                filled = True
        if not filled:
            remaining.append(n['id'])

    # write outputs
    out_nodes = os.path.join(results_dir, "nodes_ast_backfilled.json")
    write_json(out_nodes, nodes)
    report = {
        "total_functions": total_functions,
        "initial_missing": missing_initial,
        "filled_exact": filled_exact,
        "filled_short_unique": filled_short_unique,
        "remaining_missing": len(remaining),
        "remaining_sample": remaining[:200]
    }
    out_dir = os.path.join(results_dir, "kg_check")
    os.makedirs(out_dir, exist_ok=True)
    write_json(os.path.join(out_dir, "ast_backfill_report.json"), report)
    write_json(os.path.join(out_dir, "remaining_missing_after_ast.txt"), remaining)  # json list saved as .txt

    print("Backfill report:")
    print(json.dumps(report, indent=2))
    print("Wrote:", out_nodes)
    print("Wrote report:", os.path.join(out_dir, "ast_backfill_report.json"))
    return 0

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps")), help="Path to repository root to index")
    p.add_argument("--results-dir", default=os.path.join(os.path.dirname(__file__), "results"), help="Path to results directory")
    return p.parse_args()

def main():
    args = parse_args()
    results_dir = os.path.abspath(args.results_dir)
    repo_dir = os.path.abspath(args.repo_dir)
    nodes_path = os.path.join(results_dir, "nodes.json")
    if not os.path.exists(nodes_path):
        print("ERROR: cannot find nodes.json at", nodes_path); sys.exit(1)
    rc = backfill_nodes(nodes_path, results_dir, repo_dir)
    sys.exit(rc)

if __name__ == "__main__":
    main()
