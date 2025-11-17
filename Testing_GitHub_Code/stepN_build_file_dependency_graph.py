"""
stepN_build_file_dependency_graph.py

Build a file-to-file dependency graph from:
  - import mapping (module_index.json, import_aliases.json)
  - function-level call graph (call_graph.json / call_graph_enriched.json)

Outputs:
  - results/file_dependency_graph.json   (list of edges with counts & provenance)
  - results/file_dependency_matrix.json  (map src -> tgt -> details)

Usage:
  python stepN_build_file_dependency_graph.py --repo-dir /path/to/repo --results-dir ./results
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import pathlib
import sys
from collections import defaultdict
from typing import Dict, Any, List, Tuple, Optional

# ---------- helpers ----------
def setup_logger(results_dir: str):
    os.makedirs(results_dir, exist_ok=True)
    logger = logging.getLogger("file-deps")
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
    handler.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def load_json(path: str) -> Optional[Any]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return None

def safe_rel(repo_dir: str, p: str) -> str:
    try:
        return str(pathlib.Path(p).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
    except Exception:
        return p

# ---------- mapping helpers ----------
def module_to_file_map(module_index: Dict[str, Any]) -> Dict[str, str]:
    """
    Returns module_name -> file_rel mapping from module_index.
    """
    m = {}
    for mod, info in module_index.items():
        file_rel = info.get("file")
        if file_rel:
            m[mod] = file_rel
    return m

def resolve_module_alias(alias_target: str, module_to_file: Dict[str,str]) -> Optional[str]:
    """
    Given an alias target (like 'pkg.mod' or 'pkg.mod.Symbol'), return file if exists.
    """
    if not alias_target:
        return None
    # direct module match
    if alias_target in module_to_file:
        return module_to_file[alias_target]
    # if it's module.symbol, strip last component
    parts = alias_target.split(".")
    for i in range(len(parts), 0, -1):
        cand = ".".join(parts[:i])
        if cand in module_to_file:
            return module_to_file[cand]
    return None

# ---------- main builder ----------
def build_import_edges(import_aliases: Dict[str, Dict[str,str]],
                       module_to_file: Dict[str,str],
                       logger) -> Dict[Tuple[str,str], Dict[str,Any]]:
    """
    From import_aliases (module -> {local_alias: target_module_or_symbol}), build file-level edges.
    Returns dict keyed by (src_file, tgt_file) -> {count, provenance: {imported_modules: set}}
    """
    edges = defaultdict(lambda: {"count": 0, "provenance": defaultdict(int), "reasons": set()})
    for module, alias_map in import_aliases.items():
        src_file = module_to_file.get(module)
        if not src_file:
            # attempt to ignore or continue
            continue
        for local_name, target in alias_map.items():
            tgt_file = resolve_module_alias(target, module_to_file)
            if not tgt_file:
                continue
            key = (src_file, tgt_file)
            edges[key]["count"] += 1
            edges[key]["provenance"]["import_statements"] += 1
            edges[key]["reasons"].add(f"import:{local_name}->{target}")
    logger.info("Built %d import-based file edges", len(edges))
    return edges

def build_call_edges(call_graph: List[Dict[str,Any]],
                     canonical: Dict[str, Any],
                     logger) -> Dict[Tuple[str,str], Dict[str,Any]]:
    """
    Collapse function-level call edges to file-level edges. Uses resolved_symbol if present,
    otherwise attempts to map callee_chain using canonical table (by suffix match).
    """
    edges = defaultdict(lambda: {"count": 0, "provenance": defaultdict(int), "reasons": set()})
    unresolved_callee_counter = 0
    for e in call_graph:
        src_file = e.get("caller_file")
        if not src_file:
            continue
        # determine callee file:
        callee_file = None
        # prefer resolved_symbol
        res_sym = e.get("resolved_symbol")
        if res_sym and res_sym in canonical:
            callee_file = canonical[res_sym].get("file")
        else:
            # try to map callee_chain string to canonical by suffix match
            callee_chain = e.get("callee_chain")
            if callee_chain and isinstance(callee_chain, str):
                short = callee_chain.split(".")[-1]
                # pick canonical entry whose qualified name ends with short and choose first
                for k, info in canonical.items():
                    if k.endswith(f".{short}"):
                        callee_file = info.get("file")
                        break
        if not callee_file:
            unresolved_callee_counter += 1
            # skip or still add as unresolved target (we skip)
            continue
        key = (src_file, callee_file)
        edges[key]["count"] += 1
        edges[key]["provenance"]["call_sites"] += 1
        # include some reason detail (lineno or callee chain)
        reason = e.get("callee_chain") or e.get("enrichments", {}).get("resolve_reason") or "call"
        edges[key]["reasons"].add(reason)
    logger.info("Built %d call-derived file edges (skipped %d unresolved callees)", len(edges), unresolved_callee_counter)
    return edges

def merge_edges(import_edges: Dict[Tuple[str,str], Dict[str,Any]],
                call_edges: Dict[Tuple[str,str], Dict[str,Any]],
                logger) -> Tuple[List[Dict[str,Any]], Dict[str, Dict[str,Any]]]:
    """
    Merge two edge dicts into final structures.
    """
    combined = defaultdict(lambda: {"count":0, "provenance": defaultdict(int), "reasons": set()})
    for key, meta in import_edges.items():
        combined[key]["count"] += meta["count"]
        for k,v in meta["provenance"].items():
            combined[key]["provenance"][k] += v
        combined[key]["reasons"].update(meta["reasons"])
    for key, meta in call_edges.items():
        combined[key]["count"] += meta["count"]
        for k,v in meta["provenance"].items():
            combined[key]["provenance"][k] += v
        combined[key]["reasons"].update(meta["reasons"])
    # convert to list form and matrix form
    edge_list = []
    matrix = {}
    for (src, tgt), meta in combined.items():
        prov = dict(meta["provenance"])
        reasons = sorted(list(meta["reasons"]))
        rec = {"source": src, "target": tgt, "count": meta["count"], "provenance": prov, "reasons": reasons}
        edge_list.append(rec)
        matrix.setdefault(src, {})[tgt] = {"count": meta["count"], "provenance": prov, "reasons": reasons}
    logger.info("Merged total file-level edges: %d", len(edge_list))
    return edge_list, matrix

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Build file-to-file dependency graph from import & call artifacts.")
    p.add_argument("--repo-dir", required=True, help="Repo root")
    p.add_argument("--results-dir", default="./results", help="Directory where module_index.json, import_aliases.json, call_graph.json live")
    return p.parse_args()

def main():
    args = parse_args()
    repo_dir = args.repo_dir
    results_dir = args.results_dir
    logger = setup_logger(results_dir)

    # load artifacts
    module_index = load_json(os.path.join(results_dir, "module_index.json")) or {}
    import_aliases = load_json(os.path.join(results_dir, "import_aliases.json")) or {}
    canonical = load_json(os.path.join(results_dir, "symbol_table.json")) or {}
    call_graph = load_json(os.path.join(results_dir, "call_graph.json")) or []
    # prefer enriched if present
    enriched = load_json(os.path.join(results_dir, "call_graph_enriched.json"))
    if enriched:
        logger.info("Using enriched call graph (call_graph_enriched.json)")
        call_graph = enriched

    module_to_file = module_to_file_map(module_index)

    # build import edges
    import_edges = build_import_edges(import_aliases, module_to_file, logger)

    # build call edges
    call_edges = build_call_edges(call_graph, canonical, logger)

    # merge edges
    edge_list, matrix = merge_edges(import_edges, call_edges, logger)

    # write outputs
    out_list = os.path.join(results_dir, "file_dependency_graph.json")
    out_matrix = os.path.join(results_dir, "file_dependency_matrix.json")
    with open(out_list, "w", encoding="utf-8") as fo:
        json.dump(edge_list, fo, indent=2, ensure_ascii=False)
    with open(out_matrix, "w", encoding="utf-8") as fo:
        json.dump(matrix, fo, indent=2, ensure_ascii=False)

    logger.info("Wrote outputs: %s, %s", out_list, out_matrix)
    logger.info("Done.")

if __name__ == "__main__":
    main()
