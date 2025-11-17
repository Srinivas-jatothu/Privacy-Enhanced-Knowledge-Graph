"""
stepM_enrich_call_graph_with_jedi.py

Purpose
-------
Enrich an existing heuristic call graph (results/call_graph.json) by attempting to resolve
callees using Jedi (static name resolution / inference). Writes an enriched call graph:

  results/call_graph_enriched.json

Behavior
--------
- Loads the following inputs from the `results/` directory:
    - call_graph.json
    - symbol_table.json
    - import_aliases.json (optional)
    - module_index.json (optional)
- For each call site, opens the corresponding source file and invokes Jedi
  at the callee expression location to get inferred definitions.
- Maps Jedi definitions to canonical symbols where possible (by file path and name).
- Adds enriched fields to each edge:
    - resolved_by: "jedi"|"heuristic" (existing)|"none"
    - resolved_symbol: canonical_name or None
    - resolve_confidence: float (0..1)
    - resolve_reason: string
    - jedi_matches: list of raw jedi definition dicts (file, name, module, line)
- Outputs statistics summary and writes enriched JSON.

Requirements
------------
- jedi installed in the environment (`pip install jedi`)
- Local repository files must be present at the paths referenced in the call_graph.

Usage
-----
python stepM_enrich_call_graph_with_jedi.py --repo-dir /path/to/repo --results-dir ./results --max-workers 4

Author: PEKG pipeline (production-grade)
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import pathlib
import ast
from typing import Any, Dict, List, Optional, Tuple

# try import jedi
try:
    import jedi
except Exception as e:
    jedi = None

# ----------------------------
# Defaults
# ----------------------------
DEFAULT_REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps"))
DEFAULT_RESULTS_DIR = os.path.join(os.getcwd(), "results")
LOGFILE = "stepM_enrich_call_graph_with_jedi.log"
OUTFILE = "call_graph_enriched.json"

# ----------------------------
# Logging
# ----------------------------
def setup_logger(results_dir: str, level=logging.INFO) -> logging.Logger:
    os.makedirs(results_dir, exist_ok=True)
    logger = logging.getLogger("callgraph-jedi")
    logger.setLevel(level)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(os.path.join(results_dir, LOGFILE))
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)
    return logger

# ----------------------------
# Helpers
# ----------------------------
def safe_relpath(repo_dir: str, path: str) -> str:
    try:
        return str(pathlib.Path(path).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
    except Exception:
        return path

def load_json_if_exists(path: str) -> Optional[Any]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def parse_source_for_column(source: str, lineno: int, name_token: str) -> int:
    """
    Given source, a lineno (1-indexed) and name token, try to find column offset for first occurrence
    of name_token on that line. Return 0-based column. If not found, return 0.
    """
    lines = source.splitlines()
    if lineno <= 0 or lineno > len(lines):
        return 0
    line = lines[lineno - 1]
    idx = line.find(name_token)
    if idx >= 0:
        return idx
    # fallback: try splitting on whitespace and find the token-like segment
    tokens = []
    current = ""
    for i, ch in enumerate(line):
        if ch.isalnum() or ch in ("_", "."):
            current += ch
        else:
            if current:
                tokens.append((current, i - len(current)))
            current = ""
    if current:
        tokens.append((current, len(line) - len(current)))
    for t, col in tokens:
        if t.endswith(name_token) or name_token.endswith(t) or name_token in t:
            return col
    return 0

# ----------------------------
# Jedi resolution wrapper
# ----------------------------
def jedi_infer_at(repo_dir: str, file_rel: str, line: int, column: int, logger) -> List[Dict[str, Any]]:
    """
    Use jedi to infer definitions at given position.
    Returns list of dicts: {name, module_name, module_path, line, type, description}
    """
    if jedi is None:
        raise RuntimeError("jedi not installed. Please pip install jedi")
    abs_path = os.path.join(repo_dir, file_rel)
    try:
        with open(abs_path, "r", encoding="utf-8", errors="replace") as fh:
            src = fh.read()
    except Exception as e:
        logger.debug("Failed to read %s: %s", abs_path, e)
        return []

    try:
        # create jedi Script
        script = jedi.Script(code=src, path=abs_path)
        # jedi expects 1-based line, 0-based column
        definitions = script.infer(line=line, column=column)
        out = []
        for d in definitions:
            try:
                mpath = d.module_path if hasattr(d, "module_path") else None
            except Exception:
                mpath = None
            out.append({
                "name": getattr(d, "name", None),
                "type": getattr(d, "type", None),
                "module_name": getattr(d, "module_name", None),
                "module_path": str(mpath) if mpath else None,
                "line": getattr(d, "line", None),
                "description": getattr(d, "description", None)
            })
        return out
    except Exception as e:
        logger.debug("Jedi inference failed for %s:%d:%d -> %s", file_rel, line, column, e)
        return []

# ----------------------------
# Canonical mapping helper
# ----------------------------
def map_jedi_to_canonical(jedi_def: Dict[str, Any], canonical_index: Dict[str, Dict], repo_dir: str, logger) -> Optional[str]:
    """
    Try to map a jedi definition to an entry in canonical symbol_table.
    Strategies:
      - If module_path exists: convert to relpath and match file + name (function/class/method)
      - If module_name exists: try module_name.name
      - Fallback: find any canonical symbol whose last component matches jedi_def['name'] and module matches
    Returns canonical_name or None.
    """
    name = jedi_def.get("name")
    module_path = jedi_def.get("module_path")
    module_name = jedi_def.get("module_name")

    # Map by module_path + name
    if module_path:
        try:
            rel = str(pathlib.Path(module_path).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
        except Exception:
            rel = os.path.relpath(module_path)
        # search canonical for file match and name suffix
        for k, info in canonical_index.items():
            if info.get("file") == rel and k.split(".")[-1] == name:
                return k
            # try matching by qualified last two components (Class.method)
            if k.endswith(f".{name}") and info.get("file") == rel:
                return k

    # Map by module_name + name
    if module_name and name:
        cand = f"{module_name}.{name}"
        if cand in canonical_index:
            return cand
        # also try prefixes removal (module_name may be package)
        for k in canonical_index:
            if k.endswith(f".{module_name}.{name}") or k.endswith(f".{name}") and module_name in k:
                return k

    # Fallback: match by short name (ambiguous) - choose first sensible candidate in same module if possible
    short = name
    candidates = [k for k in canonical_index if k.split(".")[-1] == short]
    if len(candidates) == 1:
        return candidates[0]
    # try to prefer candidates whose module part is substring of module_name
    if module_name:
        for c in candidates:
            if module_name in c:
                return c
    # else None
    return None

# ----------------------------
# Main enrichment loop
# ----------------------------
def enrich_call_graph(repo_dir: str, results_dir: str, max_infers: int, logger) -> None:
    call_graph_path = os.path.join(results_dir, "call_graph.json")
    canonical_path = os.path.join(results_dir, "symbol_table.json")
    module_index_path = os.path.join(results_dir, "module_index.json")
    aliases_path = os.path.join(results_dir, "import_aliases.json")

    call_graph = load_json_if_exists(call_graph_path) or []
    canonical = load_json_if_exists(canonical_path) or {}
    module_index = load_json_if_exists(module_index_path) or {}
    aliases = load_json_if_exists(aliases_path) or {}

    logger.info("Loaded call graph edges: %d", len(call_graph))
    logger.info("Loaded canonical symbols: %d", len(canonical))

    enriched_edges: List[Dict[str, Any]] = []
    total = len(call_graph)
    resolved_by_jedi = 0
    already_resolved = 0
    could_not_resolve = 0
    processed = 0

    for edge in call_graph:
        processed += 1
        # copy original edge
        e = dict(edge)
        e.setdefault("enrichments", {})
        # skip if already resolved
        if e.get("resolved") and e.get("resolved_symbol"):
            e["enrichments"]["status"] = "already_resolved"
            already_resolved += 1
            enriched_edges.append(e)
            continue

        # determine source file and site lineno
        caller_file = e.get("caller_file") or e.get("caller_file")
        site_lineno = e.get("site_lineno") or e.get("caller_lineno")
        callee_chain = e.get("callee_chain")  # dotted chain as string or None
        # try best candidate name token for column: last identifier of chain
        name_token = None
        if callee_chain:
            name_token = callee_chain.split(".")[-1]
        else:
            # if no chain, skip jedi attempt
            e["enrichments"]["status"] = "no_callee_chain"
            enriched_edges.append(e)
            could_not_resolve += 1
            continue

        if not caller_file or not site_lineno:
            e["enrichments"]["status"] = "missing_site_info"
            enriched_edges.append(e)
            could_not_resolve += 1
            continue

        # Read source file
        abs_file = os.path.join(repo_dir, caller_file)
        if not os.path.exists(abs_file):
            e["enrichments"]["status"] = "source_missing"
            enriched_edges.append(e)
            could_not_resolve += 1
            continue

        try:
            with open(abs_file, "r", encoding="utf-8", errors="replace") as fh:
                src = fh.read()
        except Exception as ex:
            logger.debug("Failed reading %s: %s", abs_file, ex)
            e["enrichments"]["status"] = "source_read_fail"
            enriched_edges.append(e)
            could_not_resolve += 1
            continue

        # Compute column offset for name token heuristically
        col = parse_source_for_column(src, int(site_lineno), name_token)
        # Jedi infer (protect with try/except)
        jedi_matches = []
        try:
            jedi_defs = jedi_infer_at(repo_dir, caller_file, int(site_lineno), int(col), logger) if jedi else []
            jedi_matches = jedi_defs
        except Exception as ex:
            logger.debug("Jedi exception at %s:%s:%s -> %s", caller_file, site_lineno, col, ex)
            jedi_matches = []

        # map jedi defs to canonical symbols
        mapped = None
        mapped_by = None
        map_reason = None
        confidence = 0.0
        if jedi_matches:
            # iterate definitions and attempt to map
            for jd in jedi_matches[:max_infers]:
                cand = map_jedi_to_canonical(jd, canonical, repo_dir, logger)
                if cand:
                    mapped = cand
                    mapped_by = jd
                    map_reason = f"jedi:{jd.get('module_name')}@{jd.get('line')}"
                    confidence = 0.95  # high confidence for direct mapping
                    break
            # if no direct mapping, but jedi returned items, mark with low confidence fallback
            if not mapped:
                # pick first jedi result and try module_name.name
                first = jedi_matches[0]
                fallback_name = None
                if first.get("module_name") and first.get("name"):
                    fallback_name = f"{first.get('module_name')}.{first.get('name')}"
                if fallback_name in canonical:
                    mapped = fallback_name
                    mapped_by = first
                    map_reason = "jedi_fallback_module_name"
                    confidence = 0.6
                else:
                    # leave unresolved but include jedi matches
                    map_reason = "jedi_no_map"
                    confidence = 0.0
        else:
            map_reason = "jedi_no_matches"
            confidence = 0.0

        # fill enrichment fields
        e["enrichments"]["jedi_matches"] = jedi_matches
        e["enrichments"]["jedi_attempt"] = bool(jedi_matches)
        e["enrichments"]["resolved_by"] = "jedi" if mapped else "none"
        e["enrichments"]["resolved_symbol"] = mapped
        e["enrichments"]["resolve_reason"] = map_reason
        e["enrichments"]["confidence"] = float(confidence)
        if mapped:
            resolved_by_jedi += 1
        else:
            could_not_resolve += 1

        enriched_edges.append(e)

        # occasional progress logging
        if processed % 200 == 0:
            logger.info("Processed %d/%d edges  (jedi_resolved=%d)", processed, total, resolved_by_jedi)

    # final stats
    logger.info("Enrichment complete. edges=%d jedi_resolved=%d already_resolved=%d unresolved=%d",
                total, resolved_by_jedi, already_resolved, could_not_resolve)

    # write output
    out_path = os.path.join(results_dir, OUTFILE)
    with open(out_path, "w", encoding="utf-8") as fo:
        json.dump(enriched_edges, fo, indent=2, ensure_ascii=False)
    logger.info("Wrote enriched call graph to: %s", out_path)

# ----------------------------
# CLI
# ----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Enrich call graph with Jedi inference results.")
    p.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Repository root (where files referenced by call graph live).")
    p.add_argument("--results-dir", default=DEFAULT_RESULTS_DIR, help="Directory containing call_graph.json and output location.")
    p.add_argument("--max-infers", default=3, type=int, help="Max number of jedi definitions to consider per call site.")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level.")
    return p.parse_args()

def main():
    args = parse_args()
    logger = setup_logger(args.results_dir, getattr(logging, args.log_level))
    if jedi is None:
        logger.error("Jedi is not installed in this environment. Install with: pip install jedi")
        sys.exit(2)
    logger.info("Starting Jedi-based call-graph enrichment")
    logger.info("Repo dir: %s", args.repo_dir)
    logger.info("Results dir: %s", args.results_dir)
    enrich_call_graph(args.repo_dir, args.results_dir, args.max_infers, logger)
    logger.info("Done.")

if __name__ == "__main__":
    main()
