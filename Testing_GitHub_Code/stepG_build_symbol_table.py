"""
stepG_build_symbol_table.py

Purpose
-------
Build a cross-file Python symbol table for a repository:
  - Discover module names from file paths
  - Parse Python files (or consume precomputed ASTs / code entities if present)
  - Collect definitions: functions, classes, methods (with file & lineno)
  - Parse import statements to map local aliases -> fully-qualified names
  - Resolve imported/simple names to canonical definitions when possible
  - Emit results/symbol_table.json mapping canonical_symbol -> definition entry

Usage
-----
# Default: assume repo at ../Ecommerce-Data-MLOps and results/ dir is cwd/results
python stepG_build_symbol_table.py

# Customize repo path / output path
python stepG_build_symbol_table.py --repo-dir "C:/path/to/repo" --out-dir "./results"

Notes
-----
- Heuristic resolver: best-effort static mapping using import statements and file layout.
- Not a full type/resolution engine — intended as a practical lightweight symbol table to
  support call-graph building and KG linking.
- Output format: JSON mapping: canonical_name -> {
      "type": "function"|"class"|"method",
      "module": "pkg.mod",
      "qualified_name": "pkg.mod.Class.method" (or pkg.mod.func),
      "file": "relative/path.py",
      "lineno": int,
      "end_lineno": int|null
  }
"""

from __future__ import annotations
import os
import sys
import argparse
import json
import logging
import pathlib
import ast
from typing import Dict, List, Tuple, Optional, Any

# ----------------------------
# Utilities
# ----------------------------
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger("symbol-table")


def compute_module_name(repo_dir: str, file_path: str) -> str:
    """
    Compute a plausible Python module name from a file path:
      - relative path from repo_dir
      - strip .py suffix
      - treat __init__.py so that package directory becomes module name
      - replace path separators with dots
    """
    repo = pathlib.Path(repo_dir).resolve()
    path = pathlib.Path(file_path).resolve()
    try:
        rel = path.relative_to(repo)
    except Exception:
        rel = path
    parts = rel.parts
    # Remove suffix if .py
    if parts[-1].endswith(".py"):
        parts = list(parts)
        parts[-1] = parts[-1][:-3]
        # handle package __init__ -> drop the last part
        if parts[-1] == "__init__":
            parts = parts[:-1]
    # filter out leading empty or dot segments
    mod = ".".join([p for p in parts if p and p != "."])
    return mod

def find_py_files(repo_dir: str, patterns: List[str]) -> List[str]:
    repo = pathlib.Path(repo_dir)
    files = []
    for pat in patterns:
        for p in repo.glob(pat):
            if p.is_file() and p.suffix == ".py":
                files.append(str(p.resolve()))
    files = sorted(list(dict.fromkeys(files)))
    return files

def parse_ast_file(path: str) -> Optional[ast.AST]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            src = f.read()
        return ast.parse(src)
    except Exception:
        return None

# ----------------------------
# Extraction passes
# ----------------------------
def collect_defs_from_ast(tree: ast.AST) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Return (functions, classes)
    functions: list of {name, lineno, end_lineno}
    classes: list of {name, lineno, end_lineno, methods: [{name, lineno, end_lineno}]}
    Only top-level functions and top-level classes are included (methods inside classes are captured).
    """
    functions = []
    classes = []
    for node in getattr(tree, "body", []):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append({
                "name": node.name,
                "lineno": getattr(node, "lineno", None),
                "end_lineno": getattr(node, "end_lineno", None)
            })
        elif isinstance(node, ast.ClassDef):
            methods = []
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods.append({
                        "name": child.name,
                        "lineno": getattr(child, "lineno", None),
                        "end_lineno": getattr(child, "end_lineno", None)
                    })
            classes.append({
                "name": node.name,
                "lineno": getattr(node, "lineno", None),
                "end_lineno": getattr(node, "end_lineno", None),
                "methods": methods
            })
    return functions, classes

def collect_imports_from_ast(tree: ast.AST) -> List[Dict[str, str]]:
    """
    Return a list of imports as dicts:
      - {'type':'import', 'module': 'pkg.mod', 'name': None, 'alias': 'alias' or None}
      - {'type':'from', 'module': 'pkg.mod', 'name': 'symbol', 'alias': 'alias' or None}
    """
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.append({"type": "import", "module": n.name, "name": None, "alias": n.asname or n.name.split(".")[0]})
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for n in node.names:
                imports.append({"type": "from", "module": module, "name": n.name, "alias": n.asname or n.name})
    return imports

# ----------------------------
# Main resolution logic
# ----------------------------
def build_initial_def_map(repo_dir: str, py_files: List[str], logger) -> Dict[str, Dict]:
    """
    Build a map: module -> local definitions
    and a global initial index of canonical symbol -> def-info
    """
    module_defs: Dict[str, Dict[str, List[Dict]]] = {}
    canonical: Dict[str, Dict] = {}

    for p in py_files:
        tree = parse_ast_file(p)
        if tree is None:
            logger.warning("AST parse failed: %s", p)
            continue
        mod = compute_module_name(repo_dir, p) or "<root>"
        funcs, classes = collect_defs_from_ast(tree)
        module_defs[mod] = {"file": safe_relpath(repo_dir, p), "functions": funcs, "classes": classes}
        # populate canonical names for top-level functions and classes
        for f in funcs:
            qn = f"{mod}.{f['name']}" if mod else f"{f['name']}"
            canonical[qn] = {
                "type": "function",
                "module": mod,
                "qualified_name": qn,
                "file": safe_relpath(repo_dir, p),
                "lineno": f.get("lineno"),
                "end_lineno": f.get("end_lineno")
            }
        for c in classes:
            qn = f"{mod}.{c['name']}"
            canonical[qn] = {
                "type": "class",
                "module": mod,
                "qualified_name": qn,
                "file": safe_relpath(repo_dir, p),
                "lineno": c.get("lineno"),
                "end_lineno": c.get("end_lineno")
            }
            # methods: qualified as modul.Class.method
            for m in c.get("methods", []):
                mqn = f"{mod}.{c['name']}.{m['name']}"
                canonical[mqn] = {
                    "type": "method",
                    "module": mod,
                    "qualified_name": mqn,
                    "file": safe_relpath(repo_dir, p),
                    "lineno": m.get("lineno"),
                    "end_lineno": m.get("end_lineno")
                }
    return canonical, module_defs

def build_import_aliases(repo_dir: str, py_files: List[str], logger) -> Dict[str, Dict[str, str]]:
    """
    For each module, build a map local_name -> fully_qualified_name (best effort)
    returns: module -> { local_alias: candidate_fully_qualified_name_or_module }
    Example:
      - "from pkg.mod import X as Y" -> local alias Y -> "pkg.mod.X"
      - "import pkg.mod as pm" -> local alias pm -> "pkg.mod"
      - "import pkg.mod" -> local alias pkg -> "pkg"
    """
    aliases: Dict[str, Dict[str, str]] = {}
    for p in py_files:
        tree = parse_ast_file(p)
        if tree is None:
            continue
        mod = compute_module_name(repo_dir, p) or "<root>"
        ali = {}
        for im in collect_imports_from_ast(tree):
            if im["type"] == "import":
                module = im["module"]
                alias = im["alias"]
                ali[alias] = module
            elif im["type"] == "from":
                module = im["module"]
                name = im["name"]
                alias = im["alias"]
                if module:
                    ali[alias] = f"{module}.{name}"
                else:
                    ali[alias] = name  # relative import or same package; best-effort
        aliases[mod] = ali
    return aliases

def resolve_symbol(name: str, current_module: str, aliases_map: Dict[str, Dict[str, str]], canonical_index: Dict[str, Dict], logger) -> Optional[str]:
    """
    Resolve a possibly simple or dotted name into a canonical qualified symbol (if known).
    Heuristics:
      - If name already looks fully-qualified and exists in canonical_index -> return it
      - If name is dotted like "pkg.mod.func" -> check direct match
      - If simple name and present in current module canonical (current_module.name) -> map
      - If simple name and present in aliases for current module -> expand alias then check
      - If alias resolved to module X and name looks like X.Y -> attempt to find in canonical_index
    Returns canonical qualified_name or None.
    """
    # direct full name
    if name in canonical_index:
        return name
    # dotted name
    if "." in name:
        # try exact
        if name in canonical_index:
            return name
        # try trimming leading dots (relative)
        cand = name
        if cand in canonical_index:
            return cand
    # try simple name in current module
    q_local = f"{current_module}.{name}" if current_module else name
    if q_local in canonical_index:
        return q_local
    # try alias mapping
    aliases = aliases_map.get(current_module, {}) if aliases_map else {}
    if name in aliases:
        ali = aliases[name]  # e.g., "pkg.mod" or "pkg.mod.Class"
        # if alias is module, try alias + '.' + name? tough — check canonical for alias and alias.* heuristics
        if ali in canonical_index:
            return ali
        # if ali ends with '.X', try that
        if ali in canonical_index:
            return ali
    # no resolution
    return None

# ----------------------------
# Helpers (safe relpath)
# ----------------------------
def safe_relpath(repo_dir: str, p: str) -> str:
    try:
        return str(pathlib.Path(p).resolve().relative_to(pathlib.Path(repo_dir).resolve())).replace("\\", "/")
    except Exception:
        return os.path.basename(p)

# ----------------------------
# Main entry
# ----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Build lightweight Python symbol table (cross-file resolution heuristics).")
    p.add_argument("--repo-dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Ecommerce-Data-MLOps")),
                   help="Path to repository root (default: ../Ecommerce-Data-MLOps)")
    p.add_argument("--out-dir", default=os.path.join(os.getcwd(), "results"),
                   help="Output directory (default: ./results)")
    p.add_argument("--patterns", nargs="*", default=["**/*.py"], help="Glob patterns to find python files")
    return p.parse_args()

def main():
    args = parse_args()
    logger = setup_logging()
    repo_dir = args.repo_dir
    out_dir = args.out_dir
    patterns = args.patterns

    os.makedirs(out_dir, exist_ok=True)
    symbol_out = os.path.join(out_dir, "symbol_table.json")
    module_index_out = os.path.join(out_dir, "module_defs.json")
    aliases_out = os.path.join(out_dir, "import_aliases.json")

    logger.info("Repository dir: %s", repo_dir)
    logger.info("Output dir: %s", out_dir)
    logger.info("Patterns: %s", patterns)

    py_files = find_py_files(repo_dir, patterns)
    logger.info("Discovered %d Python files", len(py_files))

    # Step 1: initial canonical index from definitions
    canonical_index, module_defs = build_initial_def_map(repo_dir, py_files, logger)
    logger.info("Initial canonical definitions: %d symbols", len(canonical_index))

    # Step 2: collect import aliases to resolve cross-module aliases
    aliases_map = build_import_aliases(repo_dir, py_files, logger)
    logger.info("Built import alias map for %d modules", len(aliases_map))

    # Step 3: attempt to resolve local simple names used across modules (best-effort)
    # For now, the symbol table will contain the canonical_index entries; for each module we
    # also record its aliases so downstream callgraph code can use the alias map to resolve calls.
    symbol_table: Dict[str, Dict] = {}
    for qn, info in canonical_index.items():
        # copy info verbatim
        symbol_table[qn] = info.copy()

    # Step 4: attempt heuristic additional names: record local short names pointing to canonical
    # Example: if module pkg.mod has function foo, also add mapping pkg.mod:foo -> pkg.mod.foo (for quick lookup)
    # and record simple name -> canonical candidates under module-scoped index saved separately.
    module_scoped_map: Dict[str, Dict[str, List[str]]] = {}
    for qn, info in canonical_index.items():
        module = info.get("module") or ""
        short = qn.split(".")[-1]
        module_scoped_map.setdefault(module, {}).setdefault(short, []).append(qn)

    # Save results
    try:
        with open(symbol_out, "w", encoding="utf-8") as fo:
            json.dump(symbol_table, fo, indent=2, ensure_ascii=False)
        with open(module_index_out, "w", encoding="utf-8") as fo:
            json.dump(module_defs, fo, indent=2, ensure_ascii=False)
        with open(aliases_out, "w", encoding="utf-8") as fo:
            json.dump(aliases_map, fo, indent=2, ensure_ascii=False)
        logger.info("Wrote symbol_table (%d entries) to: %s", len(symbol_table), symbol_out)
        logger.info("Wrote module_defs to: %s", module_index_out)
        logger.info("Wrote import_aliases to: %s", aliases_out)
    except Exception as e:
        logger.error("Failed to write outputs: %s", e)
        raise

    # print quick examples
    logger.info("Example symbols (first 20):")
    for i, (k, v) in enumerate(symbol_table.items()):
        if i >= 20:
            break
        logger.info("  %s -> %s:%s", k, v.get("file"), v.get("lineno"))

if __name__ == "__main__":
    main()
